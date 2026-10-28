import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import unittest
from unittest import mock
from urllib.parse import unquote, urlparse
import uuid

import migrate_player_storage as migration


def payload(owner, value=None):
    return json.dumps({"owner": owner, "scopes": {"save:世界": {"score": [0, 1, 255] if value is None else value, "empty": []}}}, ensure_ascii=False).encode()


def seed(path, table, rows):
    with sqlite3.connect(path) as connection:
        connection.execute(f"CREATE TABLE {table} (owner TEXT PRIMARY KEY, payload BLOB NOT NULL)")
        connection.executemany(f"INSERT INTO {table} VALUES (?,?)", rows)


class ValidationTests(unittest.TestCase):
    def test_accounting_names_match_rust_shared_fixture(self):
        fixture = Path(__file__).resolve().parents[2] / "BasisRustServer/crates/basis-server-storage/tests/fixtures/accounting-names.json"
        for case in json.loads(fixture.read_text()):
            self.assertEqual(migration.usage_table(case["table"]), case["usage"])

    def test_validates_records_and_rejects_unsafe_inputs(self):
        self.assertTrue(migration.validate_record("alice", payload("alice")))
        for owner, data in [
            ("bob", payload("alice")),
            ("alice", b'{"owner":"alice","scopes":{},"scopes":{}}'),
            ("alice", json.dumps({"owner": "alice", "scopes": {"save": {"key": [True]}}}).encode()),
            ("alice", json.dumps({"owner": "alice", "scopes": {"\u202e": {"key": [1]}}}).encode()),
        ]:
            with self.assertRaises(ValueError):
                migration.validate_record(owner, data)
        for table in ["x;DROP TABLE x", "9bad", "a" * 64, "é"]:
            with self.assertRaises(ValueError):
                migration.checked_table(table)
        self.assertEqual(migration.checked_table("Mixed_CASE"), "mixed_case")

    def test_dry_run_is_read_only_and_encodes_data_outside_sql_syntax(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.db"
            owner = "did:key:unicode-世界'\\copy"
            data = payload(owner)
            seed(path, "basis_storage", [(owner, data)])
            before = path.read_bytes()
            with mock.patch.object(subprocess, "run", side_effect=AssertionError("dry-run must not connect")):
                result = migration.migrate(path)
            self.assertEqual(result["records"], 1)
            self.assertEqual(path.read_bytes(), before)
            stream = io.BytesIO()
            migration.prepare(path, "basis_storage", stream)
            sql = stream.read()
            self.assertNotIn(owner.encode(), sql)
            self.assertNotIn(data, sql)
            self.assertIn(data.hex().encode(), sql)
            self.assertIn(b"ON CONFLICT(owner) DO NOTHING", sql)
            self.assertTrue(sql.endswith(b"COMMIT;\n"))

    def test_invalid_source_never_starts_remote_operation(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.db"
            seed(path, "basis_storage", [("alice", payload("bob"))])
            with mock.patch.object(subprocess, "run") as process:
                with self.assertRaises(ValueError):
                    migration.migrate(path, apply=True)
                process.assert_not_called()


@unittest.skipUnless(os.environ.get("BASIS_TEST_POSTGRES_URL"), "requires test PostgreSQL")
class PostgreSqlTests(unittest.TestCase):
    def setUp(self):
        url = urlparse(os.environ["BASIS_TEST_POSTGRES_URL"])
        env = {
            "PGHOST": url.hostname or "127.0.0.1", "PGPORT": str(url.port or 5432),
            "PGDATABASE": unquote(url.path.lstrip("/")), "PGUSER": unquote(url.username or ""),
            "PGPASSWORD": unquote(url.password or ""),
        }
        self.environment = mock.patch.dict(os.environ, env)
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "source.db"
        self.table = "migration_test_" + uuid.uuid4().hex
        self.usage = migration.usage_table(self.table)
        self.sql(f"""
CREATE TABLE {self.table} (owner TEXT PRIMARY KEY, payload BYTEA NOT NULL);
CREATE TABLE {self.usage} (id INTEGER PRIMARY KEY, used BIGINT NOT NULL);
INSERT INTO {self.usage} VALUES (1,0);
CREATE FUNCTION {self.usage}_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF TG_OP='INSERT' THEN UPDATE {self.usage} SET used=used+octet_length(NEW.payload) WHERE id=1;
 ELSIF TG_OP='DELETE' THEN UPDATE {self.usage} SET used=used-octet_length(OLD.payload) WHERE id=1;
 ELSE UPDATE {self.usage} SET used=used+octet_length(NEW.payload)-octet_length(OLD.payload) WHERE id=1;
 END IF; RETURN NULL; END $$;
CREATE TRIGGER migration_counter AFTER INSERT OR UPDATE OR DELETE ON {self.table} FOR EACH ROW EXECUTE FUNCTION {self.usage}_fn();
""")
        self.addCleanup(self.sql, f"DROP TABLE IF EXISTS {self.table} CASCADE; DROP TABLE IF EXISTS {self.usage}; DROP TABLE IF EXISTS {self.usage}_legacy; DROP FUNCTION IF EXISTS {self.usage}_fn();")

    def sql(self, sql):
        result = subprocess.run(["psql", "-X", "-w", "-qAt", "-v", "ON_ERROR_STOP=1"], input=sql, text=True, capture_output=True, check=True)
        return result.stdout.strip()

    def test_transactional_copy_preserves_existing_accounts_and_wipes(self):
        seed(self.path, self.table, [("alice", payload("alice")), ("bob", payload("bob"))])
        original = payload("alice", [99])
        self.sql(f"INSERT INTO {self.table} VALUES ('alice',decode('{original.hex()}','hex'));")
        result = migration.migrate(self.path, self.table, apply=True)
        self.assertEqual((result["imported"], result["preserved"]), (1, 1))
        self.assertEqual(self.sql(f"SELECT encode(payload,'hex') FROM {self.table} WHERE owner='alice'"), original.hex())
        self.assertEqual(self.sql(f"SELECT used=(SELECT SUM(octet_length(payload)) FROM {self.table}) FROM {self.usage}"), "t")
        self.sql(f"DELETE FROM {self.table} WHERE owner='bob';")
        result = migration.migrate(self.path, self.table, apply=True)
        self.assertEqual((result["imported"], result["already_processed"]), (0, 2))
        self.assertEqual(self.sql(f"SELECT COUNT(*) FROM {self.table}"), "1")

    def test_missing_accounting_rolls_back_data_and_import_markers(self):
        seed(self.path, self.table, [("alice", payload("alice"))])
        self.sql(f"DROP TRIGGER migration_counter ON {self.table};")
        with self.assertRaises(RuntimeError):
            migration.migrate(self.path, self.table, apply=True)
        self.assertEqual(self.sql(f"SELECT COUNT(*) FROM {self.table}"), "0")
        self.assertEqual(self.sql(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name='{self.usage}_legacy'"), "0")


if __name__ == "__main__":
    unittest.main()
