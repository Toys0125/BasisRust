#!/usr/bin/env python3
"""Offline migration of converted BasisRust SQLite saves to PostgreSQL.

Dry-run by default. Authentication uses normal libpq PG* environment variables,
PGSERVICE/.pg_service.conf and .pgpass; credentials never appear in command args.
"""
import argparse
import hashlib
import json
import re
import shutil
import sqlite3
import subprocess
import tempfile
import unicodedata
from pathlib import Path

MAX_PLAYER_BYTES = 1_048_576
MAX_JSON_BYTES = 8 * MAX_PLAYER_BYTES


def checked_table(table):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", table):
        raise ValueError("table must be an ASCII SQL identifier of at most 63 bytes")
    return table.lower()  # Match unquoted PostgreSQL identifiers and Rust accounting.


def usage_table(table):
    return "basis_storage_usage_" + hashlib.sha256(checked_table(table).encode()).hexdigest()[:32]


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON property in source record")
        result[key] = value
    return result


def valid_name(name):
    return (
        isinstance(name, str)
        and 0 < len(name.encode("utf-8")) <= 64
        and name.strip() == name
        and all(unicodedata.category(c) not in {"Cc", "Cf", "Zl", "Zp"} for c in name)
    )


def validate_record(owner, payload):
    if not isinstance(owner, str) or not 0 < len(owner.encode()) <= 4096 or "\0" in owner:
        raise ValueError("invalid source owner")
    if not isinstance(payload, bytes) or len(payload) > MAX_JSON_BYTES:
        raise ValueError("source payload is not a bounded JSON blob")
    record = json.loads(payload, object_pairs_hook=unique_object)
    if not isinstance(record, dict) or record.get("owner") != owner:
        raise ValueError("source owner/payload mismatch")
    scopes = record.get("scopes")
    if not isinstance(scopes, dict) or len(scopes) > 64:
        raise ValueError("invalid source scopes")
    count = used = 0
    for scope, entries in scopes.items():
        if not valid_name(scope) or not isinstance(entries, dict) or not entries:
            raise ValueError("invalid source scope")
        used += 3 + len(scope.encode())
        for key, value in entries.items():
            if (
                not valid_name(key)
                or not isinstance(value, list)
                or len(value) > 65_535
                or any(type(byte) is not int or not 0 <= byte <= 255 for byte in value)
            ):
                raise ValueError("invalid source key/value")
            used += 3 + len(key.encode()) + len(value)
            count += 1
    if used > MAX_PLAYER_BYTES or count > 1024:
        raise ValueError("source record exceeds BasisVR absolute limits")
    return bool(scopes)


def prepare(source, table, stream):
    """Validate a stable read-only snapshot before starting any remote operation."""
    table = checked_table(table)
    usage = usage_table(table)
    ledger = usage + "_legacy"
    if not source.is_file():
        raise ValueError("source SQLite database does not exist")
    stream.write(f"""BEGIN;
SELECT pg_advisory_xact_lock(813411,20261025);
LOCK TABLE {table} IN SHARE ROW EXCLUSIVE MODE;
CREATE TABLE IF NOT EXISTS {ledger} (owner_key TEXT PRIMARY KEY NOT NULL);
CREATE TEMP TABLE basisrust_migration_input (owner_key TEXT PRIMARY KEY, owner_hex TEXT NOT NULL, payload_hex TEXT NOT NULL) ON COMMIT DROP;
COPY basisrust_migration_input (owner_key,owner_hex,payload_hex) FROM STDIN;
""".encode())
    records = payload_bytes = empty = 0
    with sqlite3.connect(source.resolve().as_uri() + "?mode=ro", uri=True) as connection:
        connection.execute("BEGIN")
        # A table identifier is validated above; record data never becomes SQL text.
        for owner, payload in connection.execute(f"SELECT owner,payload FROM {table} ORDER BY owner"):
            if not validate_record(owner, payload):
                empty += 1
                continue
            owner_bytes = owner.encode()
            owner_key = hashlib.sha256(owner_bytes).hexdigest()
            stream.write(f"{owner_key}\t{owner_bytes.hex()}\t{payload.hex()}\n".encode())
            records += 1
            payload_bytes += len(payload)
    stream.write(f"""\\.
CREATE TEMP TABLE basisrust_migration_pending ON COMMIT DROP AS
 SELECT owner_key,convert_from(decode(owner_hex,'hex'),'UTF8') AS owner,decode(payload_hex,'hex') AS payload
 FROM basisrust_migration_input i WHERE NOT EXISTS (SELECT 1 FROM {ledger} l WHERE l.owner_key=i.owner_key);
CREATE TEMP TABLE basisrust_migration_result ON COMMIT DROP AS
 WITH inserted AS (INSERT INTO {table} (owner,payload) SELECT owner,payload FROM basisrust_migration_pending ON CONFLICT(owner) DO NOTHING RETURNING 1)
 SELECT COUNT(*) AS imported FROM inserted;
INSERT INTO {ledger} (owner_key) SELECT owner_key FROM basisrust_migration_pending ON CONFLICT DO NOTHING;
DO $$ BEGIN
 IF (SELECT used FROM {usage} WHERE id=1) IS DISTINCT FROM (SELECT COALESCE(SUM(octet_length(payload)),0) FROM {table}) THEN
  RAISE EXCEPTION 'storage accounting mismatch; initialize the destination using BasisRust before migrating';
 END IF;
END $$;
SELECT json_build_object('imported',r.imported,'preserved',(SELECT COUNT(*) FROM basisrust_migration_pending)-r.imported,'already_processed',(SELECT COUNT(*) FROM basisrust_migration_input)-(SELECT COUNT(*) FROM basisrust_migration_pending)) FROM basisrust_migration_result r;
COMMIT;
""".encode())
    stream.seek(0)
    return {"records": records, "serialized_bytes": payload_bytes, "empty_skipped": empty}


def migrate(source, table="basis_storage", apply=False):
    with tempfile.TemporaryFile() as stream:
        summary = prepare(Path(source), table, stream)
        if not apply:
            return {"dry_run": True, **summary}
        psql = shutil.which("psql")
        if psql is None:
            raise RuntimeError("psql is required for --apply")
        result = subprocess.run(
            [psql, "--no-psqlrc", "--no-password", "--set", "ON_ERROR_STOP=1", "--quiet", "--tuples-only", "--no-align"],
            stdin=stream, capture_output=True, check=False,
        )
        if result.returncode:
            # PostgreSQL error details can echo entire payloads. Do not print them.
            raise RuntimeError("psql failed; check access and the initialized target schema. If the connection failed during COMMIT, the outcome may be unknown. Rerunning is safe: existing accounts and import markers are preserved.")
        lines = result.stdout.decode().strip().splitlines()
        return {"dry_run": False, **summary, **json.loads(lines[-1])}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sqlite", type=Path, required=True, help="offline converted BasisRust SQLite database")
    parser.add_argument("--table", default="basis_storage", help="same StorageSqlTable name in source and destination")
    parser.add_argument("--apply", action="store_true", help="commit to PostgreSQL using libpq PG* connection settings")
    args = parser.parse_args()
    try:
        print(json.dumps(migrate(args.sqlite, args.table, args.apply), sort_keys=True))
    except (ValueError, UnicodeError, sqlite3.Error, RuntimeError, OSError) as error:
        parser.exit(1, f"Migration failed: {error}\n")


if __name__ == "__main__":
    main()
