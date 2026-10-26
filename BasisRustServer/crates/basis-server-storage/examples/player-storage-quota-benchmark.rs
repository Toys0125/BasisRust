//! Fixed-work quota benchmark: 512 overwrites, eight concurrent writers, 1 KiB values.
use anyhow::Result;
use basis_protocol::storage::{StorageOp, StorageRequest, StorageStatus};
use basis_server_storage::{player::PlayerStorage, sql::SqlPlayerStorage};
use sha2::{Digest, Sha256};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use std::{path::Path, sync::Arc, time::Instant};

const WRITERS: usize = 8;
const WRITES: usize = 512;
const LIMIT: usize = 1024 * 1024 * 1024;

fn owner(index: usize) -> String {
    format!("owner-{index:05}")
}

fn payload(owner: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "owner": owner,
        "scopes": {"bench": {"value": vec![1u8; 1024]}}
    }))
    .unwrap()
}

fn request(op: StorageOp) -> StorageRequest {
    StorageRequest {
        op,
        id: 1,
        scope: "bench".into(),
        key: "value".into(),
        value: if op == StorageOp::Set {
            vec![2; 1024]
        } else {
            vec![]
        },
    }
}

async fn sqlite_seed(path: &Path, accounts: usize) -> Result<()> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(path)
                .create_if_missing(true)
                .journal_mode(SqliteJournalMode::Wal),
        )
        .await?;
    sqlx::query("CREATE TABLE basis_storage (owner TEXT PRIMARY KEY, payload BLOB NOT NULL)")
        .execute(&pool)
        .await?;
    let mut tx = pool.begin().await?;
    for index in 0..accounts {
        let owner = owner(index);
        sqlx::query("INSERT INTO basis_storage VALUES (?1, ?2)")
            .bind(&owner)
            .bind(payload(&owner))
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    pool.close().await;
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let backend = args.get(1).map(String::as_str).unwrap_or("file");
    let accounts: usize = args.get(2).map(|n| n.parse()).transpose()?.unwrap_or(1000);
    anyhow::ensure!(accounts >= WRITES, "need at least {WRITES} accounts");
    let temp = tempfile::tempdir()?;
    let sqlite_path = temp.path().join("players.sqlite3");
    let (file, sql) = match backend {
        "file" => {
            for index in 0..accounts {
                let owner = owner(index);
                let name: String = Sha256::digest(owner.as_bytes())
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect();
                std::fs::write(temp.path().join(format!("{name}.json")), payload(&owner))?;
            }
            (
                Some(PlayerStorage::new(Some(temp.path().to_path_buf()))?),
                None,
            )
        }
        "sqlite" => {
            sqlite_seed(&sqlite_path, accounts).await?;
            let (storage, _) =
                SqlPlayerStorage::connect("", Some(&sqlite_path), "basis_storage").await?;
            (None, Some(Arc::new(storage)))
        }
        _ => anyhow::bail!("backend must be file or sqlite"),
    };
    let mut tasks = tokio::task::JoinSet::new();
    let start = Instant::now();
    for worker in 0..WRITERS {
        let file = file.clone();
        let sql = sql.clone();
        tasks.spawn(async move {
            let mut latency = Vec::new();
            for index in (worker..WRITES).step_by(WRITERS) {
                let owner = owner(index);
                let start = Instant::now();
                let reply = if let Some(storage) = &file {
                    let storage = storage.clone();
                    tokio::task::spawn_blocking(move || {
                        storage.execute(&owner, request(StorageOp::Set), 16384, LIMIT)
                    })
                    .await
                    .unwrap()
                } else {
                    sql.as_ref()
                        .unwrap()
                        .execute(&owner, request(StorageOp::Set), 16384, LIMIT)
                        .await
                };
                anyhow::ensure!(
                    reply.status == StorageStatus::Ok,
                    "write failed: {:?}",
                    reply.status
                );
                latency.push(start.elapsed().as_secs_f64() * 1000.0);
            }
            Ok::<_, anyhow::Error>(latency)
        });
    }
    let mut latency = Vec::new();
    while let Some(result) = tasks.join_next().await {
        latency.extend(result??);
    }
    let elapsed = start.elapsed().as_secs_f64();
    // Verify work after timing; no speedup may come from skipping writes.
    for index in 0..WRITES {
        let reply = if let Some(storage) = &file {
            storage.execute(&owner(index), request(StorageOp::Get), 16384, LIMIT)
        } else {
            sql.as_ref()
                .unwrap()
                .execute(&owner(index), request(StorageOp::Get), 16384, LIMIT)
                .await
        };
        anyhow::ensure!(
            reply.status == StorageStatus::Ok && reply.value == vec![2; 1024],
            "stored value mismatch"
        );
    }
    latency.sort_by(f64::total_cmp);
    println!("{{\"backend\":\"{backend}\",\"accounts\":{accounts},\"writers\":{WRITERS},\"writes\":{WRITES},\"errors\":0,\"elapsed_ms\":{:.3},\"writes_per_second\":{:.1},\"p50_ms\":{:.3},\"p95_ms\":{:.3}}}", elapsed*1000.0, WRITES as f64/elapsed, latency[latency.len()/2], latency[latency.len()*95/100]);
    Ok(())
}
