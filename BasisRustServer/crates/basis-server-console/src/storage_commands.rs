use basis_server_core::{ServerState, StorageAdminAction};
use std::time::Duration;

pub fn handle(server: &ServerState, runtime: &tokio::runtime::Handle, args: &[&str]) {
    let Some(action) = parse(args) else {
        println!("Usage: /storage | /storage show <uuid> | /storage wipe <uuid>");
        return;
    };
    match runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(10), server.storage_admin(action)).await
    }) {
        Ok(Ok(reply)) => println!("{reply}"),
        Ok(Err(error)) => println!("Storage command failed: {error}"),
        Err(_) => {
            println!("Storage command timed out; an operation already started may still complete.")
        }
    }
}

fn parse(args: &[&str]) -> Option<StorageAdminAction> {
    match args {
        [] => Some(StorageAdminAction::Statistics),
        [command, owner] if !owner.is_empty() && owner.len() <= 4096 => {
            if command.eq_ignore_ascii_case("show") {
                Some(StorageAdminAction::Inspect((*owner).into()))
            } else if command.eq_ignore_ascii_case("wipe") {
                Some(StorageAdminAction::Wipe((*owner).into()))
            } else {
                None
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn storage_commands_require_explicit_account_and_valid_syntax() {
        assert_eq!(parse(&[]), Some(StorageAdminAction::Statistics));
        assert_eq!(
            parse(&["SHOW", "did:key:alice"]),
            Some(StorageAdminAction::Inspect("did:key:alice".into()))
        );
        assert_eq!(
            parse(&["wipe", "did:key:alice"]),
            Some(StorageAdminAction::Wipe("did:key:alice".into()))
        );
        for args in [
            vec!["wipe"],
            vec!["wipe", ""],
            vec!["wipe", "alice", "extra"],
            vec!["unknown", "alice"],
        ] {
            assert_eq!(parse(&args), None);
        }
    }
}
