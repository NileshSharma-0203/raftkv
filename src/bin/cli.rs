//! Command-line client.

use raftkv::client::*;
use raftkv::raft::Role;
use std::time::Duration;

const USAGE: &str = "raftkv-cli --nodes 1=HOST:PORT,2=HOST:PORT,... COMMAND

Commands:
  put KEY VALUE
  get KEY
  del KEY
  append KEY VALUE
  cas KEY EXPECTED VALUE     (use - for \"key must not exist\")
  status";

fn main() {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 3 || args[0] != "--nodes" {
        eprintln!("{USAGE}");
        std::process::exit(2);
    }
    let nodes: Vec<(u64, String)> = args[1]
        .split(',')
        .map(|p| {
            let (id, a) = p.split_once('=').expect("--nodes must be ID=HOST:PORT,...");
            (id.parse().expect("numeric node id"), a.to_string())
        })
        .collect();
    args.drain(..2);

    if args[0] == "status" {
        let mut unreachable = false;
        for (id, addr) in &nodes {
            match status(addr, false, Duration::from_secs(1)) {
                Ok(s) => println!(
                    "node={} role={} term={} leader={:?} commit={} applied={} last={} snapshot={} keys={} sessions={}",
                    s.id,
                    Role::name(s.role),
                    s.term,
                    s.leader,
                    s.commit,
                    s.applied,
                    s.last_index,
                    s.snapshot_index,
                    s.keys,
                    s.sessions
                ),
                Err(e) => {
                    unreachable = true;
                    println!("node={id} unreachable: {e}");
                }
            }
        }
        if unreachable {
            std::process::exit(1);
        }
        return;
    }

    let mut c = Client::new(nodes, ClientOptions::default());
    let a: Vec<&[u8]> = args.iter().map(|s| s.as_bytes()).collect();
    let result: Result<String, ClientError> = match (args[0].as_str(), a.len()) {
        ("put", 3) => c.put(a[1], a[2]).map(|_| "OK".into()),
        ("append", 3) => c.append(a[1], a[2]).map(|_| "OK".into()),
        ("del", 2) => c.delete(a[1]).map(|_| "OK".into()),
        ("get", 2) => c.get(a[1]).map(|v| match v {
            Some(b) => String::from_utf8_lossy(&b).to_string(),
            None => "(nil)".into(),
        }),
        ("cas", 4) => {
            let expected = if args[2] == "-" { None } else { Some(a[2]) };
            c.cas(a[1], expected, a[3]).map(|s| if s { "SWAPPED".into() } else { "NOT SWAPPED".into() })
        }
        _ => {
            eprintln!("{USAGE}");
            std::process::exit(2);
        }
    };
    match result {
        Ok(s) => println!("{s}"),
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(match e {
                ClientError::Indeterminate(_) => 3,
                ClientError::Failed(_) => 1,
            });
        }
    }
}
