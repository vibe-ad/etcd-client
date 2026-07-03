//! Integration tests for the `failover` feature. The whole file compiles away
//! when the feature is off. They target a single etcd on `DEFAULT_TEST_ENDPOINT`
//! plus a dead port, so failover is exercised deterministically without needing
//! a multi-node cluster to kill.
#![cfg(feature = "failover")]

mod testing;

use crate::testing::{get_client, Result, DEFAULT_TEST_ENDPOINT};
use etcd_client::{
    Client, Compare, CompareOp, ConnectOptions, DeleteOptions, EventType, LeaseKeepAliveStream,
    LeaseKeeper, Txn, TxnOp, WatchStream,
};
use std::time::Duration;

/// A closed port on localhost: connecting to it fails fast with a refused
/// connection, the cleanest stand-in for a down node.
const DEAD_ENDPOINT: &str = "127.0.0.1:2999";

fn dead_and_healthy() -> [String; 2] {
    [DEAD_ENDPOINT.to_string(), DEFAULT_TEST_ENDPOINT.to_string()]
}

/// A second closed port, to prove a pool with more than one dead endpoint still
/// finds the single healthy node.
const DEAD_ENDPOINT_2: &str = "127.0.0.1:2998";

/// Client ports of the local 3-node cluster the `#[ignore]` tests expect. See
/// the doc comment on `follower_kill_reads_and_idempotent_writes_continue` for
/// how to bring it up.
fn three_node_cluster() -> [String; 3] {
    [
        "localhost:2379".to_string(),
        "localhost:2381".to_string(),
        "localhost:2383".to_string(),
    ]
}

/// Opens a watch, retrying the open until it establishes. Used only by the
/// reconnect-disabled test: with reconnection off the initial open is
/// single-shot, so against a dead-containing pool the balancer can route it to
/// the dead node. A real caller retries, so does this. (With reconnection on,
/// the library fails the open over itself and this is unnecessary.)
async fn open_watch(client: &mut Client, key: &str) -> Result<WatchStream> {
    let mut last = None;
    for _ in 0..40 {
        match client.watch(key, None).await {
            Ok(stream) => return Ok(stream),
            Err(e) => last = Some(e),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(last.expect("watch attempted at least once"))
}

/// Opens a lease keep-alive, retrying the open until it establishes. Used only
/// by the reconnect-disabled test, for the same reason as `open_watch`.
async fn open_keep_alive(
    client: &mut Client,
    id: i64,
) -> Result<(LeaseKeeper, LeaseKeepAliveStream)> {
    let mut last = None;
    for _ in 0..40 {
        match client.lease_keep_alive(id).await {
            Ok(pair) => return Ok(pair),
            Err(e) => last = Some(e),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(last.expect("keep-alive attempted at least once"))
}

/// Reads and idempotent writes succeed when a dead endpoint sits in the pool
/// alongside a healthy one: the request is retried around the dead node.
#[tokio::test]
async fn dead_endpoint_reads_and_writes_succeed() -> Result<()> {
    let options = ConnectOptions::new().with_connect_timeout(Duration::from_secs(1));
    let mut client = Client::connect(dead_and_healthy(), Some(options)).await?;

    for i in 0..20 {
        let key = format!("failover/{i}");
        client.put(key.clone(), "v", None).await?;
        let resp = client.get(key.clone(), None).await?;
        assert_eq!(resp.kvs().first().map(|kv| kv.value()), Some(&b"v"[..]));
    }

    client
        .delete("failover/", Some(DeleteOptions::new().with_prefix()))
        .await?;
    Ok(())
}

/// With retry disabled, a single healthy endpoint still works normally.
#[tokio::test]
async fn retry_disabled_still_works() -> Result<()> {
    let options = ConnectOptions::new().with_retries(0);
    let mut client = Client::connect([DEFAULT_TEST_ENDPOINT], Some(options)).await?;

    client.put("no-retry", "v", None).await?;
    let resp = client.get("no-retry", None).await?;
    assert_eq!(resp.kvs().first().map(|kv| kv.value()), Some(&b"v"[..]));

    client.delete("no-retry", None).await?;
    Ok(())
}

/// Authenticated operations flow through the retry-and-reauth path. Mutates
/// global auth state, so it is ignored by default. Run in isolation:
/// `cargo test --features failover --test failover -- --ignored auth_ops`.
#[ignore]
#[tokio::test]
async fn auth_ops_are_reliable() -> Result<()> {
    let mut client = get_client().await?;
    // Root user + role are required before auth can be enabled. Tolerate them
    // already existing from a prior run.
    let _ = client.user_add("root", "rootpw", None).await;
    let _ = client.role_add("root").await;
    let _ = client.user_grant_role("root", "root").await;
    client.auth_enable().await?;

    let options = ConnectOptions::new().with_user("root", "rootpw");
    let mut authed = Client::connect([DEFAULT_TEST_ENDPOINT], Some(options)).await?;
    authed.put("auth-reliability", "v", None).await?;
    let resp = authed.get("auth-reliability", None).await?;
    assert_eq!(resp.kvs().first().map(|kv| kv.value()), Some(&b"v"[..]));

    // Restore an open cluster for the rest of the suite.
    authed.auth_disable().await?;
    authed.delete("auth-reliability", None).await?;
    Ok(())
}

/// A mutating op (a compare-and-put txn) succeeds with a dead endpoint in the
/// pool. Txn is only retried when it provably never reached a server, so this
/// proves the balancer routes the write to the healthy node.
#[tokio::test]
async fn mutating_txn_succeeds_around_dead_endpoint() -> Result<()> {
    let options = ConnectOptions::new().with_connect_timeout(Duration::from_secs(1));
    let mut client = Client::connect(dead_and_healthy(), Some(options)).await?;

    let key = "failover-txn/cas";
    // Start clean so the create-if-absent compare is deterministic across reruns.
    client.delete(key, None).await?;

    let txn = Txn::new()
        .when(&[Compare::version(key, CompareOp::Equal, 0)][..])
        .and_then(&[TxnOp::put(key, "v", None)][..])
        .or_else(&[TxnOp::get(key, None)][..]);
    let resp = client.txn(txn).await?;
    assert!(resp.succeeded());

    let got = client.get(key, None).await?;
    assert_eq!(got.kvs().first().map(|kv| kv.value()), Some(&b"v"[..]));

    client.delete(key, None).await?;
    Ok(())
}

/// Two dead endpoints plus one healthy endpoint: reads and writes still succeed
/// because the balancer settles on the only reachable node.
#[tokio::test]
async fn multiple_dead_endpoints_one_healthy() -> Result<()> {
    // Two of three endpoints are dead, so a wider retry budget is needed to
    // sweep past them to the single healthy node.
    let options = ConnectOptions::new()
        .with_connect_timeout(Duration::from_secs(1))
        .with_retries(40)
        .with_retry_backoff(Duration::from_millis(5), 0.0);
    let endpoints = [DEAD_ENDPOINT, DEAD_ENDPOINT_2, DEFAULT_TEST_ENDPOINT];
    let mut client = Client::connect(endpoints, Some(options)).await?;

    for i in 0..10 {
        let key = format!("failover-multi/{i}");
        client.put(key.clone(), "v", None).await?;
        let resp = client.get(key, None).await?;
        assert_eq!(resp.kvs().first().map(|kv| kv.value()), Some(&b"v"[..]));
    }

    client
        .delete("failover-multi/", Some(DeleteOptions::new().with_prefix()))
        .await?;
    Ok(())
}

/// A watch created through a pool that contains a dead endpoint still receives
/// events: with reconnection on, the initial open itself fails over to the
/// healthy node, so a plain `watch()` succeeds without caller-side retry.
#[tokio::test]
async fn watch_through_dead_endpoint_receives_events() -> Result<()> {
    let options = ConnectOptions::new().with_connect_timeout(Duration::from_secs(1));
    let mut client = Client::connect(dead_and_healthy(), Some(options)).await?;

    let key = "failover-watch/k";
    let mut stream = client.watch(key, None).await?;
    // First message is the create ack. Awaiting it guarantees the watch is
    // registered before the put, so the event cannot be missed.
    let created = stream.message().await?.expect("watch create response");
    assert!(created.created());
    let watch_id = created.watch_id();

    client.put(key, "v1", None).await?;
    let resp = stream.message().await?.expect("watch event");
    assert_eq!(resp.events().len(), 1);
    let event = &resp.events()[0];
    assert_eq!(event.event_type(), EventType::Put);
    assert_eq!(event.kv().map(|kv| kv.value()), Some(&b"v1"[..]));

    stream.cancel(watch_id).await?;
    client.delete(key, None).await?;
    Ok(())
}

/// A lease grant and keep-alive established through a pool with a dead endpoint
/// work: with reconnection on, the initial open fails over, so a plain
/// `lease_keep_alive()` succeeds and the response echoes a positive ttl.
#[tokio::test]
async fn lease_keep_alive_through_dead_endpoint() -> Result<()> {
    let options = ConnectOptions::new().with_connect_timeout(Duration::from_secs(1));
    let mut client = Client::connect(dead_and_healthy(), Some(options)).await?;

    let grant = client.lease_grant(60, None).await?;
    assert_eq!(grant.ttl(), 60);
    let id = grant.id();

    let (mut keeper, mut stream) = client.lease_keep_alive(id).await?;
    keeper.keep_alive().await?;
    let resp = stream.message().await?.expect("keep-alive response");
    assert_eq!(resp.id(), id);
    assert!(resp.ttl() > 0);

    client.lease_revoke(id).await?;
    Ok(())
}

/// Custom retry config (explicit attempt count and backoff) connects and
/// operates correctly around a dead endpoint.
#[tokio::test]
async fn custom_retry_config_connects_and_operates() -> Result<()> {
    let options = ConnectOptions::new()
        .with_connect_timeout(Duration::from_secs(1))
        .with_retries(5)
        .with_retry_backoff(Duration::from_millis(10), 0.1);
    let mut client = Client::connect(dead_and_healthy(), Some(options)).await?;

    let key = "failover-retry/k";
    client.put(key, "v", None).await?;
    let resp = client.get(key, None).await?;
    assert_eq!(resp.kvs().first().map(|kv| kv.value()), Some(&b"v"[..]));

    client.delete(key, None).await?;
    Ok(())
}

/// Removing then re-adding the healthy endpoint works with failover enabled,
/// mirroring the base `test_remove_and_add_endpoint`. The dead endpoint stays
/// in the pool throughout.
#[tokio::test]
async fn remove_and_add_endpoint_with_failover() -> Result<()> {
    let options = ConnectOptions::new()
        .with_connect_timeout(Duration::from_secs(1))
        .with_retries(5);
    let mut client = Client::connect(dead_and_healthy(), Some(options)).await?;

    let key = "failover-endpoint/k";
    client.put(key, "v", None).await?;

    // A get between the remove and add would have no reachable endpoint, so add
    // the healthy node back before reading (same ordering as the base test).
    client.remove_endpoint(DEFAULT_TEST_ENDPOINT).await?;
    client.add_endpoint(DEFAULT_TEST_ENDPOINT).await?;

    let resp = client.get(key, None).await?;
    assert_eq!(resp.kvs().first().map(|kv| kv.value()), Some(&b"v"[..]));

    client.delete(key, None).await?;
    Ok(())
}

/// Opting out of stream reconnection is not broken: with watch and keep-alive
/// reconnect disabled, both streams still work normally on a healthy node.
#[tokio::test]
async fn stream_reconnect_disabled_still_works() -> Result<()> {
    let options = ConnectOptions::new()
        .with_connect_timeout(Duration::from_secs(1))
        .with_watch_reconnect(false)
        .with_lease_keepalive_reconnect(false);
    let mut client = Client::connect(dead_and_healthy(), Some(options)).await?;

    let key = "failover-noreconnect/k";
    let mut stream = open_watch(&mut client, key).await?;
    let created = stream.message().await?.expect("watch create response");
    let watch_id = created.watch_id();
    client.put(key, "v", None).await?;
    let resp = stream.message().await?.expect("watch event");
    assert_eq!(resp.events()[0].event_type(), EventType::Put);
    stream.cancel(watch_id).await?;

    let grant = client.lease_grant(60, None).await?;
    let id = grant.id();
    let (mut keeper, mut lease_stream) = open_keep_alive(&mut client, id).await?;
    keeper.keep_alive().await?;
    let ka = lease_stream.message().await?.expect("keep-alive response");
    assert!(ka.ttl() > 0);
    client.lease_revoke(id).await?;

    client.delete(key, None).await?;
    Ok(())
}

/// All endpoints dead surfaces an error instead of hanging. Closed ports refuse
/// instantly, so a short connect timeout plus a bounded retry budget make the
/// failure fast and deterministic. The tokio guard only turns an unexpected
/// hang into a failed test rather than a stuck suite.
#[tokio::test]
async fn all_endpoints_dead_errors_without_hanging() -> Result<()> {
    let attempt = tokio::time::timeout(Duration::from_secs(10), async {
        let options = ConnectOptions::new()
            .with_connect_timeout(Duration::from_millis(200))
            .with_timeout(Duration::from_millis(200))
            .with_retries(2)
            .with_retry_backoff(Duration::from_millis(10), 0.0);
        let mut client = Client::connect([DEAD_ENDPOINT, DEAD_ENDPOINT_2], Some(options)).await?;
        client.get("failover-alldead/probe", None).await
    })
    .await;

    let inner = attempt.expect("all-dead operation hung past the guard");
    assert!(inner.is_err(), "every endpoint dead must surface an error");
    Ok(())
}

/// Reads and idempotent writes continue when a cluster follower is killed
/// mid-run. Needs a local 3-node cluster and manual node kill, so it is ignored
/// by default.
///
/// Bring up the cluster (peer ports offset high to avoid collisions):
///
/// ```bash
/// etcd --name n1 --data-dir /tmp/etcd-n1 \
///   --listen-client-urls http://127.0.0.1:2379 --advertise-client-urls http://127.0.0.1:2379 \
///   --listen-peer-urls http://127.0.0.1:2390 --initial-advertise-peer-urls http://127.0.0.1:2390 \
///   --initial-cluster n1=http://127.0.0.1:2390,n2=http://127.0.0.1:2392,n3=http://127.0.0.1:2394 \
///   --initial-cluster-state new
/// # n2 on client 2381 / peer 2392, n3 on client 2383 / peer 2394, same initial-cluster
/// ```
///
/// Run with `cargo test --features failover --test failover -- --ignored`,
/// then `kill` one follower process while the loop runs.
#[ignore]
#[tokio::test]
async fn follower_kill_reads_and_idempotent_writes_continue() -> Result<()> {
    let options = ConnectOptions::new()
        .with_connect_timeout(Duration::from_secs(1))
        .with_timeout(Duration::from_secs(2))
        .with_retries(5);
    let mut client = Client::connect(three_node_cluster(), Some(options)).await?;

    // Kill one follower while this loop runs. Puts (idempotent) and gets must
    // keep succeeding by failing over to a surviving node.
    for i in 0..40 {
        let key = format!("failover-cluster-kill/{i}");
        client.put(key.clone(), "v", None).await?;
        let resp = client.get(key, None).await?;
        assert_eq!(resp.kvs().first().map(|kv| kv.value()), Some(&b"v"[..]));
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    client
        .delete(
            "failover-cluster-kill/",
            Some(DeleteOptions::new().with_prefix()),
        )
        .await?;
    Ok(())
}

/// A watch survives the node hosting it going down and resumes delivering
/// events with no gaps. Needs the same 3-node cluster as
/// `follower_kill_reads_and_idempotent_writes_continue`, run with
/// `cargo test --features failover --test failover -- --ignored`, then kill a
/// node while the loop runs.
#[ignore]
#[tokio::test]
async fn watch_survives_node_down_and_resumes() -> Result<()> {
    let options = ConnectOptions::new()
        .with_connect_timeout(Duration::from_secs(1))
        .with_timeout(Duration::from_secs(2))
        .with_retries(5);
    let mut client = Client::connect(three_node_cluster(), Some(options)).await?;

    let key = "failover-watch-survive/k";
    client.delete(key, None).await?;
    let mut stream = client.watch(key, None).await?;
    let created = stream.message().await?.expect("watch create response");
    let watch_id = created.watch_id();

    // Each put overwrites the key with its index. Every event must arrive once
    // and in order even across a reconnect, so no index may be skipped.
    for i in 0..40 {
        client.put(key, i.to_string(), None).await?;
        let resp = stream.message().await?.expect("watch event");
        let event = &resp.events()[0];
        assert_eq!(event.event_type(), EventType::Put);
        assert_eq!(
            event.kv().map(|kv| kv.value()),
            Some(i.to_string().as_bytes())
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    stream.cancel(watch_id).await?;
    client.delete(key, None).await?;
    Ok(())
}

/// A lease keep-alive survives the node hosting it going down. The 30s ttl and
/// 500ms cadence keep the lease alive across a brief reconnect. Needs the same
/// 3-node cluster as `follower_kill_reads_and_idempotent_writes_continue`, run
/// with `cargo test --features failover --test failover -- --ignored`, then
/// kill a node while the loop runs.
#[ignore]
#[tokio::test]
async fn lease_keep_alive_survives_node_down() -> Result<()> {
    let options = ConnectOptions::new()
        .with_connect_timeout(Duration::from_secs(1))
        .with_timeout(Duration::from_secs(2))
        .with_retries(5);
    let mut client = Client::connect(three_node_cluster(), Some(options)).await?;

    let grant = client.lease_grant(30, None).await?;
    let id = grant.id();

    let (mut keeper, mut stream) = client.lease_keep_alive(id).await?;
    for _ in 0..40 {
        keeper.keep_alive().await?;
        let resp = stream.message().await?.expect("keep-alive response");
        assert_eq!(resp.id(), id);
        assert!(resp.ttl() > 0);
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    client.lease_revoke(id).await?;
    Ok(())
}
