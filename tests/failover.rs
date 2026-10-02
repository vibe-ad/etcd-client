//! Integration tests for the `failover` feature. The whole file compiles away
//! when the feature is off. They target a single etcd on `DEFAULT_TEST_ENDPOINT`
//! plus a dead port, so failover is exercised deterministically without needing
//! a multi-node cluster to kill.
#![cfg(feature = "failover")]

mod testing;

use crate::testing::{get_client, Result, DEFAULT_TEST_ENDPOINT};
use etcd_client::{
    Client, Compare, CompareOp, ConnectOptions, DeleteOptions, Error, EventType, GetOptions,
    LeaseKeepAliveStream, LeaseKeeper, Txn, TxnOp, WatchOptions, WatchResponse, WatchStream,
};
use serial_test::{parallel, serial};
use std::future::Future;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{AbortHandle, JoinSet};

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
/// the doc comment on `follower_kill_reads_continue_and_writes_stay_at_most_once` for
/// how to bring it up.
fn three_node_cluster() -> [String; 3] {
    [
        "localhost:2379".to_string(),
        "localhost:2381".to_string(),
        "localhost:2383".to_string(),
    ]
}

/// A TCP proxy in front of `DEFAULT_TEST_ENDPOINT` that can drop every
/// connection on demand. Pointing a client at it lets a test break a live
/// watch or keep-alive stream deterministically, without a multi-node cluster.
/// Dropping it also closes the listener, so the endpoint dies under established
/// connections the way a restarted member does, unlike a port that never
/// listened.
struct Proxy {
    endpoint: String,
    conns: Arc<Mutex<Vec<AbortHandle>>>,
    accepted: Arc<AtomicUsize>,
    /// When non-zero, every new connection is dropped after this many ms.
    flap_ms: Arc<AtomicU64>,
    accept_task: AbortHandle,
}

impl Proxy {
    async fn start() -> Proxy {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
        let endpoint = listener.local_addr().expect("proxy addr").to_string();
        let conns = Arc::new(Mutex::new(Vec::new()));
        let accepted = Arc::new(AtomicUsize::new(0));
        let flap_ms = Arc::new(AtomicU64::new(0));
        let accept_task = {
            let (conns, accepted, flap_ms) = (conns.clone(), accepted.clone(), flap_ms.clone());
            tokio::spawn(async move {
                while let Ok((mut down, _)) = listener.accept().await {
                    accepted.fetch_add(1, Ordering::SeqCst);
                    let hold = flap_ms.load(Ordering::SeqCst);
                    let conn = tokio::spawn(async move {
                        let Ok(mut up) = TcpStream::connect(DEFAULT_TEST_ENDPOINT).await else {
                            return;
                        };
                        let pipe = tokio::io::copy_bidirectional(&mut down, &mut up);
                        if hold == 0 {
                            pipe.await.ok();
                        } else {
                            tokio::time::timeout(Duration::from_millis(hold), pipe)
                                .await
                                .ok();
                        }
                    });
                    conns.lock().unwrap().push(conn.abort_handle());
                }
            })
            .abort_handle()
        };
        Proxy {
            endpoint,
            conns,
            accepted,
            flap_ms,
            accept_task,
        }
    }

    /// Drops every live connection, breaking any stream running over it.
    fn sever(&self) {
        for conn in self.conns.lock().unwrap().drain(..) {
            conn.abort();
        }
    }

    /// From now on, drops every new connection `hold` after it is accepted.
    fn flap(&self, hold: Duration) {
        self.flap_ms
            .store(hold.as_millis() as u64, Ordering::SeqCst);
    }

    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.accept_task.abort();
        self.sever();
    }
}

/// Waits for the next response that carries events or ends a watch, skipping
/// created acks and progress notifications. Panics with what arrived instead
/// on a timeout, an error or the end of the stream.
async fn next_event(stream: &mut WatchStream, within: Duration) -> WatchResponse {
    let wait = async {
        loop {
            match stream.message().await {
                Ok(Some(resp)) if resp.events().is_empty() && !resp.canceled() => continue,
                other => return other,
            }
        }
    };
    match tokio::time::timeout(within, wait).await {
        Ok(Ok(Some(resp))) => resp,
        other => panic!("expected a watch event, got {other:?}"),
    }
}

/// Runs `body` with auth enabled on the test etcd, handing it a root client.
/// Auth is disabled again afterwards even when `body` fails or panics, so a red
/// test does not leave the shared etcd locked for the rest of the suite. Every
/// caller is `#[serial(auth)]` and every other test `#[parallel(auth)]`.
async fn with_auth<F, Fut>(body: F) -> Result<()>
where
    F: FnOnce(Client) -> Fut,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    let mut root = enable_auth().await?;
    let outcome = tokio::spawn(body(root.clone())).await;
    root.auth_disable().await?;
    match outcome {
        Ok(result) => result,
        Err(e) => std::panic::resume_unwind(e.into_panic()),
    }
}

async fn enable_auth() -> Result<Client> {
    let mut admin = get_client().await?;
    // Root user + role are required before auth can be enabled. Tolerate them
    // already existing from a prior run.
    let _ = admin.user_add("root", "rootpwd", None).await;
    let _ = admin.role_add("root").await;
    let _ = admin.user_grant_role("root", "root").await;
    admin.auth_enable().await?;
    let options = ConnectOptions::new().with_user("root", "rootpwd");
    Client::connect([DEFAULT_TEST_ENDPOINT], Some(options)).await
}

/// Comfortably past the CI fixture's 2s `ETCD_AUTH_TOKEN_TTL`.
const TOKEN_TTL: Duration = Duration::from_secs(4);

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
#[parallel(auth)]
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

/// Reads keep succeeding when an endpoint dies mid-session. Calls in flight at
/// the kill are severed (`Unknown`, broken pipe), later ones hit the refused
/// reconnect (`Unavailable`), and both must fail over. Half the readers go
/// through a raw `KvClient`, which must fail over the same way as `Client`.
#[tokio::test(flavor = "multi_thread")]
#[parallel(auth)]
async fn reads_survive_an_endpoint_killed_mid_session() -> Result<()> {
    let proxy = Proxy::start().await;
    // The balancer keeps offering the refused endpoint, so each attempt is a coin
    // flip. A wide budget keeps 3200 calls from ever losing every flip.
    let options = ConnectOptions::new()
        .with_connect_timeout(Duration::from_secs(1))
        .with_retries(40);
    let endpoints = [proxy.endpoint.clone(), DEFAULT_TEST_ENDPOINT.to_string()];
    let mut client = Client::connect(endpoints, Some(options)).await?;

    let prefix = "failover-killed/";
    for i in 0..10 {
        client.put(format!("{prefix}{i}"), "v", None).await?;
    }

    let mut readers = JoinSet::new();
    for i in 0..16 {
        let mut client = client.clone();
        let mut kv = client.kv_client();
        readers.spawn(async move {
            for _ in 0..200 {
                let options = Some(GetOptions::new().with_prefix());
                let resp = if i % 2 == 0 {
                    client.get(prefix, options).await?
                } else {
                    kv.get(prefix, options).await?
                };
                assert_eq!(resp.kvs().len(), 10);
            }
            Ok::<_, Error>(())
        });
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    drop(proxy);
    while let Some(reader) = readers.join_next().await {
        reader.expect("reader panicked")?;
    }

    client
        .delete(prefix, Some(DeleteOptions::new().with_prefix()))
        .await?;
    Ok(())
}

/// Every sub-client obtained from a getter fails over around a dead endpoint,
/// not only the calls made on `Client`.
#[tokio::test]
#[parallel(auth)]
async fn sub_clients_fail_over_around_dead_endpoint() -> Result<()> {
    let options = ConnectOptions::new().with_connect_timeout(Duration::from_secs(1));
    let client = Client::connect(dead_and_healthy(), Some(options)).await?;
    let (mut kv, mut lease, mut auth, mut maintenance, mut cluster) = (
        client.kv_client(),
        client.lease_client(),
        client.auth_client(),
        client.maintenance_client(),
        client.cluster_client(),
    );

    for i in 0..20 {
        let key = format!("failover-sub/{i}");
        kv.put(key.clone(), "v", None).await?;
        let resp = kv.get(key, None).await?;
        assert_eq!(resp.kvs().first().map(|kv| kv.value()), Some(&b"v"[..]));
        let grant = lease.grant(60, None).await?;
        lease.revoke(grant.id()).await?;
        auth.role_list().await?;
        maintenance.status().await?;
        assert!(!cluster.member_list().await?.members().is_empty());
    }

    kv.delete("failover-sub/", Some(DeleteOptions::new().with_prefix()))
        .await?;
    Ok(())
}

/// With retry disabled, a single healthy endpoint still works normally.
#[tokio::test]
#[parallel(auth)]
async fn retry_disabled_still_works() -> Result<()> {
    let options = ConnectOptions::new().with_retries(0);
    let mut client = Client::connect([DEFAULT_TEST_ENDPOINT], Some(options)).await?;

    client.put("no-retry", "v", None).await?;
    let resp = client.get("no-retry", None).await?;
    assert_eq!(resp.kvs().first().map(|kv| kv.value()), Some(&b"v"[..]));

    client.delete("no-retry", None).await?;
    Ok(())
}

/// Authenticated operations flow through the retry-and-reauth path.
#[tokio::test]
#[serial(auth)]
async fn auth_ops_are_reliable() -> Result<()> {
    with_auth(|_root| async move {
        let options = ConnectOptions::new().with_user("root", "rootpwd");
        let mut authed = Client::connect([DEFAULT_TEST_ENDPOINT], Some(options)).await?;
        authed.put("auth-reliability", "v", None).await?;
        let resp = authed.get("auth-reliability", None).await?;
        assert_eq!(resp.kvs().first().map(|kv| kv.value()), Some(&b"v"[..]));
        authed.delete("auth-reliability", None).await?;
        Ok(())
    })
    .await
}

/// Credentials installed after connect are honoured by the reauth path: the
/// client was built from options carrying no user, so only the live credential
/// state can tell `do_call` there is something to refresh.
///
/// Expects an etcd started with a short `ETCD_AUTH_TOKEN_TTL` (the CI value is
/// 2s), otherwise the sleep never crosses an expiry and the test is vacuous.
#[tokio::test]
#[serial(auth)]
async fn reauth_after_update_user() -> Result<()> {
    with_auth(|_root| async move {
        // Connect anonymously, then install the credentials. `ConnectOptions::user`
        // stays `None` for the client's whole life.
        let mut client = Client::connect([DEFAULT_TEST_ENDPOINT], None).await?;
        client
            .update_user(Some(("root".to_string(), "rootpwd".to_string())))
            .await?;
        client.put("reauth-after-update", "v", None).await?;

        tokio::time::sleep(TOKEN_TTL).await;
        let resp = client.get("reauth-after-update", None).await?;
        assert_eq!(resp.kvs().first().map(|kv| kv.value()), Some(&b"v"[..]));

        // Dropping the credentials must stop the reauth path rather than refresh
        // back into the stale ones.
        client.update_user(None).await?;
        client.get("reauth-after-update", None).await.unwrap_err();

        client
            .update_user(Some(("root".to_string(), "rootpwd".to_string())))
            .await?;
        client.delete("reauth-after-update", None).await?;
        Ok(())
    })
    .await
}

/// A mutating op (a compare-and-put txn) succeeds with a dead endpoint in the
/// pool. Txn is only retried when it provably never reached a server, so this
/// proves the balancer routes the write to the healthy node.
#[tokio::test]
#[parallel(auth)]
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
#[parallel(auth)]
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
#[parallel(auth)]
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
#[parallel(auth)]
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
#[parallel(auth)]
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
#[parallel(auth)]
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
#[parallel(auth)]
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
#[parallel(auth)]
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

/// Reads continue when a cluster follower is killed mid-run, and writes stay
/// at most once: a put in flight on the killed node may fail, since it may
/// have applied, and the caller decides whether to repeat it. Needs a local 3-node cluster and manual node kill, so it is ignored
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
#[parallel(auth)]
async fn follower_kill_reads_continue_and_writes_stay_at_most_once() -> Result<()> {
    let options = ConnectOptions::new()
        .with_connect_timeout(Duration::from_secs(1))
        .with_timeout(Duration::from_secs(2))
        .with_retries(5);
    let mut client = Client::connect(three_node_cluster(), Some(options)).await?;

    // Kill one follower while this loop runs. Gets must keep succeeding by
    // failing over to a surviving node. A failed put is repeated here, which is
    // the caller's call to make for an idempotent value.
    for i in 0..40 {
        let key = format!("failover-cluster-kill/{i}");
        if client.put(key.clone(), "v", None).await.is_err() {
            client.put(key.clone(), "v", None).await?;
        }
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
/// `follower_kill_reads_continue_and_writes_stay_at_most_once`, run with
/// `cargo test --features failover --test failover -- --ignored`, then kill a
/// node while the loop runs.
#[ignore]
#[tokio::test]
#[parallel(auth)]
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
/// 3-node cluster as `follower_kill_reads_continue_and_writes_stay_at_most_once`, run
/// with `cargo test --features failover --test failover -- --ignored`, then
/// kill a node while the loop runs.
#[ignore]
#[tokio::test]
#[parallel(auth)]
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

/// A create the server rejects is answered with `watch_id = -1`, not the id the
/// driver assigned. Once rejected it must not be replayed: with no watch left,
/// a broken stream ends instead of resubscribing the doomed create.
#[tokio::test]
#[parallel(auth)]
async fn rejected_watch_is_not_replayed_after_reconnect() -> Result<()> {
    let proxy = Proxy::start().await;
    let options = ConnectOptions::new().with_connect_timeout(Duration::from_secs(1));
    let mut client = Client::connect([proxy.endpoint.as_str()], Some(options)).await?;

    // key >= range_end, so etcd rejects the range as empty.
    let options = WatchOptions::new().with_range("failover-rejected/a");
    let mut stream = client.watch("failover-rejected/b", Some(options)).await?;
    let rejected = stream.message().await?.expect("create response");
    assert!(
        rejected.created() && rejected.canceled(),
        "the create must be rejected: {rejected:?}"
    );

    proxy.sever();
    let next = tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .expect("stream neither ended nor replayed within 5s");
    assert!(
        matches!(next, Ok(None)),
        "a rejected create must not be replayed on reconnect, got {next:?}"
    );
    Ok(())
}

/// A create reusing a live watch's id is rejected by the server. The rejection
/// must leave the live watch's registration intact, so after a reconnect it
/// still delivers events for its own key rather than the rejected one's.
#[tokio::test]
#[parallel(auth)]
async fn duplicate_watch_id_keeps_existing_watch_after_reconnect() -> Result<()> {
    let proxy = Proxy::start().await;
    let options = ConnectOptions::new().with_connect_timeout(Duration::from_secs(1));
    let mut client = Client::connect([proxy.endpoint.as_str()], Some(options)).await?;
    let mut writer = get_client().await?;

    let (key, other) = ("failover-dup-id/watched", "failover-dup-id/other");
    let mut stream = client.watch(key, None).await?;
    let created = stream.message().await?.expect("watch create response");
    let watch_id = created.watch_id();

    let dup = WatchOptions::new().with_watch_id(watch_id);
    stream.watch(other, Some(dup)).await?;
    let rejected = stream.message().await?.expect("duplicate create response");
    assert!(
        rejected.created() && rejected.canceled(),
        "a duplicate id must be rejected: {rejected:?}"
    );

    proxy.sever();
    writer.put(key, "v", None).await?;
    let resp = next_event(&mut stream, Duration::from_secs(10)).await;
    assert!(!resp.canceled(), "watch canceled: {resp:?}");
    assert_eq!(resp.watch_id(), watch_id);
    assert_eq!(
        resp.events()[0].kv().map(|kv| kv.key()),
        Some(key.as_bytes())
    );

    writer.delete(key, None).await?;
    Ok(())
}

/// A stream that establishes and then breaks straight away must back off
/// harder on each cycle. The `created` ack echoed by the replay is not proof of
/// a healthy stream, so it must not reset the backoff to its base delay.
#[tokio::test]
#[parallel(auth)]
async fn watch_reconnect_backs_off_while_stream_flaps() -> Result<()> {
    let proxy = Proxy::start().await;
    let options = ConnectOptions::new()
        .with_connect_timeout(Duration::from_secs(1))
        .with_retry_backoff(Duration::from_millis(25), 0.0);
    let mut client = Client::connect([proxy.endpoint.as_str()], Some(options)).await?;

    let mut stream = client.watch("failover-flap/k", None).await?;
    stream.message().await?.expect("watch create response");

    // Each reconnect gets 100ms, ample for the replayed created ack to arrive.
    proxy.flap(Duration::from_millis(100));
    proxy.sever();
    let before = proxy.accepted();
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let reconnects = proxy.accepted() - before;

    // Growing from 25ms (25, 50, 100, ... plus the 100ms hold) fits about 6
    // reconnects in the window. Resetting to 25ms on every ack fits about 20.
    assert!(
        reconnects <= 10,
        "{reconnects} reconnects in 2.5s: backoff is not growing"
    );
    drop(stream);
    Ok(())
}

/// The lease variant: the response to the keep-alive a reconnect re-primes is
/// not proof of a healthy stream either.
#[tokio::test]
#[parallel(auth)]
async fn lease_reconnect_backs_off_while_stream_flaps() -> Result<()> {
    let proxy = Proxy::start().await;
    let options = ConnectOptions::new()
        .with_connect_timeout(Duration::from_secs(1))
        .with_retry_backoff(Duration::from_millis(25), 0.0);
    let mut client = Client::connect([proxy.endpoint.as_str()], Some(options)).await?;

    let id = client.lease_grant(60, None).await?.id();
    let (_keeper, _stream) = client.lease_keep_alive(id).await?;

    proxy.flap(Duration::from_millis(100));
    proxy.sever();
    let before = proxy.accepted();
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let reconnects = proxy.accepted() - before;
    assert!(
        reconnects <= 10,
        "{reconnects} reconnects in 2.5s: backoff is not growing"
    );

    get_client().await?.lease_revoke(id).await?;
    Ok(())
}

/// The keep-alive a reconnect sends to re-prime the lease is the driver's, so
/// its response must not reach the caller: one `keep_alive()` gets one response.
#[tokio::test]
#[parallel(auth)]
async fn lease_reconnect_adds_no_unrequested_response() -> Result<()> {
    let proxy = Proxy::start().await;
    let options = ConnectOptions::new().with_connect_timeout(Duration::from_secs(1));
    let mut client = Client::connect([proxy.endpoint.as_str()], Some(options)).await?;

    let id = client.lease_grant(60, None).await?.id();
    let (mut keeper, mut stream) = client.lease_keep_alive(id).await?;
    keeper.keep_alive().await?;
    stream.message().await?.expect("keep-alive response");

    proxy.sever();
    // Long enough for the driver to reconnect and re-prime the lease.
    tokio::time::sleep(Duration::from_secs(1)).await;
    keeper.keep_alive().await?;
    let resp = tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .expect("keep-alive response")?
        .expect("keep-alive response");
    assert!(resp.ttl() > 0);
    let extra = tokio::time::timeout(Duration::from_secs(1), stream.message()).await;
    assert!(extra.is_err(), "unrequested keep-alive response: {extra:?}");

    client.lease_revoke(id).await?;
    Ok(())
}

/// A watch reconnecting after its auth token expired must re-authenticate and
/// resume. The client issues no unary call in between, so nothing else
/// refreshes the token.
#[tokio::test]
#[serial(auth)]
async fn watch_reconnect_refreshes_expired_token() -> Result<()> {
    with_auth(|mut root| async move {
        let proxy = Proxy::start().await;
        let options = ConnectOptions::new()
            .with_connect_timeout(Duration::from_secs(1))
            .with_user("root", "rootpwd");
        let mut client = Client::connect([proxy.endpoint.as_str()], Some(options)).await?;

        let key = "failover-watch-reauth/k";
        let mut stream = client.watch(key, None).await?;
        stream.message().await?.expect("watch create response");

        tokio::time::sleep(TOKEN_TTL).await;
        proxy.sever();
        root.put(key, "v", None).await?;

        let resp = next_event(&mut stream, Duration::from_secs(10)).await;
        assert!(!resp.canceled(), "watch canceled: {resp:?}");
        assert_eq!(
            resp.events()[0].kv().map(|kv| kv.key()),
            Some(key.as_bytes())
        );
        root.delete(key, None).await?;
        Ok(())
    })
    .await
}

/// A lease keep-alive reconnecting after its auth token expired must keep
/// renewing. etcd 3.5 does not check the token on keep-alive, later releases
/// do, which the reconnect's token refresh covers.
#[tokio::test]
#[serial(auth)]
async fn lease_reconnect_refreshes_expired_token() -> Result<()> {
    with_auth(|mut root| async move {
        let proxy = Proxy::start().await;
        let options = ConnectOptions::new()
            .with_connect_timeout(Duration::from_secs(1))
            .with_user("root", "rootpwd");
        let mut client = Client::connect([proxy.endpoint.as_str()], Some(options)).await?;

        let id = client.lease_grant(60, None).await?.id();
        let (mut keeper, mut stream) = client.lease_keep_alive(id).await?;
        keeper.keep_alive().await?;
        stream.message().await?.expect("keep-alive response");

        tokio::time::sleep(TOKEN_TTL).await;
        proxy.sever();
        keeper.keep_alive().await?;

        let resp = tokio::time::timeout(Duration::from_secs(10), stream.message()).await;
        match resp {
            Ok(Ok(Some(resp))) => {
                assert_eq!(resp.id(), id);
                assert!(resp.ttl() > 0, "lease lost: {resp:?}");
            }
            other => panic!("expected a keep-alive response, got {other:?}"),
        }
        root.lease_revoke(id).await?;
        Ok(())
    })
    .await
}

/// `with_auto_token_refresh(true)` must keep working for sub-clients.
#[tokio::test]
#[serial(auth)]
async fn sub_client_auto_token_refresh_still_works() -> Result<()> {
    with_auth(|_root| async move {
        let options = ConnectOptions::new()
            .with_user("root", "rootpwd")
            .with_auto_token_refresh(true);
        let client = Client::connect([DEFAULT_TEST_ENDPOINT], Some(options)).await?;
        let mut kv = client.kv_client();

        let key = "failover-subclient-refresh/k";
        kv.put(key, "v", None).await?;
        tokio::time::sleep(TOKEN_TTL).await;
        let resp = kv.get(key, None).await?;
        assert_eq!(resp.kvs().first().map(|kv| kv.value()), Some(&b"v"[..]));
        kv.delete(key, None).await?;
        Ok(())
    })
    .await
}
