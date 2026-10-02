# Review: failover commits after `808f201` (DSP-257)

Reviewed commits, oldest first:

| Commit | Summary |
|---|---|
| `5541d58` | feat(failover): opt-in request-level failover for partial node failures |
| `af8e44f` | feat(failover): warn-log failover activity at each decision point |
| `579d3b8` | feat(failover): bridge failover warns to the log facade |
| `cc40cf7` | test(failover): allow transparent token renewal under the failover feature |
| `fd0ec23` | fix(failover): repair rebase regressions against the ClientCaller layer |

The findings were checked against etcd v3.5.26, configured like CI (`ETCD_AUTH_TOKEN_TTL=2`). Unary
retry policies were compared with Go `clientv3` v3.5.9 `retry.go`. Each confirmed finding has a red
test (see [Red tests](#red-tests)).

Legend: 🔴 must fix · 🟠 should fix · 🟡 minor / follow-up · ✅ good

## Summary

The design is sound. The unary retry policies match Go's `retry.go`, or are stricter (`compact` is
treated as mutating). Write-at-most-once is reasoned through carefully: mid-stream resets are
deliberately excluded from the "not sent" errors. With the feature off, the build stays identical
to upstream.

Fix these before relying on it in production:

1. 🔴 The watch driver matches create responses by id. etcd answers every rejected create with
   `watch_id = -1`, so a rejection is never matched to the create that caused it.
2. 🔴 A watch can't survive a reconnect once its auth token has expired. The watch is dead but the
   stream stays open, so it hangs silently.
3. 🟠 `fd0ec23` turns `refresh_expired_token` off for every caller, not just the failover loop, so
   sub-clients lose token refresh.
4. 🟠 The duplicate `created` ack that comes back after a replay resets the reconnect backoff, so a
   flapping stream reconnects about every 125ms forever.

Also: CI never runs the `failover` tests (see [CI gap](#ci-gap)).

---

## `5541d58` feat(failover): opt-in request-level failover

### What it does

- **`src/failover.rs` (new):** holds the decision logic.
  - `classify()` maps an error to `Retry`, `RefreshToken` or `Stop`.
  - Idempotent RPCs retry on `Unavailable`, `DeadlineExceeded` and `Cancelled`.
  - Mutating RPCs retry only when the error proves the request never reached a server: h2
    `REFUSED_STREAM`, or a connect error like `ConnectionRefused`.
  - Auth-token errors are recognised by their message text, as in Go's `shouldRefreshToken`.
  - Unary backoff copies Go's `roundRobinQuorumBackoff`. Stream reconnects use a separate
    exponential backoff capped at 5s.
- **`src/client.rs`:** the `failover!` macro sends each unary `Client` method through
  `run_failover`, cloning the arguments for every attempt. It also adds `with_retries`,
  `with_retry_backoff`, `with_watch_reconnect` and `with_lease_keepalive_reconnect`.
- **`src/caller.rs`:** the `authenticate` RPC fails over too.
- **`src/rpc/watch.rs`:** `WatchDriver` owns the gRPC stream.
  - It assigns stable client-side watch ids.
  - It tracks a resume revision per watch: it holds still on fragments, handles progress
    notifications, and pins from-now watches to header+1.
  - On reconnect it replays all creates at once and drops the duplicate `created` acks.
- **`src/rpc/lease.rs`:** `LeaseKeepAliveDriver` re-sends a keep-alive for every tracked lease
  after a reconnect.

### Findings

#### 🔴 A rejected watch create is never removed (confirmed)

etcd rejects a create with `watch_id = -1` (`InvalidWatchID`), not the id the client asked for.
This covers an empty range, a duplicate id and, in etcd 3.5, an invalid auth token. Go's client
handles this at `watch.go:460`. It sends creates one at a time and matches each response to the
oldest pending create.

`WatchDriver::record` (`src/rpc/watch.rs`, the `resp.created()` branch) calls
`watches.remove(&-1)`, which removes nothing. Consequences:

- The bad create stays in the registry and is replayed and rejected again on every reconnect.
- The driver never stops, because `watches` is never empty.
- With a duplicate id, `apply_user_request` has already replaced the live watch's entry with the
  new create. After a reconnect, the original watch is resubscribed to the wrong key, silently.

The existing unit test `rejected_create_is_removed_and_forwarded` uses `watch_id: 1`, which the
server never sends.

What etcd actually returned:

```
watch_id: -1, created: true, canceled: true, cancel_reason: "mvcc: watcher range is empty"
```

**Fix:** match create responses to pending creates in send order (a FIFO), not by id. Replaying
creates one at a time, as Go does, makes this easy.

#### 🔴 Watch reconnect can't refresh an expired token (confirmed)

Both drivers treat `Decision::RefreshToken` as permanent (`open_retrying` and `reconnect`). The
comment says "the sub-client cannot refresh a token", but `self.inner` is a `ClientCaller`, and
that has `refresh_token()`.

On a real server the failure doesn't show up as a stream error. The stream opens fine, and etcd
rejects each replayed create:

```
watch_id: -1, created: true, canceled: true,
cancel_reason: "rpc error: code = Unauthenticated desc = etcdserver: invalid auth token"
```

Because of the `-1` finding above, the watch stays registered and the stream stays healthy. No
events arrive and nothing is reported: the watch hangs silently. etcd's default simple-token TTL is
5 minutes. A workload that only watches, with no unary calls refreshing the shared token, hits
this on the first reconnect after that.

**Fix:** once creates are matched in order, treat a rejection whose reason is an auth-token error
(`shouldRetryWatch` in Go) as "refresh, then resubscribe". Also handle `RefreshToken` in
`reconnect` / `open_retrying` by calling `inner.refresh_token()` when credentials are set.

#### ✅ Lease keep-alive survives token expiry (initial finding withdrawn)

The first review said the lease driver had the same token problem. That was wrong: etcd doesn't
check the token on `LeaseKeepAlive`, so the reconnect succeeds with an expired token. A regression
test now guards this.

#### 🟠 Replayed `created` acks reset the reconnect backoff (confirmed)

`run()` sets `reconnect_attempt = 0` on every message, including the duplicate `created` acks it
then drops. A node that accepts the stream and then drops it is retried at the base delay forever.
The measured rate was 20 reconnects in 2.5s with a 25ms base, against about 6 with real
exponential growth. The comment "the delay grows until a response arrives" doesn't hold. The same
reset is in the lease driver.

**Fix:** reset only after a message that is actually forwarded to the caller.

#### 🟠 Fragments are forwarded one by one

The resume point correctly holds until the last fragment arrives. But if the stream breaks
mid-revision, the replay re-sends fragments the caller already received, so the caller gets
duplicates. Go buffers fragments and delivers only the merged response. This only affects callers
who turned on `with_fragment()`.

#### 🟡 A black-holed node isn't covered for streams

A watch or keep-alive stream to a node that silently drops traffic never errors (Go has the same
gap). The docs for `with_retries` recommend `with_timeout`. The streaming docs should likewise
recommend `with_keep_alive`, which sets the HTTP/2 keep-alive.

#### 🟡 The tests mostly exercise the balancer, not the retry loop

A refused port never becomes "ready" in tower's balancer, so the non-ignored "dead endpoint" tests
show that requests are routed around it. Retrying after a failure mid-request is only covered by
the `#[ignore]` 3-node tests.

`follower_kill_reads_and_idempotent_writes_continue` calls puts "idempotent" and expects every put
to succeed when a node is killed. Under write-at-most-once, a put that was in flight on the killed
node should fail, so that test is expected to flake. The proxy added with the red tests can now
exercise the retry loop deterministically.

#### 🟡 Auth retry loops can nest

The `RefreshToken` path in `run_failover` calls `refresh_token` → `do_authenticate`, which has its
own retry loop. The worst case is about `max_attempts²` authenticate attempts.

#### 🟡 Lease reconnect adds an unrequested response

The keep-alive sent on reconnect produces a response the caller didn't ask for. A caller that pairs
each `keep_alive()` with one `message()` ends up one response behind.

#### ✅ Good

- The write-at-most-once reasoning, and keeping mid-stream resets out of `is_not_sent`.
- The comment pinning the h2 version: a second h2 major would make the downcast silently fail.
- The feature-off build stays identical to upstream (`repr(transparent)` is only dropped when the
  feature is on).
- Solid unit coverage of `record()` and the backoff maths.

---

## `af8e44f` feat(failover): warn logs at each decision point

**What it does:** adds an optional `tracing` dependency and `warn!` logs on target
`etcd_client::failover` for unary retry, token refresh, authenticate failover, and stream
reconnect / give-up.

**Findings:**

- ✅ Looks good, and no credentials are logged (only `%e`).
- 🟡 Stream logs fire only when `reconnect_attempt == 0`. Because of the backoff-reset bug, a
  flapping stream logs once per flap. Fixing that bug fixes this.
- 🟡 Every message is a warn, including the first, routine retry. Consider `info` for single
  retries and `warn` when the retry budget runs out.

## `579d3b8` feat(failover): bridge warns to the `log` facade

**What it does:** turns on tracing's `log` feature, so services that use `env_logger` without a
tracing subscriber still see the warns.

**Findings:** ✅ correct and minimal. tracing only emits `log` records when no tracing subscriber is
installed, so apps that use tracing won't get duplicate lines.

## `cc40cf7` test: allow transparent token renewal under failover

**What it does:** with `failover` on, `test_auth_refresh_token` now expects `get` to succeed after
the token expires.

**Findings:** ✅ fine. The behaviour difference is deliberate and documented in `fd0ec23`.

## `fd0ec23` fix(failover): repair rebase regressions against ClientCaller

**What it does:**

- Reauth now checks the live credentials through `ClientCaller::has_creds()`, instead of the stale
  `ConnectOptions.user`.
- `ClientCallerBuilder` carries the retry config, so every sub-client inherits it.
- `refresh_expired_token` is forced off under `failover`.
- `add_endpoint` / `remove_endpoint` update a shared endpoint count that the backoff reads.
- The test password now matches the CI fixture, and there is a new `reauth_after_update_user` test.

**Findings:**

- ✅ The `has_creds()` fix is right. The new test also covers dropping credentials:
  `update_user(None)` clears the token, and the next call fails instead of refreshing.
- ✅ Building the retry config before `build_client` is cleaner than overwriting it afterwards.
- 🟠 **Forcing `refresh_expired_token = false` goes too far (confirmed).** The override is in
  `From<&ConnectOptions> for CallOptions` (`src/client.rs`). `CallOptions` is shared by every
  `ClientCaller`, including direct sub-client use (`kv_client()`, `lock_client()`, …), which
  doesn't go through `run_failover`. Those callers lose token refresh completely, even when they
  ask for it with `with_auto_token_refresh(true)`. A `kv_client().get()` after the token expires
  fails with `Unauthenticated: invalid auth token`. **Fix:** keep the flag, and skip only the
  nested loop when the call comes from `run_failover`, for example with a `do_call` variant that
  doesn't refresh.
- 🟡 **The endpoint counter can drift.** `add_endpoint` counts up even if that URI is already in
  the pool, and `remove_endpoint` counts down even if it wasn't. It only affects backoff pacing; a
  set of URIs would track the real count.
- 🟡 **`max_attempts` stays fixed after connect.** It's computed from the endpoint count at connect
  time and add/remove doesn't change it. That's documented, and acceptable.

---

## Red tests

These were added alongside this review and are not committed yet. They ran against etcd v3.5.26
with `ETCD_AUTH_TOKEN_TTL=2`.

| Test | Location | Result | Covers |
|---|---|---|---|
| `rejected_create_with_invalid_id_drops_the_pending_watch` | `src/rpc/watch.rs` (unit) | 🔴 | `-1` rejection |
| `rejected_watch_is_not_replayed_after_reconnect` | `tests/failover.rs` | 🔴 | `-1` rejection |
| `duplicate_watch_id_keeps_existing_watch_after_reconnect` | `tests/failover.rs` | 🔴 | `-1` rejection / registry overwrite |
| `watch_reconnect_backs_off_while_stream_flaps` | `tests/failover.rs` | 🔴 | backoff reset |
| `watch_reconnect_refreshes_expired_token` | `tests/failover.rs`, ignored | 🔴 | watch token refresh |
| `sub_client_auto_token_refresh_still_works` | `tests/failover.rs`, ignored | 🔴 | refresh override |
| `lease_reconnect_refreshes_expired_token` | `tests/failover.rs`, ignored | 🟢 | regression guard |

The integration tests route the client through a small in-test TCP proxy (`Proxy`) that can drop
every connection (`sever()`) or keep dropping new ones (`flap()`). This breaks live streams
deterministically, without a multi-node cluster.

The three auth tests are `#[ignore]` because `auth_enable()` turns auth on for the whole shared
etcd server, which would break any test running at the same time. They run through a `with_auth`
helper that turns auth off again even if the test panics. They're also marked `#[serial(auth)]`,
which only stops them running alongside each other. Unmarked tests can still overlap with them.
Marking the other tests `#[parallel(auth)]`, as `tests/client.rs` does, would make
`--include-ignored` safe and would let the auth tests stop being ignored.

To run them:

```sh
cargo test --features failover --lib
cargo test --features failover --test failover
cargo test --features failover --test failover -- --ignored \
  watch_reconnect_refreshes lease_reconnect_refreshes sub_client_auto
```

## CI gap

CI runs plain `cargo test`. `tests/failover.rs` is `#![cfg(feature = "failover")]`, so none of the
failover tests have ever run in CI; `cargo hack` only runs clippy over the feature combinations.
Add a `cargo test --features failover` step.
