//! Etcd Watch RPC.

pub use crate::rpc::pb::mvccpb::event::EventType;

use crate::error::{Error, Result};
use crate::intercept::InterceptedChannel;
use crate::rpc::pb::etcdserverpb::watch_client::WatchClient as PbWatchClient;
use crate::rpc::pb::etcdserverpb::watch_request::RequestUnion as WatchRequestUnion;
use crate::rpc::pb::etcdserverpb::{
    WatchCancelRequest, WatchCreateRequest, WatchProgressRequest, WatchRequest,
    WatchResponse as PbWatchResponse,
};
use crate::rpc::pb::mvccpb::Event as PbEvent;
use crate::rpc::{KeyRange, KeyValue, ResponseHeader};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc::{channel, Sender};
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tonic::Streaming;

#[cfg(feature = "failover")]
use crate::failover::RetryConfig;
#[cfg(feature = "failover")]
use std::collections::{HashMap, HashSet};
#[cfg(feature = "failover")]
use tokio::sync::mpsc::Receiver;

/// Client for watch operations.
#[cfg_attr(not(feature = "failover"), repr(transparent))]
#[derive(Clone)]
pub struct WatchClient {
    inner: PbWatchClient<InterceptedChannel>,
    #[cfg(feature = "failover")]
    retry: crate::failover::RetryConfig,
}

impl WatchClient {
    /// Creates a watch client.
    #[inline]
    pub(crate) fn new(channel: InterceptedChannel) -> Self {
        let inner = PbWatchClient::new(channel);
        Self {
            inner,
            #[cfg(feature = "failover")]
            retry: crate::failover::RetryConfig::disabled(),
        }
    }

    /// Installs the failover config (called once at client construction).
    #[cfg(feature = "failover")]
    pub(crate) fn set_retry(&mut self, retry: crate::failover::RetryConfig) {
        self.retry = retry;
    }

    /// Limits the maximum size of a decoded message.
    ///
    /// Default: `4MB`
    pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
        self.inner = self.inner.max_decoding_message_size(limit);
        self
    }

    /// Watches for events happening or that have happened. Both input and output
    /// are streams. The input stream creates and cancels watchers, the output
    /// stream receives responses and events.
    ///
    /// One watch stream can watch on multiple key ranges, streaming events for several watches
    /// are grouped by watch ID. The entire event history can be watched starting from the
    /// last compaction revision.
    ///
    /// With the `failover` feature, the returned stream transparently reconnects
    /// on a healthy endpoint and resumes each watch from the revision after the
    /// last one delivered.
    pub async fn watch(
        &mut self,
        key: impl Into<Vec<u8>>,
        options: Option<WatchOptions>,
    ) -> Result<WatchStream> {
        #[cfg_attr(not(feature = "failover"), allow(unused_mut))]
        let mut create: WatchCreateRequest = options.unwrap_or_default().with_key(key).into();

        #[cfg(feature = "failover")]
        if self.retry.watch_reconnect {
            // Assign a stable client-side watch id (honoring a caller-set id) so
            // the caller's observed id does not change across reconnects.
            let mut next_id = 1;
            let id = assign_watch_id(&mut create, &mut next_id, &HashMap::new());
            let from_now = create.start_revision == 0;
            // Eagerly open so a connect error surfaces from `watch()`, retrying
            // across endpoints: the single-shot open can land on a down node.
            let (sender, stream) = self.open_retrying(vec![create.clone().into()]).await?;
            let (user_tx, driver_rx) = channel::<WatchRequest>(100);
            let (out_tx, out_rx) = channel::<Result<WatchResponse>>(100);
            let driver = WatchDriver {
                client: self.clone(),
                retry: self.retry.clone(),
                watches: HashMap::from([(
                    id,
                    WatchState {
                        create_req: create,
                        from_now,
                    },
                )]),
                seen_created: HashSet::new(),
                next_id,
                reconnect_attempt: 0,
                req_rx: driver_rx,
                out_tx,
            };
            tokio::spawn(driver.run(sender, stream));
            return Ok(WatchStream::from_driver(user_tx, out_rx));
        }

        let (sender, stream) = self.watch_raw(vec![create.into()]).await?;
        Ok(WatchStream::new(sender, stream))
    }

    /// Open a fresh gRPC watch stream with `initial` requests queued before the
    /// stream is established (etcd only emits the first response after a create
    /// request is buffered).
    async fn watch_raw(
        &mut self,
        initial: Vec<WatchRequest>,
    ) -> Result<(Sender<WatchRequest>, Streaming<PbWatchResponse>)> {
        let (tx, rx) = channel::<WatchRequest>(100);
        for req in initial {
            tx.send(req)
                .await
                .map_err(|e| Error::WatchError(e.to_string()))?;
        }
        let stream = self
            .inner
            .watch(ReceiverStream::new(rx))
            .await?
            .into_inner();
        Ok((tx, stream))
    }

    /// Open the initial watch stream, failing over to a healthy endpoint on a
    /// transient error. The balancer can route the single-shot open to a down
    /// node, so retry it like a repeatable unary RPC (quorum-paced backoff,
    /// bounded by the retry budget so a total outage still errors).
    #[cfg(feature = "failover")]
    async fn open_retrying(
        &mut self,
        initial: Vec<WatchRequest>,
    ) -> Result<(Sender<WatchRequest>, Streaming<PbWatchResponse>)> {
        use crate::failover::{classify, Decision, RetryPolicy};
        let max = self.retry.max_attempts.max(1);
        let mut last = None;
        for attempt in 0..max {
            let wait = self.retry.backoff(attempt);
            if !wait.is_zero() {
                tokio::time::sleep(wait).await;
            }
            match self.watch_raw(initial.clone()).await {
                Ok(pair) => return Ok(pair),
                Err(e) => match classify(&e, RetryPolicy::Repeatable) {
                    Decision::Retry => last = Some(e),
                    // The sub-client cannot refresh a token, so an auth error
                    // here is terminal rather than worth burning the budget on.
                    Decision::Stop | Decision::RefreshToken => return Err(e),
                },
            }
        }
        Err(last.expect("retry budget runs at least once"))
    }
}

/// Options for `Watch` operation.
#[derive(Debug, Default, Clone)]
pub struct WatchOptions {
    req: WatchCreateRequest,
    key_range: KeyRange,
}

impl WatchOptions {
    /// Sets key.
    #[inline]
    pub fn with_key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.key_range.with_key(key);
        self
    }

    /// Creates a new `WatchOptions`.
    #[inline]
    pub const fn new() -> Self {
        Self {
            req: WatchCreateRequest {
                key: Vec::new(),
                range_end: Vec::new(),
                start_revision: 0,
                progress_notify: false,
                filters: Vec::new(),
                prev_kv: false,
                watch_id: 0,
                fragment: false,
            },
            key_range: KeyRange::new(),
        }
    }

    /// Sets the end of the range `[key, end)` to watch.
    ///
    /// If `end` is not given, only the key argument is watched.
    ///
    /// If `end` is equal to `\0`, all keys greater than or equal to the key argument are watched.
    #[inline]
    pub fn with_range(mut self, end: impl Into<Vec<u8>>) -> Self {
        self.key_range.with_range(end);
        self
    }

    /// Watches all keys >= key.
    #[inline]
    pub fn with_from_key(mut self) -> Self {
        self.key_range.with_from_key();
        self
    }

    /// Watches all keys prefixed with key.
    #[inline]
    pub fn with_prefix(mut self) -> Self {
        self.key_range.with_prefix();
        self
    }

    /// Watches all keys.
    #[inline]
    pub fn with_all_keys(mut self) -> Self {
        self.key_range.with_all_keys();
        self
    }

    /// Sets the revision to watch from (inclusive). No `start_revision` is "now".
    #[inline]
    pub const fn with_start_revision(mut self, revision: i64) -> Self {
        self.req.start_revision = revision;
        self
    }

    /// `progress_notify` is set so that the etcd server will periodically send a `WatchResponse` with
    /// no events to the new watcher if there are no recent events. It is useful when clients
    /// wish to recover a disconnected watcher starting from a recent known revision.
    /// The etcd server may decide how often it will send notifications based on current load.
    #[inline]
    pub const fn with_progress_notify(mut self) -> Self {
        self.req.progress_notify = true;
        self
    }

    /// Filter the events at server side before it sends back to the watcher.
    #[inline]
    pub fn with_filters(mut self, filters: impl Into<Vec<WatchFilterType>>) -> Self {
        self.req.filters = filters.into().into_iter().map(|f| f as i32).collect();
        self
    }

    /// If `prev_kv` is set, created watcher gets the previous KV before the event happens.
    /// If the previous KV is already compacted, nothing will be returned.
    #[inline]
    pub const fn with_prev_key(mut self) -> Self {
        self.req.prev_kv = true;
        self
    }

    /// If `watch_id` is provided and non-zero, it will be assigned to this watcher.
    /// Since creating a watcher in etcd is not a synchronous operation,
    /// this can be used ensure that ordering is correct when creating multiple
    /// watchers on the same stream. Creating a watcher with an ID already in
    /// use on the stream will cause an error to be returned.
    #[inline]
    pub const fn with_watch_id(mut self, watch_id: i64) -> Self {
        self.req.watch_id = watch_id;
        self
    }

    /// Enables splitting large revisions into multiple watch responses.
    #[inline]
    pub const fn with_fragment(mut self) -> Self {
        self.req.fragment = true;
        self
    }
}

impl From<WatchOptions> for WatchCreateRequest {
    #[inline]
    fn from(mut options: WatchOptions) -> Self {
        let (key, range_end) = options.key_range.build();
        options.req.key = key;
        options.req.range_end = range_end;
        options.req
    }
}

impl From<WatchOptions> for WatchRequest {
    #[inline]
    fn from(options: WatchOptions) -> Self {
        Self {
            request_union: Some(WatchRequestUnion::CreateRequest(options.into())),
        }
    }
}

impl From<WatchCancelRequest> for WatchRequest {
    #[inline]
    fn from(req: WatchCancelRequest) -> Self {
        Self {
            request_union: Some(WatchRequestUnion::CancelRequest(req)),
        }
    }
}

impl From<WatchProgressRequest> for WatchRequest {
    #[inline]
    fn from(req: WatchProgressRequest) -> Self {
        Self {
            request_union: Some(WatchRequestUnion::ProgressRequest(req)),
        }
    }
}

/// Watch filter type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum WatchFilterType {
    /// Filter out put event.
    NoPut = 0,
    /// Filter out delete event.
    NoDelete = 1,
}

/// Response for `Watch` operation.
#[cfg_attr(feature = "pub-response-field", visible::StructFields(pub))]
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct WatchResponse(PbWatchResponse);

impl WatchResponse {
    /// Creates a new `WatchResponse`.
    #[inline]
    const fn new(resp: PbWatchResponse) -> Self {
        Self(resp)
    }

    /// Watch response header.
    #[inline]
    pub fn header(&self) -> Option<&ResponseHeader> {
        self.0.header.as_ref().map(From::from)
    }

    /// Takes the header out of the response, leaving a [`None`] in its place.
    #[inline]
    pub fn take_header(&mut self) -> Option<ResponseHeader> {
        self.0.header.take().map(ResponseHeader::new)
    }

    /// The ID of the watcher that corresponds to the response.
    #[inline]
    pub const fn watch_id(&self) -> i64 {
        self.0.watch_id
    }

    /// created is set to true if the response is for a create watch request.
    /// The client should record the watch_id and expect to receive events for
    /// the created watcher from the same stream.
    /// All events sent to the created watcher will attach with the same watch_id.
    #[inline]
    pub const fn created(&self) -> bool {
        self.0.created
    }

    /// `canceled` is set to true if the response is for a cancel watch request.
    /// No further events will be sent to the canceled watcher.
    #[inline]
    pub const fn canceled(&self) -> bool {
        self.0.canceled
    }

    /// `compact_revision` is set to the minimum index if a watcher tries to watch
    /// at a compacted index.
    ///
    /// This happens when creating a watcher at a compacted revision or the watcher cannot
    /// catch up with the progress of the key-value store.
    ///
    /// The client should treat the watcher as canceled and should not try to create any
    /// watcher with the same start_revision again.
    #[inline]
    pub const fn compact_revision(&self) -> i64 {
        self.0.compact_revision
    }

    /// Indicates the reason for canceling the watcher.
    #[inline]
    pub fn cancel_reason(&self) -> &str {
        &self.0.cancel_reason
    }

    /// Events happened on the watched keys.
    #[inline]
    pub fn events(&self) -> &[Event] {
        unsafe { &*(self.0.events.as_slice() as *const _ as *const [Event]) }
    }
}

/// Watching event.
#[cfg_attr(feature = "pub-response-field", visible::StructFields(pub))]
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct Event(PbEvent);

impl Event {
    /// The kind of event. If type is a `Put`, it indicates
    /// new data has been stored to the key. If type is a `Delete`,
    /// it indicates the key was deleted.
    #[inline]
    pub fn event_type(&self) -> EventType {
        match self.0.r#type {
            0 => EventType::Put,
            1 => EventType::Delete,
            i => panic!("unknown event {i}"),
        }
    }

    /// The KeyValue for the event.
    /// A `Put` event contains current kv pair.
    /// A `Put` event with `kv.version()==1` indicates the creation of a key.
    /// A `Delete` event contains the deleted key with
    /// its modification revision set to the revision of deletion.
    #[inline]
    pub fn kv(&self) -> Option<&KeyValue> {
        self.0.kv.as_ref().map(From::from)
    }

    /// The key-value pair before the event happens.
    #[inline]
    pub fn prev_kv(&self) -> Option<&KeyValue> {
        self.0.prev_kv.as_ref().map(From::from)
    }
}

/// The watching handle.
#[cfg_attr(feature = "pub-response-field", visible::StructFields(pub))]
#[derive(Debug)]
pub struct WatchStream {
    request_sender: WatchRequestSender,
    response_stream: WatchResponseStream,
}

/// The sender for sending watch requests in the existing watch stream.
///
/// The watch request can be sending using the [`WatchStream`] or the [`WatchRequestSender`].
///
/// The [`WatchRequestSender`] can be obtained by splitting the [`WatchStream`] using the
/// [`WatchStream::split`] method.
#[cfg_attr(feature = "pub-response-field", visible::StructFields(pub))]
#[derive(Debug)]
pub struct WatchRequestSender(Sender<WatchRequest>);

/// The response stream for receiving watch responses in the existing watch stream.
///
/// The watch response can be receiving using the [`WatchStream`] or the [`WatchResponseStream`].
///
/// The [`WatchResponseStream`] can be obtained by splitting the [`WatchStream`] using the
/// [`WatchStream::split`] method.
#[cfg_attr(feature = "pub-response-field", visible::StructFields(pub))]
#[cfg_attr(feature = "pub-response-field", allow(private_interfaces))]
#[derive(Debug)]
pub struct WatchResponseStream(WatchResponseInner);

/// The response side of a watch: a raw gRPC stream, or (with the `failover`
/// feature) the output of the reconnect driver. Without `failover` this is
/// always `Direct`, behaving exactly as the raw `tonic::Streaming` it wraps.
#[cfg_attr(feature = "failover", allow(clippy::large_enum_variant))]
#[derive(Debug)]
enum WatchResponseInner {
    Direct(Streaming<PbWatchResponse>),
    #[cfg(feature = "failover")]
    Resilient(Receiver<Result<WatchResponse>>),
}

impl WatchResponseStream {
    /// Receive [`WatchResponse`] from this watch response stream.
    ///
    /// See also [`WatchStream::message`] for receiving watch response from the [`WatchStream`].
    #[inline]
    pub async fn message(&mut self) -> Result<Option<WatchResponse>> {
        match &mut self.0 {
            WatchResponseInner::Direct(stream) => stream
                .message()
                .await
                .map(|resp| resp.map(WatchResponse::new))
                .map_err(From::from),
            #[cfg(feature = "failover")]
            WatchResponseInner::Resilient(rx) => match rx.recv().await {
                Some(resp) => resp.map(Some),
                None => Ok(None),
            },
        }
    }
}

impl WatchStream {
    /// Creates a new `WatchStream`.
    #[inline]
    const fn new(
        request_sender: Sender<WatchRequest>,
        response_stream: Streaming<PbWatchResponse>,
    ) -> Self {
        Self {
            request_sender: WatchRequestSender(request_sender),
            response_stream: WatchResponseStream(WatchResponseInner::Direct(response_stream)),
        }
    }

    /// Creates a `WatchStream` backed by the resilient reconnect driver: the
    /// request sender feeds the driver, and the response stream reads the
    /// driver's forwarded output.
    #[cfg(feature = "failover")]
    fn from_driver(
        request_sender: Sender<WatchRequest>,
        output: Receiver<Result<WatchResponse>>,
    ) -> Self {
        Self {
            request_sender: WatchRequestSender(request_sender),
            response_stream: WatchResponseStream(WatchResponseInner::Resilient(output)),
        }
    }

    /// Send watch request in the existing watch stream.
    #[inline]
    pub async fn watch(
        &mut self,
        key: impl Into<Vec<u8>>,
        options: Option<WatchOptions>,
    ) -> Result<()> {
        self.request_sender.watch(key, options).await
    }

    /// Cancels watch by specified `watch_id`.
    #[inline]
    pub async fn cancel(&mut self, watch_id: i64) -> Result<()> {
        self.request_sender.cancel(watch_id).await
    }

    /// Requests a watch stream progress status be sent in the watch response stream as soon as
    /// possible.
    #[inline]
    pub async fn request_progress(&mut self) -> Result<()> {
        self.request_sender.request_progress().await
    }

    /// Receive [`WatchResponse`] from this watch stream.
    #[inline]
    pub async fn message(&mut self) -> Result<Option<WatchResponse>> {
        self.response_stream.message().await
    }

    /// Splits the watch stream into a request sender and a response receiver (stream).
    pub fn split(self) -> (WatchRequestSender, WatchResponseStream) {
        (self.request_sender, self.response_stream)
    }
}

impl WatchRequestSender {
    /// Send watch request in the existing watch stream.
    #[inline]
    async fn send(&mut self, req: WatchRequest) -> Result<()> {
        self.0
            .send(req)
            .await
            .map_err(|e| Error::WatchError(e.to_string()))
    }

    /// Send watch request in the existing watch stream.
    ///
    /// See also [`WatchStream::watch`] for sending watch request using [`WatchStream`].
    #[inline]
    pub async fn watch(
        &mut self,
        key: impl Into<Vec<u8>>,
        options: Option<WatchOptions>,
    ) -> Result<()> {
        self.send(options.unwrap_or_default().with_key(key).into())
            .await
    }

    /// Cancels watch by specified `watch_id`.
    ///
    ///
    /// See also [`WatchStream::cancel`] for canceling watch using [`WatchStream`].
    #[inline]
    pub async fn cancel(&mut self, watch_id: i64) -> Result<()> {
        let req = WatchCancelRequest { watch_id };
        self.send(req.into()).await
    }

    /// Requests a watch stream progress status be sent in the watch response stream as soon as
    /// possible.
    ///
    /// See also [`WatchStream::request_progress`] for requesting watch stream progress status
    /// using [`WatchStream`].
    #[inline]
    pub async fn request_progress(&mut self) -> Result<()> {
        let req = WatchProgressRequest {};
        self.send(req.into()).await
    }
}

impl Stream for WatchResponseStream {
    type Item = Result<WatchResponse>;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.get_mut().0 {
            WatchResponseInner::Direct(stream) => Pin::new(stream).poll_next(cx).map(|t| match t {
                Some(Ok(resp)) => Some(Ok(WatchResponse::new(resp))),
                Some(Err(e)) => Some(Err(From::from(e))),
                None => None,
            }),
            #[cfg(feature = "failover")]
            WatchResponseInner::Resilient(rx) => rx.poll_recv(cx),
        }
    }
}

impl Stream for WatchStream {
    type Item = Result<WatchResponse>;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().response_stream).poll_next(cx)
    }
}

/// Broadcast progress notifications (a `request_progress` with no id) carry this
/// sentinel id and apply to every watch on the stream. Matches etcd's
/// `InvalidWatchID`.
#[cfg(feature = "failover")]
const INVALID_WATCH_ID: i64 = -1;

/// Assign a stable client-side watch id: honor a caller-provided non-zero id,
/// otherwise draw the next free id from `next_id`.
#[cfg(feature = "failover")]
fn assign_watch_id(
    create: &mut WatchCreateRequest,
    next_id: &mut i64,
    watches: &HashMap<i64, WatchState>,
) -> i64 {
    if create.watch_id == 0 {
        // Skip ids already in use so an auto-assigned id never overwrites a
        // caller-assigned one: the registry is keyed by id, so a clash would
        // silently drop a watch.
        while watches.contains_key(next_id) {
            *next_id += 1;
        }
        create.watch_id = *next_id;
        *next_id += 1;
    } else if *next_id <= create.watch_id {
        // Keep the auto counter ahead of caller-chosen ids to avoid a later clash.
        *next_id = create.watch_id + 1;
    }
    create.watch_id
}

impl From<WatchCreateRequest> for WatchRequest {
    #[inline]
    fn from(create: WatchCreateRequest) -> Self {
        Self {
            request_union: Some(WatchRequestUnion::CreateRequest(create)),
        }
    }
}

/// Per-watch state the resilient driver keeps so it can replay a watch after a
/// reconnect. `create_req.start_revision` holds the resume point.
#[cfg(feature = "failover")]
struct WatchState {
    create_req: WatchCreateRequest,
    /// The watch was requested from "now" (start_revision 0), so it has no
    /// history to replay and its resume point is pinned once created.
    from_now: bool,
}

/// Background task that keeps a watch alive across connection failures: it owns
/// the gRPC stream, forwards responses to the caller, tracks each watch's resume
/// revision, and re-establishes the stream on a healthy endpoint when it breaks.
#[cfg(feature = "failover")]
struct WatchDriver {
    client: WatchClient,
    retry: RetryConfig,
    watches: HashMap<i64, WatchState>,
    /// Watch ids whose `created` ack was already delivered, so the duplicate
    /// echoed after a reconnect replay is suppressed.
    seen_created: HashSet<i64>,
    next_id: i64,
    /// Consecutive reconnect attempts without an intervening response, used to
    /// grow the reconnect backoff. Reset to 0 once the stream delivers again.
    reconnect_attempt: u32,
    req_rx: Receiver<WatchRequest>,
    out_tx: Sender<Result<WatchResponse>>,
}

#[cfg(feature = "failover")]
impl WatchDriver {
    async fn run(
        mut self,
        mut sender: Sender<WatchRequest>,
        mut stream: Streaming<PbWatchResponse>,
    ) {
        let mut req_open = true;
        loop {
            tokio::select! {
                // Caller dropped the response stream: nothing left to serve.
                _ = self.out_tx.closed() => return,
                r = self.req_rx.recv(), if req_open => match r {
                    Some(req) => {
                        let outbound = self.apply_user_request(req);
                        if sender.send(outbound).await.is_err() {
                            match self.reconnect().await {
                                Some((s, st)) => { sender = s; stream = st; }
                                None => return,
                            }
                        }
                    }
                    // Caller dropped the request side: stop accepting requests
                    // but keep delivering responses.
                    None => req_open = false,
                },
                msg = stream.message() => match msg {
                    Ok(Some(resp)) => {
                        // A delivered response proves the stream is healthy.
                        self.reconnect_attempt = 0;
                        if self.forward(WatchResponse::new(resp)).await.is_err() {
                            return;
                        }
                    }
                    Ok(None) | Err(_) => match self.reconnect().await {
                        Some((s, st)) => { sender = s; stream = st; }
                        None => return,
                    },
                },
            }
        }
    }

    /// Record a user request in the registry and return the request to forward,
    /// assigning a stable client-side id to creates.
    fn apply_user_request(&mut self, req: WatchRequest) -> WatchRequest {
        match req.request_union {
            Some(WatchRequestUnion::CreateRequest(mut create)) => {
                let id = assign_watch_id(&mut create, &mut self.next_id, &self.watches);
                let from_now = create.start_revision == 0;
                self.watches.insert(
                    id,
                    WatchState {
                        create_req: create.clone(),
                        from_now,
                    },
                );
                create.into()
            }
            Some(WatchRequestUnion::CancelRequest(cancel)) => {
                // Drop from the registry so a reconnect does not recreate it.
                self.watches.remove(&cancel.watch_id);
                self.seen_created.remove(&cancel.watch_id);
                cancel.into()
            }
            other => WatchRequest {
                request_union: other,
            },
        }
    }

    /// Forward a response, updating the watch's resume revision. Returns `Err`
    /// when the caller has dropped the response stream.
    async fn forward(&mut self, resp: WatchResponse) -> std::result::Result<(), ()> {
        if !Self::record(&mut self.watches, &mut self.seen_created, &resp) {
            return Ok(());
        }
        self.out_tx.send(Ok(resp)).await.map_err(|_| ())
    }

    /// Update the registry for `resp` and report whether it should reach the
    /// caller. Returns `false` to suppress a duplicate `created` ack echoed
    /// after a reconnect replay. Pure over the two maps so it is unit-testable.
    fn record(
        watches: &mut HashMap<i64, WatchState>,
        seen_created: &mut HashSet<i64>,
        resp: &WatchResponse,
    ) -> bool {
        let id = resp.watch_id();
        let header_rev = resp.header().map(|h| h.revision()).unwrap_or(0);

        if resp.created() {
            // A create the server rejects (denied or invalid range) comes back
            // as created and canceled together. Drop it so a reconnect does not
            // replay a doomed create forever, and still forward it so the caller
            // sees the cancel reason.
            if resp.canceled() || resp.compact_revision() != 0 {
                watches.remove(&id);
                seen_created.remove(&id);
            } else if seen_created.insert(id) {
                if let Some(ws) = watches.get_mut(&id) {
                    // etcd binds a from-now watch at header+1, so resuming there
                    // reproduces the server's effective start without replaying
                    // the pre-watch event at `header`.
                    if ws.from_now {
                        ws.create_req.start_revision = header_rev + 1;
                    }
                }
            } else {
                // Duplicate created ack echoed after a reconnect replay.
                return false;
            }
        } else if resp.canceled() || resp.compact_revision() != 0 {
            watches.remove(&id);
            seen_created.remove(&id);
        } else if id == INVALID_WATCH_ID {
            // Broadcast progress notification: it applies to every watch, so
            // advance them all so an idle reconnect resumes near the head
            // instead of replaying history from each watch's last event.
            for ws in watches.values_mut() {
                if header_rev + 1 > ws.create_req.start_revision {
                    ws.create_req.start_revision = header_rev + 1;
                }
            }
        } else if let Some(ws) = watches.get_mut(&id) {
            // Hold the resume point on a non-final fragment: the rest of the
            // revision arrives in later fragments and resuming past it would
            // skip them.
            if !resp.0.fragment {
                // Events carry the highest revision in this batch. A per-watch
                // progress notification (no events) advances to the header.
                let last_event_rev = resp
                    .events()
                    .last()
                    .and_then(|e| e.kv().map(|kv| kv.mod_revision()));
                let new_start = last_event_rev.map_or(header_rev + 1, |r| r + 1);
                if new_start > ws.create_req.start_revision {
                    ws.create_req.start_revision = new_start;
                }
            }
        }
        true
    }

    /// Re-establish the stream and replay active watches from their resume
    /// revision. Returns `None` to stop the driver: the caller gave up, or no
    /// active watches remain to resubscribe.
    async fn reconnect(&mut self) -> Option<(Sender<WatchRequest>, Streaming<PbWatchResponse>)> {
        use crate::failover::{classify, Decision, RetryPolicy};
        loop {
            if self.out_tx.is_closed() || self.watches.is_empty() {
                return None;
            }
            if self.reconnect_attempt == 0 {
                tracing::warn!(
                    target: "etcd_client::failover",
                    watches = self.watches.len(),
                    "etcd watch stream broke, reconnecting and resuming from last revision",
                );
            }
            // Always wait before (re)opening: a stream that establishes then
            // immediately breaks would otherwise hot-loop with no floor. The
            // delay grows until a response arrives (which resets the counter),
            // mirroring etcd's per-cycle retryConnWait.
            let wait = self.retry.reconnect_backoff(self.reconnect_attempt);
            self.reconnect_attempt = self.reconnect_attempt.saturating_add(1);
            tokio::time::sleep(wait).await;
            let initial: Vec<WatchRequest> = self
                .watches
                .values()
                .map(|ws| ws.create_req.clone().into())
                .collect();
            match self.client.watch_raw(initial).await {
                Ok(pair) => return Some(pair),
                // A permanent error (e.g. an expired auth token the driver
                // cannot refresh) would otherwise retry forever as a silent
                // hang. Surface it and stop so the caller can rebuild through
                // Client.
                Err(e) if !matches!(classify(&e, RetryPolicy::Repeatable), Decision::Retry) => {
                    tracing::warn!(
                        target: "etcd_client::failover",
                        error = %e,
                        "etcd watch stream reconnect hit a permanent error, giving up",
                    );
                    let _ = self.out_tx.send(Err(e)).await;
                    return None;
                }
                Err(_) => {}
            }
        }
    }
}

#[cfg(all(test, feature = "failover"))]
mod driver_tests {
    use super::*;
    use crate::rpc::pb::etcdserverpb::ResponseHeader as PbHeader;
    use crate::rpc::pb::mvccpb::KeyValue as PbKeyValue;

    fn ws(from_now: bool, start_revision: i64) -> WatchState {
        WatchState {
            create_req: WatchCreateRequest {
                start_revision,
                ..Default::default()
            },
            from_now,
        }
    }

    fn header(rev: i64) -> Option<PbHeader> {
        Some(PbHeader {
            revision: rev,
            ..Default::default()
        })
    }

    fn event(mod_revision: i64) -> PbEvent {
        PbEvent {
            kv: Some(PbKeyValue {
                mod_revision,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn record(
        watches: &mut HashMap<i64, WatchState>,
        seen: &mut HashSet<i64>,
        pb: PbWatchResponse,
    ) -> bool {
        WatchDriver::record(watches, seen, &WatchResponse(pb))
    }

    #[test]
    fn created_ack_forwarded_once_then_deduped() {
        let mut watches = HashMap::from([(1, ws(false, 7))]);
        let mut seen = HashSet::new();
        assert!(record(
            &mut watches,
            &mut seen,
            PbWatchResponse {
                watch_id: 1,
                created: true,
                header: header(10),
                ..Default::default()
            }
        ));
        assert!(seen.contains(&1));
        // A replayed created ack after a reconnect is suppressed.
        assert!(!record(
            &mut watches,
            &mut seen,
            PbWatchResponse {
                watch_id: 1,
                created: true,
                header: header(10),
                ..Default::default()
            }
        ));
    }

    #[test]
    fn from_now_created_pins_resume_to_header_plus_one() {
        let mut watches = HashMap::from([(1, ws(true, 0))]);
        let mut seen = HashSet::new();
        record(
            &mut watches,
            &mut seen,
            PbWatchResponse {
                watch_id: 1,
                created: true,
                header: header(42),
                ..Default::default()
            },
        );
        assert_eq!(watches[&1].create_req.start_revision, 43);
    }

    #[test]
    fn rejected_create_is_removed_and_forwarded() {
        // A doomed create comes back as created and canceled together.
        let mut watches = HashMap::from([(1, ws(false, 0))]);
        let mut seen = HashSet::new();
        let forwarded = record(
            &mut watches,
            &mut seen,
            PbWatchResponse {
                watch_id: 1,
                created: true,
                canceled: true,
                cancel_reason: "denied".into(),
                header: header(5),
                ..Default::default()
            },
        );
        assert!(forwarded, "caller must see the rejection");
        assert!(
            watches.is_empty(),
            "doomed create must not be replayed on reconnect"
        );
        assert!(!seen.contains(&1));
    }

    #[test]
    fn events_advance_resume_past_last_mod_revision() {
        let mut watches = HashMap::from([(1, ws(false, 0))]);
        let mut seen = HashSet::from([1]);
        record(
            &mut watches,
            &mut seen,
            PbWatchResponse {
                watch_id: 1,
                header: header(20),
                events: vec![event(18), event(20)],
                ..Default::default()
            },
        );
        assert_eq!(watches[&1].create_req.start_revision, 21);
    }

    #[test]
    fn non_final_fragment_holds_resume() {
        let mut watches = HashMap::from([(1, ws(false, 0))]);
        let mut seen = HashSet::from([1]);
        record(
            &mut watches,
            &mut seen,
            PbWatchResponse {
                watch_id: 1,
                header: header(20),
                fragment: true,
                events: vec![event(20)],
                ..Default::default()
            },
        );
        assert_eq!(
            watches[&1].create_req.start_revision, 0,
            "resume must not advance on a non-final fragment"
        );
    }

    #[test]
    fn canceled_removes_watch() {
        let mut watches = HashMap::from([(1, ws(false, 5))]);
        let mut seen = HashSet::from([1]);
        record(
            &mut watches,
            &mut seen,
            PbWatchResponse {
                watch_id: 1,
                canceled: true,
                header: header(9),
                ..Default::default()
            },
        );
        assert!(watches.is_empty());
        assert!(!seen.contains(&1));
    }

    #[test]
    fn broadcast_progress_advances_all_watches() {
        let mut watches = HashMap::from([(1, ws(false, 2)), (2, ws(false, 3))]);
        let mut seen = HashSet::from([1, 2]);
        record(
            &mut watches,
            &mut seen,
            PbWatchResponse {
                watch_id: INVALID_WATCH_ID,
                header: header(50),
                ..Default::default()
            },
        );
        assert_eq!(watches[&1].create_req.start_revision, 51);
        assert_eq!(watches[&2].create_req.start_revision, 51);
    }
}
