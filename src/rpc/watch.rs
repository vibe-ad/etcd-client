//! Etcd Watch RPC.

#[cfg(feature = "failover")]
use crate::caller::RetryPolicy;
use crate::caller::{ClientCaller, ClientCallerBuilder};
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

type Client = PbWatchClient<InterceptedChannel>;

#[cfg(feature = "failover")]
use crate::failover::RetryConfig;
#[cfg(feature = "failover")]
use std::collections::{hash_map::Entry, HashMap, HashSet, VecDeque};
#[cfg(feature = "failover")]
use tokio::sync::mpsc::Receiver;

/// Client for watch operations.
#[cfg_attr(not(feature = "failover"), repr(transparent))]
#[derive(Clone)]
pub struct WatchClient {
    inner: ClientCaller<Client>,
    #[cfg(feature = "failover")]
    retry: crate::failover::RetryConfig,
}

impl WatchClient {
    /// Creates a watch client.
    #[inline]
    pub(crate) fn new(builder: ClientCallerBuilder) -> Self {
        #[cfg(feature = "failover")]
        let retry = builder.retry().clone();
        Self {
            inner: builder.build(Client::new),
            #[cfg(feature = "failover")]
            retry,
        }
    }

    /// Limits the maximum size of a decoded message.
    ///
    /// Default: `4MB`
    pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
        self.inner = self
            .inner
            .with(|client| client.max_decoding_message_size(limit));
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
            // Eagerly open so a connect error surfaces from `watch()`, failing
            // over across endpoints: the single-shot open can land on a down node.
            let (sender, stream) = self
                .inner
                .do_call(
                    RetryPolicy::Repeatable,
                    vec![create.clone().into()],
                    open_stream,
                )
                .await?;
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
                        reauthed: false,
                    },
                )]),
                seen_created: HashSet::new(),
                pending: VecDeque::from([PendingCreate {
                    id,
                    registered: true,
                }]),
                fragments: HashMap::new(),
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

    /// Open a fresh gRPC watch stream with `initial` requests queued, in a
    /// single attempt.
    async fn watch_raw(
        &mut self,
        initial: Vec<WatchRequest>,
    ) -> Result<(Sender<WatchRequest>, Streaming<PbWatchResponse>)> {
        self.inner.do_call_once(initial, open_stream).await
    }
}

/// Open a gRPC watch stream with `initial` requests queued before the stream is
/// established (etcd only emits the first response after a create request is
/// buffered).
async fn open_stream(
    client: &mut Client,
    initial: Vec<WatchRequest>,
) -> Result<(Sender<WatchRequest>, Streaming<PbWatchResponse>)> {
    let (tx, rx) = channel::<WatchRequest>(100);
    for req in initial {
        tx.send(req)
            .await
            .map_err(|e| Error::WatchError(e.to_string()))?;
    }
    let stream = client.watch(ReceiverStream::new(rx)).await?.into_inner();
    Ok((tx, stream))
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
    /// A token rejection already triggered a refresh and resubscribe, so a
    /// second one in a row is final. Cleared once the create is accepted.
    reauthed: bool,
}

/// A create sent on the current stream and not answered yet. etcd answers
/// creates in send order, and a rejected one with `watch_id = -1` instead of
/// the requested id, so responses are matched to creates by order.
#[cfg(feature = "failover")]
#[derive(Clone, Copy, Debug)]
struct PendingCreate {
    id: i64,
    /// The create owns its registry entry. False for a create reusing the id
    /// of a live watch: the server rejects it, and the live watch must stay.
    registered: bool,
}

/// What the driver does with a response once the registry is updated.
#[cfg(feature = "failover")]
#[derive(Debug, PartialEq, Eq)]
enum Delivery {
    Forward,
    /// Not for the caller: a duplicate `created` ack after a replay, or a
    /// fragment held until the rest of its revision arrives.
    Suppress,
    /// A create rejected for an expired token: refresh it and resubscribe.
    Reauth,
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
    /// Creates sent on the current stream and not answered yet, in send order.
    pending: VecDeque<PendingCreate>,
    /// Fragments of a revision not complete yet, merged before delivery so a
    /// replay after a mid-revision reconnect delivers no duplicates.
    fragments: HashMap<i64, WatchResponse>,
    next_id: i64,
    /// Consecutive reconnect attempts without a response delivered to the
    /// caller, used to grow the reconnect backoff. Replayed `created` acks do
    /// not count: a node can accept the stream and then drop it.
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
                    Ok(Some(resp)) => match self.forward(WatchResponse::new(resp)).await {
                        // Only a response the caller receives proves the stream healthy.
                        Ok(Delivery::Forward) => self.reconnect_attempt = 0,
                        Ok(Delivery::Suppress) => {}
                        // The token travels with the stream, so a fresh one needs a
                        // fresh stream: refresh, then resubscribe every watch.
                        Ok(Delivery::Reauth) => {
                            let _ = self.client.inner.refresh_token().await;
                            match self.reconnect().await {
                                Some((s, st)) => { sender = s; stream = st; }
                                None => return,
                            }
                        }
                        Err(()) => return,
                    },
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
                // A caller-chosen id of a live watch is rejected by the server,
                // so it must not replace that watch's registration.
                let registered = match self.watches.entry(id) {
                    Entry::Occupied(_) => false,
                    Entry::Vacant(slot) => {
                        slot.insert(WatchState {
                            create_req: create.clone(),
                            from_now: create.start_revision == 0,
                            reauthed: false,
                        });
                        true
                    }
                };
                self.pending.push_back(PendingCreate { id, registered });
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

    /// Update the registry for `resp` and forward it unless the registry says
    /// otherwise. Returns `Err` when the caller has dropped the response stream.
    async fn forward(&mut self, resp: WatchResponse) -> std::result::Result<Delivery, ()> {
        let Some(resp) = Self::merge_fragments(&mut self.fragments, resp) else {
            return Ok(Delivery::Suppress);
        };
        let delivery = Self::record(
            &mut self.watches,
            &mut self.seen_created,
            &mut self.pending,
            &resp,
        );
        if delivery == Delivery::Forward {
            self.out_tx.send(Ok(resp)).await.map_err(|_| ())?;
        }
        Ok(delivery)
    }

    /// Holds a non-final fragment and returns `None`, or returns the complete
    /// response once the final fragment of its revision arrives. A held
    /// fragment never reaches `record`, so the resume point stays put.
    fn merge_fragments(
        fragments: &mut HashMap<i64, WatchResponse>,
        mut resp: WatchResponse,
    ) -> Option<WatchResponse> {
        let id = resp.watch_id();
        if resp.0.fragment {
            match fragments.entry(id) {
                Entry::Occupied(mut head) => head.get_mut().0.events.append(&mut resp.0.events),
                Entry::Vacant(slot) => {
                    slot.insert(resp);
                }
            }
            return None;
        }
        let Some(mut head) = fragments.remove(&id) else {
            return Some(resp);
        };
        head.0.events.append(&mut resp.0.events);
        head.0.header = resp.0.header;
        head.0.fragment = false;
        Some(head)
    }

    /// Update the registry for `resp` and decide what to do with it. Pure over
    /// the registry so it is unit-testable.
    fn record(
        watches: &mut HashMap<i64, WatchState>,
        seen_created: &mut HashSet<i64>,
        pending: &mut VecDeque<PendingCreate>,
        resp: &WatchResponse,
    ) -> Delivery {
        let header_rev = resp.header().map(|h| h.revision()).unwrap_or(0);

        if resp.created() {
            // A rejection carries `watch_id = -1`, so the create it answers is
            // the oldest one pending, not the one its id names.
            let PendingCreate { id, registered } = pending.pop_front().unwrap_or(PendingCreate {
                id: resp.watch_id(),
                registered: true,
            });
            if resp.canceled() || resp.compact_revision() != 0 {
                if !registered {
                    return Delivery::Forward;
                }
                if crate::failover::is_auth_token_message(resp.cancel_reason()) {
                    if let Some(ws) = watches.get_mut(&id).filter(|ws| !ws.reauthed) {
                        ws.reauthed = true;
                        return Delivery::Reauth;
                    }
                }
                // Drop it so a reconnect does not replay a doomed create
                // forever, and still forward it so the caller sees the reason.
                watches.remove(&id);
                seen_created.remove(&id);
            } else if seen_created.insert(id) {
                if let Some(ws) = watches.get_mut(&id) {
                    ws.reauthed = false;
                    // etcd binds a from-now watch at header+1, so resuming there
                    // reproduces the server's effective start without replaying
                    // the pre-watch event at `header`.
                    if ws.from_now {
                        ws.create_req.start_revision = header_rev + 1;
                    }
                }
            } else {
                if let Some(ws) = watches.get_mut(&id) {
                    ws.reauthed = false;
                }
                // Duplicate created ack echoed after a reconnect replay.
                return Delivery::Suppress;
            }
            return Delivery::Forward;
        }

        let id = resp.watch_id();
        if resp.canceled() || resp.compact_revision() != 0 {
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
            // Fragments are merged before `record`, so this is a whole revision.
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
        Delivery::Forward
    }

    /// Re-establish the stream and replay active watches from their resume
    /// revision. Returns `None` to stop the driver: the caller gave up, or no
    /// active watches remain to resubscribe.
    async fn reconnect(&mut self) -> Option<(Sender<WatchRequest>, Streaming<PbWatchResponse>)> {
        use crate::failover::{classify, Decision};
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
            // The old stream's answers are gone: the replayed creates are the
            // only pending ones, and a half-received revision is replayed whole.
            self.pending.clear();
            self.fragments.clear();
            let initial: Vec<WatchRequest> = self
                .watches
                .iter()
                .map(|(&id, ws)| {
                    self.pending.push_back(PendingCreate {
                        id,
                        registered: true,
                    });
                    ws.create_req.clone().into()
                })
                .collect();
            let e = match self.client.watch_raw(initial).await {
                Ok(pair) => return Some(pair),
                Err(e) => e,
            };
            match classify(&e, RetryPolicy::Repeatable) {
                Decision::Retry => {}
                Decision::RefreshToken if self.client.inner.has_creds() => {
                    let _ = self.client.inner.refresh_token().await;
                }
                // A permanent error would otherwise retry forever as a silent
                // hang. Surface it and stop so the caller can rebuild.
                _ => {
                    tracing::warn!(
                        target: "etcd_client::failover",
                        error = %e,
                        "etcd watch stream reconnect hit a permanent error, giving up",
                    );
                    let _ = self.out_tx.send(Err(e)).await;
                    return None;
                }
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
            reauthed: false,
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

    /// Records `pb` with every watch not acked yet pending, in id order, which
    /// is the order the driver sent their creates in these tests.
    fn record(
        watches: &mut HashMap<i64, WatchState>,
        seen: &mut HashSet<i64>,
        pb: PbWatchResponse,
    ) -> bool {
        let mut ids: Vec<i64> = watches
            .keys()
            .filter(|id| !seen.contains(id))
            .copied()
            .collect();
        ids.sort_unstable();
        let mut pending = ids
            .into_iter()
            .map(|id| PendingCreate {
                id,
                registered: true,
            })
            .collect();
        WatchDriver::record(watches, seen, &mut pending, &WatchResponse(pb)) == Delivery::Forward
    }

    fn rejection(reason: &str) -> WatchResponse {
        WatchResponse(PbWatchResponse {
            watch_id: INVALID_WATCH_ID,
            created: true,
            canceled: true,
            cancel_reason: reason.into(),
            header: header(5),
            ..Default::default()
        })
    }

    #[test]
    fn rejected_create_with_invalid_id_drops_the_pending_watch() {
        // etcd rejects a create (duplicate id, empty range) with
        // `watch_id = InvalidWatchID`, not the requested id. The rejection must
        // still retire the pending create, otherwise it is replayed on every
        // reconnect and the driver never runs out of watches.
        let mut watches = HashMap::from([(1, ws(false, 7)), (2, ws(false, 0))]);
        let mut seen = HashSet::from([1]);
        let forwarded = record(
            &mut watches,
            &mut seen,
            PbWatchResponse {
                watch_id: INVALID_WATCH_ID,
                created: true,
                canceled: true,
                cancel_reason: "mvcc: watcher range is empty".into(),
                header: header(5),
                ..Default::default()
            },
        );
        assert!(forwarded, "caller must see the rejection");
        assert!(
            !watches.contains_key(&2),
            "rejected pending create must not be replayed on reconnect"
        );
        assert!(
            watches.contains_key(&1),
            "an already-acked watch must survive another create's rejection"
        );
    }

    #[test]
    fn rejected_duplicate_id_keeps_the_live_watch() {
        let mut watches = HashMap::from([(1, ws(false, 7))]);
        let mut seen = HashSet::from([1]);
        let mut pending = VecDeque::from([PendingCreate {
            id: 1,
            registered: false,
        }]);
        let delivery = WatchDriver::record(
            &mut watches,
            &mut seen,
            &mut pending,
            &rejection("mvcc: duplicate watch ID provided on the WatchStream"),
        );
        assert_eq!(delivery, Delivery::Forward);
        assert_eq!(watches[&1].create_req.start_revision, 7);
        assert!(seen.contains(&1));
    }

    #[test]
    fn token_rejection_reauths_once_then_drops() {
        let reason = "rpc error: code = Unauthenticated desc = etcdserver: invalid auth token";
        let mut watches = HashMap::from([(1, ws(false, 7))]);
        let mut seen = HashSet::from([1]);
        let pending = || {
            VecDeque::from([PendingCreate {
                id: 1,
                registered: true,
            }])
        };
        let first =
            WatchDriver::record(&mut watches, &mut seen, &mut pending(), &rejection(reason));
        assert_eq!(first, Delivery::Reauth);
        assert!(watches.contains_key(&1), "kept for the resubscribe");
        let second =
            WatchDriver::record(&mut watches, &mut seen, &mut pending(), &rejection(reason));
        assert_eq!(
            second,
            Delivery::Forward,
            "a refreshed token still rejected is final"
        );
        assert!(watches.is_empty());
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
    fn fragments_merge_into_one_delivery() {
        let mut fragments = HashMap::new();
        let fragment = |mod_revision, fragment| {
            WatchResponse(PbWatchResponse {
                watch_id: 1,
                header: header(9),
                events: vec![event(mod_revision)],
                fragment,
                ..Default::default()
            })
        };
        let mut merge = |resp| WatchDriver::merge_fragments(&mut fragments, resp);
        assert!(merge(fragment(9, true)).is_none());
        assert!(merge(fragment(9, true)).is_none());
        let merged = merge(fragment(9, false)).expect("final fragment");
        assert_eq!(merged.events().len(), 3);
        assert!(!merged.0.fragment);
        assert!(fragments.is_empty());
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
