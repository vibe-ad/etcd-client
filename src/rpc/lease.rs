//! Etcd Lease RPC.

use crate::caller::{ClientCaller, ClientCallerBuilder};
use crate::error::Result;
use crate::intercept::InterceptedChannel;
use crate::rpc::pb::etcdserverpb::lease_client::LeaseClient as PbLeaseClient;
use crate::rpc::pb::etcdserverpb::{
    LeaseGrantRequest as PbLeaseGrantRequest, LeaseGrantResponse as PbLeaseGrantResponse,
    LeaseKeepAliveRequest as PbLeaseKeepAliveRequest,
    LeaseKeepAliveResponse as PbLeaseKeepAliveResponse, LeaseLeasesRequest as PbLeaseLeasesRequest,
    LeaseLeasesResponse as PbLeaseLeasesResponse, LeaseRevokeRequest as PbLeaseRevokeRequest,
    LeaseRevokeResponse as PbLeaseRevokeResponse, LeaseStatus as PbLeaseStatus,
    LeaseTimeToLiveRequest as PbLeaseTimeToLiveRequest,
    LeaseTimeToLiveResponse as PbLeaseTimeToLiveResponse,
};
use crate::rpc::ResponseHeader;
use crate::vec::VecExt;
use crate::Error;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc::{channel, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{IntoRequest, Request, Streaming};

type Client = PbLeaseClient<InterceptedChannel>;

#[cfg(feature = "failover")]
use crate::failover::RetryConfig;
#[cfg(feature = "failover")]
use std::collections::HashSet;
#[cfg(feature = "failover")]
use tokio::sync::mpsc::Receiver;

/// Client for lease operations.
#[cfg_attr(not(feature = "failover"), repr(transparent))]
#[derive(Clone)]
pub struct LeaseClient {
    inner: ClientCaller<Client>,
    #[cfg(feature = "failover")]
    retry: crate::failover::RetryConfig,
}

impl LeaseClient {
    /// Creates a `LeaseClient`.
    #[inline]
    pub(crate) fn new(builder: ClientCallerBuilder) -> Self {
        Self {
            inner: builder.build(Client::new),
            #[cfg(feature = "failover")]
            retry: crate::failover::RetryConfig::disabled(),
        }
    }

    /// Installs the failover config (called once at client construction).
    #[cfg(feature = "failover")]
    pub(crate) fn set_retry(&mut self, retry: crate::failover::RetryConfig) {
        self.retry = retry;
    }

    /// Creates a lease which expires if the server does not receive a keepAlive
    /// within a given time to live period. All keys attached to the lease will be expired and
    /// deleted if the lease expires. Each expired key generates a delete event in the event history.
    #[inline]
    pub async fn grant(
        &mut self,
        ttl: i64,
        options: Option<LeaseGrantOptions>,
    ) -> Result<LeaseGrantResponse> {
        async fn grant_impl(
            client: &mut Client,
            options: LeaseGrantOptions,
        ) -> Result<LeaseGrantResponse> {
            Ok(LeaseGrantResponse::new(
                client.lease_grant(options).await?.into_inner(),
            ))
        }
        self.inner
            .do_call(options.unwrap_or_default().with_ttl(ttl), grant_impl)
            .await
    }

    /// Revokes a lease. All keys attached to the lease will expire and be deleted.
    #[inline]
    pub async fn revoke(&mut self, id: i64) -> Result<LeaseRevokeResponse> {
        async fn revoke_impl(
            client: &mut Client,
            options: LeaseRevokeOptions,
        ) -> Result<LeaseRevokeResponse> {
            let resp = client.lease_revoke(options).await?.into_inner();
            Ok(LeaseRevokeResponse::new(resp))
        }

        self.inner
            .do_call(LeaseRevokeOptions::new().with_id(id), revoke_impl)
            .await
    }

    /// Keeps the lease alive by streaming keep alive requests from the client
    /// to the server and streaming keep alive responses from the server to the client.
    #[inline]
    pub async fn keep_alive(&mut self, id: i64) -> Result<(LeaseKeeper, LeaseKeepAliveStream)> {
        let req: PbLeaseKeepAliveRequest = LeaseKeepAliveOptions::new().with_id(id).into();

        // Eagerly open, failing over across endpoints: the single-shot open can
        // land on a down node.
        #[cfg(feature = "failover")]
        let (sender, mut stream) = self.open_retrying(vec![req]).await?;
        #[cfg(not(feature = "failover"))]
        let (sender, mut stream) = self.keep_alive_raw(vec![req]).await?;

        // Consume the first response to validate the lease.
        let id = match stream.message().await? {
            Some(resp) => {
                if resp.ttl <= 0 {
                    return Err(Error::LeaseKeepAliveError("lease not found".to_string()));
                }
                resp.id
            }
            None => {
                return Err(Error::WatchError(
                    "failed to create lease keeper".to_string(),
                ));
            }
        };

        #[cfg(feature = "failover")]
        if self.retry.lease_reconnect {
            let (user_tx, driver_rx) = channel::<PbLeaseKeepAliveRequest>(100);
            let (out_tx, out_rx) = channel::<Result<LeaseKeepAliveResponse>>(100);
            let driver = LeaseKeepAliveDriver {
                client: self.clone(),
                retry: self.retry.clone(),
                lease_ids: HashSet::from([id]),
                reconnect_attempt: 0,
                req_rx: driver_rx,
                out_tx,
            };
            tokio::spawn(driver.run(sender, stream));
            return Ok((
                LeaseKeeper::new(id, user_tx),
                LeaseKeepAliveStream::from_driver(out_rx),
            ));
        }

        Ok((
            LeaseKeeper::new(id, sender),
            LeaseKeepAliveStream::new(stream),
        ))
    }

    /// Open a fresh gRPC keep-alive stream with `initial` requests queued before
    /// the stream is established.
    async fn keep_alive_raw(
        &mut self,
        initial: Vec<PbLeaseKeepAliveRequest>,
    ) -> Result<(
        Sender<PbLeaseKeepAliveRequest>,
        Streaming<PbLeaseKeepAliveResponse>,
    )> {
        async fn keep_alive_impl(
            client: &mut Client,
            initial: Vec<PbLeaseKeepAliveRequest>,
        ) -> Result<(
            Sender<PbLeaseKeepAliveRequest>,
            Streaming<PbLeaseKeepAliveResponse>,
        )> {
            let (tx, rx) = channel::<PbLeaseKeepAliveRequest>(100);
            for req in initial {
                tx.send(req)
                    .await
                    .map_err(|e| Error::LeaseKeepAliveError(e.to_string()))?;
            }
            let stream = client
                .lease_keep_alive(ReceiverStream::new(rx))
                .await?
                .into_inner();
            Ok((tx, stream))
        }
        self.inner.do_call(initial, keep_alive_impl).await
    }

    /// Open the initial keep-alive stream, failing over to a healthy endpoint on
    /// a transient error. The balancer can route the single-shot open to a down
    /// node, so retry it like a repeatable unary RPC (quorum-paced backoff,
    /// bounded by the retry budget so a total outage still errors).
    #[cfg(feature = "failover")]
    async fn open_retrying(
        &mut self,
        initial: Vec<PbLeaseKeepAliveRequest>,
    ) -> Result<(
        Sender<PbLeaseKeepAliveRequest>,
        Streaming<PbLeaseKeepAliveResponse>,
    )> {
        use crate::failover::{classify, Decision, RetryPolicy};
        let max = self.retry.max_attempts.max(1);
        let mut last = None;
        for attempt in 0..max {
            let wait = self.retry.backoff(attempt);
            if !wait.is_zero() {
                tokio::time::sleep(wait).await;
            }
            match self.keep_alive_raw(initial.clone()).await {
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

    /// Retrieves lease information.
    #[inline]
    pub async fn time_to_live(
        &mut self,
        id: i64,
        options: Option<LeaseTimeToLiveOptions>,
    ) -> Result<LeaseTimeToLiveResponse> {
        async fn time_to_live_impl(
            client: &mut Client,
            options: LeaseTimeToLiveOptions,
        ) -> Result<LeaseTimeToLiveResponse> {
            let resp = client.lease_time_to_live(options).await?.into_inner();
            Ok(LeaseTimeToLiveResponse::new(resp))
        }

        self.inner
            .do_call(options.unwrap_or_default().with_id(id), time_to_live_impl)
            .await
    }

    /// Lists all existing leases.
    #[inline]
    pub async fn leases(&mut self) -> Result<LeaseLeasesResponse> {
        async fn leases_impl(
            client: &mut Client,
            req: PbLeaseLeasesRequest,
        ) -> Result<LeaseLeasesResponse> {
            let resp = client.lease_leases(req).await?.into_inner();
            Ok(LeaseLeasesResponse::new(resp))
        }

        self.inner
            .do_call(PbLeaseLeasesRequest {}, leases_impl)
            .await
    }
}

/// Options for `Grant` operation.
#[derive(Debug, Default, Clone)]
#[repr(transparent)]
pub struct LeaseGrantOptions(PbLeaseGrantRequest);

impl LeaseGrantOptions {
    /// Set ttl
    #[inline]
    const fn with_ttl(mut self, ttl: i64) -> Self {
        self.0.ttl = ttl;
        self
    }

    /// Set id
    #[inline]
    pub const fn with_id(mut self, id: i64) -> Self {
        self.0.id = id;
        self
    }

    /// Creates a `LeaseGrantOptions`.
    #[inline]
    pub const fn new() -> Self {
        Self(PbLeaseGrantRequest { ttl: 0, id: 0 })
    }
}

impl From<LeaseGrantOptions> for PbLeaseGrantRequest {
    #[inline]
    fn from(options: LeaseGrantOptions) -> Self {
        options.0
    }
}

impl IntoRequest<PbLeaseGrantRequest> for LeaseGrantOptions {
    #[inline]
    fn into_request(self) -> Request<PbLeaseGrantRequest> {
        Request::new(self.into())
    }
}

/// Response for `Grant` operation.
#[cfg_attr(feature = "pub-response-field", visible::StructFields(pub))]
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct LeaseGrantResponse(PbLeaseGrantResponse);

impl LeaseGrantResponse {
    /// Creates a new `LeaseGrantResponse` from pb lease grant response.
    #[inline]
    const fn new(resp: PbLeaseGrantResponse) -> Self {
        Self(resp)
    }

    /// Get response header.
    #[inline]
    pub fn header(&self) -> Option<&ResponseHeader> {
        self.0.header.as_ref().map(From::from)
    }

    /// Takes the header out of the response, leaving a [`None`] in its place.
    #[inline]
    pub fn take_header(&mut self) -> Option<ResponseHeader> {
        self.0.header.take().map(ResponseHeader::new)
    }

    /// TTL is the server chosen lease time-to-live in seconds
    #[inline]
    pub const fn ttl(&self) -> i64 {
        self.0.ttl
    }

    /// ID is the lease ID for the granted lease.
    #[inline]
    pub const fn id(&self) -> i64 {
        self.0.id
    }

    /// Error message if return error.
    #[inline]
    pub fn error(&self) -> &str {
        &self.0.error
    }
}

/// Options for `Revoke` operation.
#[derive(Debug, Default, Clone)]
#[repr(transparent)]
struct LeaseRevokeOptions(PbLeaseRevokeRequest);

impl LeaseRevokeOptions {
    /// Set id
    #[inline]
    fn with_id(mut self, id: i64) -> Self {
        self.0.id = id;
        self
    }

    /// Creates a `LeaseRevokeOptions`.
    #[inline]
    pub const fn new() -> Self {
        Self(PbLeaseRevokeRequest { id: 0 })
    }
}

impl From<LeaseRevokeOptions> for PbLeaseRevokeRequest {
    #[inline]
    fn from(options: LeaseRevokeOptions) -> Self {
        options.0
    }
}

impl IntoRequest<PbLeaseRevokeRequest> for LeaseRevokeOptions {
    #[inline]
    fn into_request(self) -> Request<PbLeaseRevokeRequest> {
        Request::new(self.into())
    }
}

/// Response for `Revoke` operation.
#[cfg_attr(feature = "pub-response-field", visible::StructFields(pub))]
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct LeaseRevokeResponse(PbLeaseRevokeResponse);

impl LeaseRevokeResponse {
    /// Creates a new `LeaseRevokeResponse` from pb lease revoke response.
    #[inline]
    const fn new(resp: PbLeaseRevokeResponse) -> Self {
        Self(resp)
    }

    /// Get response header.
    #[inline]
    pub fn header(&self) -> Option<&ResponseHeader> {
        self.0.header.as_ref().map(From::from)
    }

    /// Takes the header out of the response, leaving a [`None`] in its place.
    #[inline]
    pub fn take_header(&mut self) -> Option<ResponseHeader> {
        self.0.header.take().map(ResponseHeader::new)
    }
}

/// Options for `KeepAlive` operation.
#[derive(Debug, Default, Clone)]
#[repr(transparent)]
struct LeaseKeepAliveOptions(PbLeaseKeepAliveRequest);

impl LeaseKeepAliveOptions {
    /// Set id
    #[inline]
    fn with_id(mut self, id: i64) -> Self {
        self.0.id = id;
        self
    }

    /// Creates a `LeaseKeepAliveOptions`.
    #[inline]
    pub const fn new() -> Self {
        Self(PbLeaseKeepAliveRequest { id: 0 })
    }
}

impl From<LeaseKeepAliveOptions> for PbLeaseKeepAliveRequest {
    #[inline]
    fn from(options: LeaseKeepAliveOptions) -> Self {
        options.0
    }
}

impl IntoRequest<PbLeaseKeepAliveRequest> for LeaseKeepAliveOptions {
    #[inline]
    fn into_request(self) -> Request<PbLeaseKeepAliveRequest> {
        Request::new(self.into())
    }
}

/// Response for `KeepAlive` operation.
#[cfg_attr(feature = "pub-response-field", visible::StructFields(pub))]
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct LeaseKeepAliveResponse(PbLeaseKeepAliveResponse);

impl LeaseKeepAliveResponse {
    /// Creates a new `LeaseKeepAliveResponse` from pb lease KeepAlive response.
    #[inline]
    const fn new(resp: PbLeaseKeepAliveResponse) -> Self {
        Self(resp)
    }

    /// Get response header.
    #[inline]
    pub fn header(&self) -> Option<&ResponseHeader> {
        self.0.header.as_ref().map(From::from)
    }

    /// Takes the header out of the response, leaving a [`None`] in its place.
    #[inline]
    pub fn take_header(&mut self) -> Option<ResponseHeader> {
        self.0.header.take().map(ResponseHeader::new)
    }

    /// TTL is the new time-to-live for the lease.
    #[inline]
    pub const fn ttl(&self) -> i64 {
        self.0.ttl
    }

    /// ID is the lease ID for the keep alive request.
    #[inline]
    pub const fn id(&self) -> i64 {
        self.0.id
    }
}

/// Options for `TimeToLive` operation.
#[derive(Debug, Default, Clone)]
#[repr(transparent)]
pub struct LeaseTimeToLiveOptions(PbLeaseTimeToLiveRequest);

impl LeaseTimeToLiveOptions {
    /// ID is the lease ID for the lease.
    #[inline]
    const fn with_id(mut self, id: i64) -> Self {
        self.0.id = id;
        self
    }

    /// Keys is true to query all the keys attached to this lease.
    #[inline]
    pub const fn with_keys(mut self) -> Self {
        self.0.keys = true;
        self
    }

    /// Creates a `LeaseTimeToLiveOptions`.
    #[inline]
    pub const fn new() -> Self {
        Self(PbLeaseTimeToLiveRequest { id: 0, keys: false })
    }
}

impl From<LeaseTimeToLiveOptions> for PbLeaseTimeToLiveRequest {
    #[inline]
    fn from(options: LeaseTimeToLiveOptions) -> Self {
        options.0
    }
}

impl IntoRequest<PbLeaseTimeToLiveRequest> for LeaseTimeToLiveOptions {
    #[inline]
    fn into_request(self) -> Request<PbLeaseTimeToLiveRequest> {
        Request::new(self.into())
    }
}

/// Response for `TimeToLive` operation.
#[cfg_attr(feature = "pub-response-field", visible::StructFields(pub))]
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct LeaseTimeToLiveResponse(PbLeaseTimeToLiveResponse);

impl LeaseTimeToLiveResponse {
    /// Creates a new `LeaseTimeToLiveResponse` from pb lease TimeToLive response.
    #[inline]
    const fn new(resp: PbLeaseTimeToLiveResponse) -> Self {
        Self(resp)
    }

    /// Get response header.
    #[inline]
    pub fn header(&self) -> Option<&ResponseHeader> {
        self.0.header.as_ref().map(From::from)
    }

    /// Takes the header out of the response, leaving a [`None`] in its place.
    #[inline]
    pub fn take_header(&mut self) -> Option<ResponseHeader> {
        self.0.header.take().map(ResponseHeader::new)
    }

    /// TTL is the remaining TTL in seconds for the lease; the lease will expire in under TTL+1 seconds.
    #[inline]
    pub const fn ttl(&self) -> i64 {
        self.0.ttl
    }

    /// ID is the lease ID from the keep alive request.
    #[inline]
    pub const fn id(&self) -> i64 {
        self.0.id
    }

    /// GrantedTTL is the initial granted time in seconds upon lease creation/renewal.
    #[inline]
    pub const fn granted_ttl(&self) -> i64 {
        self.0.granted_ttl
    }

    /// Keys is the list of keys attached to this lease.
    #[inline]
    pub fn keys(&self) -> &[Vec<u8>] {
        &self.0.keys
    }

    #[inline]
    pub(crate) fn strip_keys_prefix(&mut self, prefix: &[u8]) {
        self.0.keys.iter_mut().for_each(|key| {
            key.strip_key_prefix(prefix);
        });
    }
}

/// Response for `Leases` operation.
#[cfg_attr(feature = "pub-response-field", visible::StructFields(pub))]
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct LeaseLeasesResponse(PbLeaseLeasesResponse);

impl LeaseLeasesResponse {
    /// Creates a new `LeaseLeasesResponse` from pb lease Leases response.
    #[inline]
    const fn new(resp: PbLeaseLeasesResponse) -> Self {
        Self(resp)
    }

    /// Get response header.
    #[inline]
    pub fn header(&self) -> Option<&ResponseHeader> {
        self.0.header.as_ref().map(From::from)
    }

    /// Takes the header out of the response, leaving a [`None`] in its place.
    #[inline]
    pub fn take_header(&mut self) -> Option<ResponseHeader> {
        self.0.header.take().map(ResponseHeader::new)
    }

    /// Get leases status
    #[inline]
    pub fn leases(&self) -> &[LeaseStatus] {
        unsafe { &*(self.0.leases.as_slice() as *const _ as *const [LeaseStatus]) }
    }
}

/// Lease status.
#[cfg_attr(feature = "pub-response-field", visible::StructFields(pub))]
#[derive(Debug, Clone, PartialEq)]
#[repr(transparent)]
pub struct LeaseStatus(PbLeaseStatus);

impl LeaseStatus {
    /// Lease id.
    #[inline]
    pub const fn id(&self) -> i64 {
        self.0.id
    }
}

impl From<&PbLeaseStatus> for &LeaseStatus {
    #[inline]
    fn from(src: &PbLeaseStatus) -> Self {
        unsafe { &*(src as *const _ as *const LeaseStatus) }
    }
}

/// The lease keep alive handle.
#[cfg_attr(feature = "pub-response-field", visible::StructFields(pub))]
#[derive(Debug)]
pub struct LeaseKeeper {
    id: i64,
    sender: Sender<PbLeaseKeepAliveRequest>,
}

impl LeaseKeeper {
    /// Creates a new `LeaseKeeper`.
    #[inline]
    const fn new(id: i64, sender: Sender<PbLeaseKeepAliveRequest>) -> Self {
        Self { id, sender }
    }

    /// The lease id which user want to keep alive.
    #[inline]
    pub const fn id(&self) -> i64 {
        self.id
    }

    /// Sends a keep alive request and receive response
    #[inline]
    pub async fn keep_alive(&mut self) -> Result<()> {
        self.sender
            .send(LeaseKeepAliveOptions::new().with_id(self.id).into())
            .await
            .map_err(|e| Error::LeaseKeepAliveError(e.to_string()))
    }
}

/// The lease keep alive response stream.
#[cfg_attr(feature = "pub-response-field", visible::StructFields(pub))]
#[cfg_attr(feature = "pub-response-field", allow(private_interfaces))]
#[derive(Debug)]
pub struct LeaseKeepAliveStream {
    stream: LeaseStreamInner,
}

/// A raw gRPC stream, or (with the `failover` feature) the reconnect driver's
/// output. Without `failover` this is always `Direct`.
#[cfg_attr(feature = "failover", allow(clippy::large_enum_variant))]
#[derive(Debug)]
enum LeaseStreamInner {
    Direct(Streaming<PbLeaseKeepAliveResponse>),
    #[cfg(feature = "failover")]
    Resilient(Receiver<Result<LeaseKeepAliveResponse>>),
}

impl LeaseKeepAliveStream {
    /// Creates a new `LeaseKeepAliveStream`.
    #[inline]
    const fn new(stream: Streaming<PbLeaseKeepAliveResponse>) -> Self {
        Self {
            stream: LeaseStreamInner::Direct(stream),
        }
    }

    /// Creates a stream backed by the resilient reconnect driver.
    #[cfg(feature = "failover")]
    #[inline]
    fn from_driver(output: Receiver<Result<LeaseKeepAliveResponse>>) -> Self {
        Self {
            stream: LeaseStreamInner::Resilient(output),
        }
    }

    /// Fetches the next message from this stream.
    #[inline]
    pub async fn message(&mut self) -> Result<Option<LeaseKeepAliveResponse>> {
        match &mut self.stream {
            LeaseStreamInner::Direct(stream) => match stream.message().await? {
                Some(resp) => Ok(Some(LeaseKeepAliveResponse::new(resp))),
                None => Ok(None),
            },
            #[cfg(feature = "failover")]
            LeaseStreamInner::Resilient(rx) => match rx.recv().await {
                Some(resp) => resp.map(Some),
                None => Ok(None),
            },
        }
    }
}

impl Stream for LeaseKeepAliveStream {
    type Item = Result<LeaseKeepAliveResponse>;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.get_mut().stream {
            LeaseStreamInner::Direct(stream) => Pin::new(stream).poll_next(cx).map(|t| match t {
                Some(Ok(resp)) => Some(Ok(LeaseKeepAliveResponse::new(resp))),
                Some(Err(e)) => Some(Err(From::from(e))),
                None => None,
            }),
            #[cfg(feature = "failover")]
            LeaseStreamInner::Resilient(rx) => rx.poll_recv(cx),
        }
    }
}

/// Background task that keeps a lease's keep-alive stream alive across
/// connection failures. On a broken stream it re-establishes and re-sends a
/// keep-alive for each active lease. A lease that expired during the outage
/// surfaces as a `ttl <= 0` response, after which it stops being tracked.
#[cfg(feature = "failover")]
struct LeaseKeepAliveDriver {
    client: LeaseClient,
    retry: RetryConfig,
    lease_ids: HashSet<i64>,
    /// Consecutive reconnect attempts without an intervening response, used to
    /// grow the reconnect backoff. Reset to 0 once the stream delivers again.
    reconnect_attempt: u32,
    req_rx: Receiver<PbLeaseKeepAliveRequest>,
    out_tx: Sender<Result<LeaseKeepAliveResponse>>,
}

#[cfg(feature = "failover")]
impl LeaseKeepAliveDriver {
    async fn run(
        mut self,
        mut sender: Sender<PbLeaseKeepAliveRequest>,
        mut stream: Streaming<PbLeaseKeepAliveResponse>,
    ) {
        let mut req_open = true;
        loop {
            tokio::select! {
                // Caller dropped the response stream: nothing left to serve.
                _ = self.out_tx.closed() => return,
                r = self.req_rx.recv(), if req_open => match r {
                    Some(req) => {
                        self.lease_ids.insert(req.id);
                        if sender.send(req).await.is_err() {
                            match self.reconnect().await {
                                Some((s, st)) => { sender = s; stream = st; }
                                None => return,
                            }
                        }
                    }
                    None => req_open = false,
                },
                msg = stream.message() => match msg {
                    Ok(Some(resp)) => {
                        // A delivered response proves the stream is healthy.
                        self.reconnect_attempt = 0;
                        let resp = LeaseKeepAliveResponse::new(resp);
                        // A ttl<=0 response means the lease is gone: stop tracking
                        // it, but still deliver the response so the caller sees it.
                        if resp.ttl() <= 0 {
                            self.lease_ids.remove(&resp.id());
                        }
                        if self.out_tx.send(Ok(resp)).await.is_err() {
                            return;
                        }
                        if self.lease_ids.is_empty() {
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

    /// Re-establish the stream and resume renewals for active leases. Returns
    /// `None` to stop the driver: the caller gave up, or no leases remain.
    async fn reconnect(
        &mut self,
    ) -> Option<(
        Sender<PbLeaseKeepAliveRequest>,
        Streaming<PbLeaseKeepAliveResponse>,
    )> {
        use crate::failover::{classify, Decision, RetryPolicy};
        loop {
            if self.out_tx.is_closed() || self.lease_ids.is_empty() {
                return None;
            }
            if self.reconnect_attempt == 0 {
                tracing::warn!(
                    target: "etcd_client::failover",
                    leases = self.lease_ids.len(),
                    "etcd lease keep-alive stream broke, reconnecting and resuming renewals",
                );
            }
            // Always wait before (re)opening: a stream that establishes then
            // immediately breaks would otherwise hot-loop with no floor. The
            // delay grows until a response arrives (which resets the counter),
            // mirroring etcd's per-cycle retryConnWait.
            let wait = self.retry.reconnect_backoff(self.reconnect_attempt);
            self.reconnect_attempt = self.reconnect_attempt.saturating_add(1);
            tokio::time::sleep(wait).await;
            let initial: Vec<PbLeaseKeepAliveRequest> = self
                .lease_ids
                .iter()
                .map(|&id| PbLeaseKeepAliveRequest { id })
                .collect();
            match self.client.keep_alive_raw(initial).await {
                Ok(pair) => return Some(pair),
                // A permanent error (e.g. an expired auth token the driver
                // cannot refresh) would otherwise retry forever as a silent
                // hang. Surface it and stop so the caller can rebuild through
                // Client.
                Err(e) if !matches!(classify(&e, RetryPolicy::Repeatable), Decision::Retry) => {
                    tracing::warn!(
                        target: "etcd_client::failover",
                        error = %e,
                        "etcd lease keep-alive stream reconnect hit a permanent error, giving up",
                    );
                    let _ = self.out_tx.send(Err(e)).await;
                    return None;
                }
                Err(_) => {}
            }
        }
    }
}
