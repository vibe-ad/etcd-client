//! Asynchronous client & synchronous client.

#[cfg(feature = "raw-channel")]
use crate::channel::Channel;
use crate::error::{Error, Result};
use crate::intercept::{InterceptedChannel, Interceptor};
use crate::lock::RwLockExt;
#[cfg(feature = "tls-openssl")]
use crate::openssl_tls::{OpenSslClientConfig, OpenSslConnector};
use crate::rpc::auth::Permission;
use crate::rpc::auth::{AuthClient, AuthDisableResponse, AuthEnableResponse};
use crate::rpc::auth::{
    RoleAddResponse, RoleDeleteResponse, RoleGetResponse, RoleGrantPermissionResponse,
    RoleListResponse, RoleRevokePermissionOptions, RoleRevokePermissionResponse, UserAddOptions,
    UserAddResponse, UserChangePasswordResponse, UserDeleteResponse, UserGetResponse,
    UserGrantRoleResponse, UserListResponse, UserRevokeRoleResponse,
};
use crate::rpc::cluster::{
    ClusterClient, MemberAddOptions, MemberAddResponse, MemberListResponse, MemberPromoteResponse,
    MemberRemoveResponse, MemberUpdateResponse,
};
use crate::rpc::election::{
    CampaignResponse, ElectionClient, LeaderResponse, ObserveStream, ProclaimOptions,
    ProclaimResponse, ResignOptions, ResignResponse,
};
use crate::rpc::kv::{
    CompactionOptions, CompactionResponse, DeleteOptions, DeleteResponse, GetOptions, GetResponse,
    KvClient, PutOptions, PutResponse, Txn, TxnResponse,
};
use crate::rpc::lease::{
    LeaseClient, LeaseGrantOptions, LeaseGrantResponse, LeaseKeepAliveStream, LeaseKeeper,
    LeaseLeasesResponse, LeaseRevokeResponse, LeaseTimeToLiveOptions, LeaseTimeToLiveResponse,
};
use crate::rpc::lock::{LockClient, LockOptions, LockResponse, UnlockResponse};
use crate::rpc::maintenance::{
    AlarmAction, AlarmOptions, AlarmResponse, AlarmType, DefragmentResponse, HashKvResponse,
    HashResponse, MaintenanceClient, MoveLeaderResponse, SnapshotStreaming, StatusResponse,
};
use crate::rpc::watch::{WatchClient, WatchOptions, WatchStream};
#[cfg(feature = "tls-openssl")]
use crate::OpenSslResult;
#[cfg(feature = "tls")]
use crate::TlsOptions;
use http::uri::Uri;
use tonic::metadata::{Ascii, MetadataValue};

use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::mpsc::Sender;

use tonic::transport::{channel::Change, Endpoint};

const HTTP_PREFIX: &str = "http://";
const HTTPS_PREFIX: &str = "https://";

/// Dispatch a unary sub-client call. With the `failover` feature it retries via
/// [`Client::run_failover`], cloning the sub-client and (owned, `Clone`)
/// arguments per attempt so the request is replayable. Without it, a plain call.
macro_rules! failover {
    ($self:ident, $policy:ident, $sub:ident, $m:ident $(, $a:ident)*) => {{
        #[cfg(not(feature = "failover"))]
        {
            $self.$sub.$m($($a),*).await
        }
        #[cfg(feature = "failover")]
        {
            $self
                .run_failover(crate::failover::RetryPolicy::$policy, || {
                    let mut client = $self.$sub.clone();
                    $(let $a = $a.clone();)*
                    async move { client.$m($($a),*).await }
                })
                .await
        }
    }};
}

/// Asynchronous `etcd` client using v3 API.
#[derive(Clone)]
pub struct Client {
    kv: KvClient,
    watch: WatchClient,
    lease: LeaseClient,
    lock: LockClient,
    auth: AuthClient,
    maintenance: MaintenanceClient,
    cluster: ClusterClient,
    election: ElectionClient,
    options: ConnectOptions,
    tx: Option<Sender<Change<Uri, Endpoint>>>,
    auth_token: Arc<RwLock<Option<MetadataValue<Ascii>>>>,
    #[cfg(feature = "failover")]
    retry: crate::failover::RetryConfig,
}

impl Client {
    /// Connect to `etcd` servers from given `endpoints`.
    pub async fn connect<E: AsRef<str>, S: AsRef<[E]>>(
        endpoints: S,
        options: Option<ConnectOptions>,
    ) -> Result<Self> {
        #[cfg(not(feature = "tls-openssl"))]
        let make_balanced_channel = crate::channel::Tonic;
        #[cfg(feature = "tls-openssl")]
        let make_balanced_channel = crate::channel::Openssl {
            conn: options
                .clone()
                .and_then(|o| o.otls)
                .unwrap_or_else(OpenSslConnector::create_default)?,
        };
        Self::connect_with_balanced_channel(endpoints, options, make_balanced_channel).await
    }

    /// Connect to `etcd` servers from given `endpoints` and a balanced channel.
    pub async fn connect_with_balanced_channel<E: AsRef<str>, S: AsRef<[E]>, MBC>(
        endpoints: S,
        options: Option<ConnectOptions>,
        make_balanced_channel: MBC,
    ) -> Result<Self>
    where
        MBC: crate::channel::BalancedChannelBuilder,
        crate::error::Error: From<MBC::Error>,
    {
        let options = options.unwrap_or_default();
        let endpoints = {
            let mut eps = Vec::new();
            for e in endpoints.as_ref() {
                let channel = Self::build_endpoint(e.as_ref(), &options)?;
                eps.push(channel);
            }
            eps
        };

        if endpoints.is_empty() {
            return Err(Error::InvalidArgs(String::from("empty endpoints")));
        }

        #[cfg(feature = "failover")]
        let endpoint_count = endpoints.len();
        let auth_token = Arc::new(RwLock::new(None));

        // Always use balance strategy even if there is only one endpoint.
        let (channel, tx) = make_balanced_channel.balanced_channel(64)?;
        let channel = InterceptedChannel::new(
            channel,
            Interceptor {
                require_leader: options.require_leader,
                auth_token: auth_token.clone(),
            },
        );
        for endpoint in endpoints {
            // The rx inside `channel` may be closed or error, e.g. the balanced service is
            // openssl based and the openssl connector is misconfigured, the send here may fail.
            tx.send(Change::Insert(endpoint.uri().clone(), endpoint))
                .await
                .map_err(|_| {
                    Error::Internal("failed to insert endpoint into the balanced channel".into())
                })?;
        }

        let client = Self::build_client(channel, Some(tx), auth_token, options);
        #[cfg(feature = "failover")]
        let client = client.with_retry_config(endpoint_count);
        client.refresh_token().await?;
        Ok(client)
    }

    #[cfg(feature = "raw-channel")]
    /// Connect to `etcd` servers represented by the given `channel`.
    pub async fn from_channel(channel: Channel, options: Option<ConnectOptions>) -> Result<Self> {
        let options = options.unwrap_or_default();
        let auth_token = Arc::new(RwLock::new(None));
        let channel = InterceptedChannel::new(
            channel,
            Interceptor {
                require_leader: options.require_leader,
                auth_token: auth_token.clone(),
            },
        );

        let client = Self::build_client(channel, None, auth_token, options);
        // A raw channel has no managed endpoint list, so pace retries as if
        // single-endpoint. Failover still works if the channel is balanced.
        #[cfg(feature = "failover")]
        let client = client.with_retry_config(1);
        client.refresh_token().await?;
        Ok(client)
    }

    fn build_endpoint(url: &str, options: &ConnectOptions) -> Result<Endpoint> {
        use tonic::transport::Channel as TonicChannel;
        let mut endpoint = if url.starts_with(HTTP_PREFIX) {
            #[cfg(feature = "tls")]
            if options.tls.is_some() {
                return Err(Error::InvalidArgs(String::from(
                    "TLS options are only supported with HTTPS URLs",
                )));
            }

            TonicChannel::builder(url.parse()?)
        } else if url.starts_with(HTTPS_PREFIX) {
            #[cfg(not(any(feature = "tls", feature = "tls-openssl")))]
            return Err(Error::InvalidArgs(String::from(
                "HTTPS URLs are only supported with the feature \"tls\"",
            )));

            #[cfg(all(feature = "tls-openssl", not(feature = "tls")))]
            {
                TonicChannel::builder(url.parse()?)
            }

            #[cfg(feature = "tls")]
            {
                let tls = options.tls.clone().unwrap_or_default();
                TonicChannel::builder(url.parse()?).tls_config(tls)?
            }
        } else {
            #[cfg(feature = "tls")]
            {
                let tls = options.tls.clone();

                match tls {
                    Some(tls) => {
                        let e = HTTPS_PREFIX.to_owned() + url;
                        TonicChannel::builder(e.parse()?).tls_config(tls)?
                    }
                    None => {
                        let e = HTTP_PREFIX.to_owned() + url;
                        TonicChannel::builder(e.parse()?)
                    }
                }
            }

            #[cfg(all(feature = "tls-openssl", not(feature = "tls")))]
            {
                let pfx = if options.otls.as_ref().is_some() {
                    HTTPS_PREFIX
                } else {
                    HTTP_PREFIX
                };
                let e = pfx.to_owned() + url;
                TonicChannel::builder(e.parse()?)
            }

            #[cfg(all(not(feature = "tls"), not(feature = "tls-openssl")))]
            {
                let e = HTTP_PREFIX.to_owned() + url;
                TonicChannel::builder(e.parse()?)
            }
        };

        if let Some((interval, timeout)) = options.keep_alive {
            endpoint = endpoint
                .keep_alive_while_idle(options.keep_alive_while_idle)
                .http2_keep_alive_interval(interval)
                .keep_alive_timeout(timeout);
        }

        if let Some(timeout) = options.timeout {
            endpoint = endpoint.timeout(timeout);
        }

        if let Some(timeout) = options.connect_timeout {
            endpoint = endpoint.connect_timeout(timeout);
        }

        if let Some(tcp_keepalive) = options.tcp_keepalive {
            endpoint = endpoint.tcp_keepalive(Some(tcp_keepalive));
        }

        Ok(endpoint)
    }

    fn build_client(
        channel: InterceptedChannel,
        tx: Option<Sender<Change<Uri, Endpoint>>>,
        auth_token: Arc<RwLock<Option<MetadataValue<Ascii>>>>,
        options: ConnectOptions,
    ) -> Self {
        let kv = KvClient::new(channel.clone());
        let watch = WatchClient::new(channel.clone());
        let lease = LeaseClient::new(channel.clone());
        let lock = LockClient::new(channel.clone());
        let auth = AuthClient::new(channel.clone());
        let cluster = ClusterClient::new(channel.clone());
        let maintenance = MaintenanceClient::new(channel.clone());
        let election = ElectionClient::new(channel);

        Self {
            kv,
            watch,
            lease,
            lock,
            auth,
            maintenance,
            cluster,
            election,
            options,
            tx,
            auth_token,
            // Placeholder, overwritten by `with_retry_config` once the endpoint
            // count is known.
            #[cfg(feature = "failover")]
            retry: crate::failover::RetryConfig::disabled(),
        }
    }

    /// Builds the retry config from options and endpoint count, then stores it.
    #[cfg(feature = "failover")]
    fn with_retry_config(mut self, endpoint_count: usize) -> Self {
        let max_attempts = self
            .options
            .max_retries
            .unwrap_or_else(|| ((2 * endpoint_count).max(5)) as u32);
        let (wait, jitter) = self
            .options
            .retry_backoff
            .unwrap_or((Duration::from_millis(25), 0.10));
        // Stream auto-reconnect defaults on whenever retry is on.
        let retry_on = max_attempts > 1;
        let watch_reconnect = self.options.watch_reconnect.unwrap_or(retry_on);
        let lease_reconnect = self.options.lease_keepalive_reconnect.unwrap_or(retry_on);
        let retry = crate::failover::RetryConfig::new(
            max_attempts,
            wait,
            jitter,
            endpoint_count,
            watch_reconnect,
            lease_reconnect,
        );
        // Streaming reconnection lives in the watch/lease sub-clients, so they
        // need the config too.
        self.watch.set_retry(retry.clone());
        self.lease.set_retry(retry.clone());
        self.retry = retry;
        self
    }

    /// Run a unary operation with retry/failover according to `policy`,
    /// re-authenticating on token expiry. A no-op wrapper when retry is
    /// disabled (single attempt).
    #[cfg(feature = "failover")]
    async fn run_failover<T, F, Fut>(
        &self,
        policy: crate::failover::RetryPolicy,
        mut op: F,
    ) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        use crate::failover::Decision;
        let cfg = &self.retry;
        let max = cfg.max_attempts.max(1);
        let mut last: Option<Error> = None;
        for attempt in 0..max {
            let wait = cfg.backoff(attempt);
            if !wait.is_zero() {
                tokio::time::sleep(wait).await;
            }
            match op().await {
                Ok(v) => return Ok(v),
                Err(e) => match crate::failover::classify(&e, policy) {
                    Decision::RefreshToken => {
                        // Without credentials there is nothing to refresh, so
                        // retrying the same auth error would just burn the budget.
                        if self.options.user.is_none() {
                            return Err(e);
                        }
                        // Reauth is best-effort: a refresh that itself fails (for
                        // example it hit the down endpoint) must not mask the
                        // original error, so keep it and let the budget continue.
                        let _ = self.refresh_token().await;
                        last = Some(e);
                    }
                    Decision::Retry => last = Some(e),
                    Decision::Stop => return Err(e),
                },
            }
        }
        Err(last.expect("retry loop runs at least once"))
    }

    /// Dynamically add an endpoint to the client.
    ///
    /// Which can be used to add a new member to the underlying balance cache.
    /// The typical scenario is that application can use a services discovery
    /// to discover the member list changes and add/remove them to/from the client.
    ///
    /// Note that the [`Client`] doesn't check the authentication before added.
    /// So the etcd member of the added endpoint REQUIRES to use the same auth
    /// token as when create the client. Otherwise, the underlying balance
    /// services will not be able to connect to the new endpoint.
    #[inline]
    pub async fn add_endpoint<E: AsRef<str>>(&self, endpoint: E) -> Result<()> {
        let endpoint = Self::build_endpoint(endpoint.as_ref(), &self.options)?;
        let Some(tx) = &self.tx else {
            return Err(Error::EndpointsNotManaged);
        };
        tx.send(Change::Insert(endpoint.uri().clone(), endpoint))
            .await
            .map_err(|e| Error::EndpointError(format!("failed to add endpoint because of {e}")))
    }

    /// Dynamically remove an endpoint from the client.
    ///
    /// Note that the `endpoint` str should be the same as it was added.
    /// And the underlying balance services cache used the hash from the Uri,
    /// which was parsed from `endpoint` str, to do the equality comparisons.
    #[inline]
    pub async fn remove_endpoint<E: AsRef<str>>(&self, endpoint: E) -> Result<()> {
        let uri = http::Uri::from_str(endpoint.as_ref())?;
        let Some(tx) = &self.tx else {
            return Err(Error::EndpointsNotManaged);
        };
        tx.send(Change::Remove(uri))
            .await
            .map_err(|e| Error::EndpointError(format!("failed to remove endpoint because of {e}")))
    }

    /// Gets a KV client.
    #[inline]
    pub fn kv_client(&self) -> KvClient {
        self.kv.clone()
    }

    /// Gets a watch client.
    #[inline]
    pub fn watch_client(&self) -> WatchClient {
        self.watch.clone()
    }

    /// Gets a lease client.
    #[inline]
    pub fn lease_client(&self) -> LeaseClient {
        self.lease.clone()
    }

    /// Gets an auth client.
    #[inline]
    pub fn auth_client(&self) -> AuthClient {
        self.auth.clone()
    }

    /// Gets a maintenance client.
    #[inline]
    pub fn maintenance_client(&self) -> MaintenanceClient {
        self.maintenance.clone()
    }

    /// Gets a cluster client.
    #[inline]
    pub fn cluster_client(&self) -> ClusterClient {
        self.cluster.clone()
    }

    /// Gets a lock client.
    #[inline]
    pub fn lock_client(&self) -> LockClient {
        self.lock.clone()
    }

    /// Gets a election client.
    #[inline]
    pub fn election_client(&self) -> ElectionClient {
        self.election.clone()
    }

    /// Put the given key into the key-value store.
    /// A put request increments the revision of the key-value store
    /// and generates one event in the event history.
    #[inline]
    pub async fn put(
        &mut self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        options: Option<PutOptions>,
    ) -> Result<PutResponse> {
        let (key, value) = (key.into(), value.into());
        failover!(self, NonRepeatable, kv, put, key, value, options)
    }

    /// Gets the key from the key-value store.
    #[inline]
    pub async fn get(
        &mut self,
        key: impl Into<Vec<u8>>,
        options: Option<GetOptions>,
    ) -> Result<GetResponse> {
        let key = key.into();
        failover!(self, Repeatable, kv, get, key, options)
    }

    /// Deletes the given key from the key-value store.
    #[inline]
    pub async fn delete(
        &mut self,
        key: impl Into<Vec<u8>>,
        options: Option<DeleteOptions>,
    ) -> Result<DeleteResponse> {
        let key = key.into();
        failover!(self, NonRepeatable, kv, delete, key, options)
    }

    /// Compacts the event history in the etcd key-value store. The key-value
    /// store should be periodically compacted or the event history will continue to grow
    /// indefinitely.
    #[inline]
    pub async fn compact(
        &mut self,
        revision: i64,
        options: Option<CompactionOptions>,
    ) -> Result<CompactionResponse> {
        failover!(self, NonRepeatable, kv, compact, revision, options)
    }

    /// Processes multiple operations in a single transaction.
    /// A txn request increments the revision of the key-value store
    /// and generates events with the same revision for every completed operation.
    /// It is not allowed to modify the same key several times within one txn.
    #[inline]
    pub async fn txn(&mut self, txn: Txn) -> Result<TxnResponse> {
        failover!(self, NonRepeatable, kv, txn, txn)
    }

    /// Watches for events happening or that have happened. Both input and output
    /// are streams; the input stream is for creating and canceling watcher and the output
    /// stream sends events. The entire event history can be watched starting from the
    /// last compaction revision.
    #[inline]
    pub async fn watch(
        &mut self,
        key: impl Into<Vec<u8>>,
        options: Option<WatchOptions>,
    ) -> Result<WatchStream> {
        self.watch.watch(key, options).await
    }

    /// Creates a lease which expires if the server does not receive a keepAlive
    /// within a given time to live period. All keys attached to the lease will be expired and
    /// deleted if the lease expires. Each expired key generates a delete event in the event history.
    #[inline]
    pub async fn lease_grant(
        &mut self,
        ttl: i64,
        options: Option<LeaseGrantOptions>,
    ) -> Result<LeaseGrantResponse> {
        failover!(self, Repeatable, lease, grant, ttl, options)
    }

    /// Revokes a lease. All keys attached to the lease will expire and be deleted.
    #[inline]
    pub async fn lease_revoke(&mut self, id: i64) -> Result<LeaseRevokeResponse> {
        failover!(self, Repeatable, lease, revoke, id)
    }

    /// Keeps the lease alive by streaming keep alive requests from the client
    /// to the server and streaming keep alive responses from the server to the client.
    #[inline]
    pub async fn lease_keep_alive(
        &mut self,
        id: i64,
    ) -> Result<(LeaseKeeper, LeaseKeepAliveStream)> {
        self.lease.keep_alive(id).await
    }

    /// Retrieves lease information.
    #[inline]
    pub async fn lease_time_to_live(
        &mut self,
        id: i64,
        options: Option<LeaseTimeToLiveOptions>,
    ) -> Result<LeaseTimeToLiveResponse> {
        failover!(self, Repeatable, lease, time_to_live, id, options)
    }

    /// Lists all existing leases.
    #[inline]
    pub async fn leases(&mut self) -> Result<LeaseLeasesResponse> {
        failover!(self, Repeatable, lease, leases)
    }

    /// Lock acquires a distributed shared lock on a given named lock.
    /// On success, it will return a unique key that exists so long as the
    /// lock is held by the caller. This key can be used in conjunction with
    /// transactions to safely ensure updates to etcd only occur while holding
    /// lock ownership. The lock is held until Unlock is called on the key or the
    /// lease associate with the owner expires.
    #[inline]
    pub async fn lock(
        &mut self,
        name: impl Into<Vec<u8>>,
        options: Option<LockOptions>,
    ) -> Result<LockResponse> {
        let name = name.into();
        failover!(self, NonRepeatable, lock, lock, name, options)
    }

    /// Unlock takes a key returned by Lock and releases the hold on lock. The
    /// next Lock caller waiting for the lock will then be woken up and given
    /// ownership of the lock.
    #[inline]
    pub async fn unlock(&mut self, key: impl Into<Vec<u8>>) -> Result<UnlockResponse> {
        let key = key.into();
        failover!(self, Repeatable, lock, unlock, key)
    }

    /// Enables authentication.
    #[inline]
    pub async fn auth_enable(&mut self) -> Result<AuthEnableResponse> {
        failover!(self, NonRepeatable, auth, auth_enable)
    }

    /// Disables authentication.
    #[inline]
    pub async fn auth_disable(&mut self) -> Result<AuthDisableResponse> {
        failover!(self, NonRepeatable, auth, auth_disable)
    }

    /// Adds role.
    #[inline]
    pub async fn role_add(&mut self, name: impl Into<String>) -> Result<RoleAddResponse> {
        let name = name.into();
        failover!(self, NonRepeatable, auth, role_add, name)
    }

    /// Deletes role.
    #[inline]
    pub async fn role_delete(&mut self, name: impl Into<String>) -> Result<RoleDeleteResponse> {
        let name = name.into();
        failover!(self, NonRepeatable, auth, role_delete, name)
    }

    /// Gets role.
    #[inline]
    pub async fn role_get(&mut self, name: impl Into<String>) -> Result<RoleGetResponse> {
        let name = name.into();
        failover!(self, Repeatable, auth, role_get, name)
    }

    /// Lists role.
    #[inline]
    pub async fn role_list(&mut self) -> Result<RoleListResponse> {
        failover!(self, Repeatable, auth, role_list)
    }

    /// Grants role permission.
    #[inline]
    pub async fn role_grant_permission(
        &mut self,
        name: impl Into<String>,
        perm: Permission,
    ) -> Result<RoleGrantPermissionResponse> {
        let name = name.into();
        failover!(self, NonRepeatable, auth, role_grant_permission, name, perm)
    }

    /// Revokes role permission.
    #[inline]
    pub async fn role_revoke_permission(
        &mut self,
        name: impl Into<String>,
        key: impl Into<Vec<u8>>,
        options: Option<RoleRevokePermissionOptions>,
    ) -> Result<RoleRevokePermissionResponse> {
        let (name, key) = (name.into(), key.into());
        failover!(
            self,
            NonRepeatable,
            auth,
            role_revoke_permission,
            name,
            key,
            options
        )
    }

    /// Add an user.
    #[inline]
    pub async fn user_add(
        &mut self,
        name: impl Into<String>,
        password: impl Into<String>,
        options: Option<UserAddOptions>,
    ) -> Result<UserAddResponse> {
        let (name, password) = (name.into(), password.into());
        failover!(self, NonRepeatable, auth, user_add, name, password, options)
    }

    /// Gets the user info by the user name.
    #[inline]
    pub async fn user_get(&mut self, name: impl Into<String>) -> Result<UserGetResponse> {
        let name = name.into();
        failover!(self, Repeatable, auth, user_get, name)
    }

    /// Lists all users.
    #[inline]
    pub async fn user_list(&mut self) -> Result<UserListResponse> {
        failover!(self, Repeatable, auth, user_list)
    }

    /// Deletes the given key from the key-value store.
    #[inline]
    pub async fn user_delete(&mut self, name: impl Into<String>) -> Result<UserDeleteResponse> {
        let name = name.into();
        failover!(self, NonRepeatable, auth, user_delete, name)
    }

    /// Change password for an user.
    #[inline]
    pub async fn user_change_password(
        &mut self,
        name: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<UserChangePasswordResponse> {
        let (name, password) = (name.into(), password.into());
        failover!(
            self,
            NonRepeatable,
            auth,
            user_change_password,
            name,
            password
        )
    }

    /// Grant role for an user.
    #[inline]
    pub async fn user_grant_role(
        &mut self,
        user: impl Into<String>,
        role: impl Into<String>,
    ) -> Result<UserGrantRoleResponse> {
        let (user, role) = (user.into(), role.into());
        failover!(self, NonRepeatable, auth, user_grant_role, user, role)
    }

    /// Revoke role for an user.
    #[inline]
    pub async fn user_revoke_role(
        &mut self,
        user: impl Into<String>,
        role: impl Into<String>,
    ) -> Result<UserRevokeRoleResponse> {
        let (user, role) = (user.into(), role.into());
        failover!(self, NonRepeatable, auth, user_revoke_role, user, role)
    }

    /// Maintain(get, active or inactive) alarms of members.
    #[inline]
    pub async fn alarm(
        &mut self,
        alarm_action: AlarmAction,
        alarm_type: AlarmType,
        options: Option<AlarmOptions>,
    ) -> Result<AlarmResponse> {
        failover!(
            self,
            Repeatable,
            maintenance,
            alarm,
            alarm_action,
            alarm_type,
            options
        )
    }

    /// Gets the status of a member.
    #[inline]
    pub async fn status(&mut self) -> Result<StatusResponse> {
        failover!(self, Repeatable, maintenance, status)
    }

    /// Defragments a member's backend database to recover storage space.
    #[inline]
    pub async fn defragment(&mut self) -> Result<DefragmentResponse> {
        failover!(self, NonRepeatable, maintenance, defragment)
    }

    /// Computes the hash of whole backend keyspace.
    /// including key, lease, and other buckets in storage.
    /// This is designed for testing ONLY!
    #[inline]
    pub async fn hash(&mut self) -> Result<HashResponse> {
        failover!(self, Repeatable, maintenance, hash)
    }

    /// Computes the hash of all MVCC keys up to a given revision.
    /// It only iterates \"key\" bucket in backend storage.
    #[inline]
    pub async fn hash_kv(&mut self, revision: i64) -> Result<HashKvResponse> {
        failover!(self, Repeatable, maintenance, hash_kv, revision)
    }

    /// Gets a snapshot of the entire backend from a member over a stream to a client.
    /// Only the stream establishment is retried under `failover`.
    #[inline]
    pub async fn snapshot(&mut self) -> Result<SnapshotStreaming> {
        failover!(self, Repeatable, maintenance, snapshot)
    }

    /// Adds current connected server as a member.
    #[inline]
    pub async fn member_add<E: AsRef<str>, S: AsRef<[E]>>(
        &mut self,
        urls: S,
        options: Option<MemberAddOptions>,
    ) -> Result<MemberAddResponse> {
        let mut eps = Vec::new();
        for e in urls.as_ref() {
            let e = e.as_ref();
            let url = if e.starts_with(HTTP_PREFIX) || e.starts_with(HTTPS_PREFIX) {
                e.to_string()
            } else {
                HTTP_PREFIX.to_owned() + e
            };
            eps.push(url);
        }
        failover!(self, NonRepeatable, cluster, member_add, eps, options)
    }

    /// Remove a member.
    #[inline]
    pub async fn member_remove(&mut self, id: u64) -> Result<MemberRemoveResponse> {
        failover!(self, NonRepeatable, cluster, member_remove, id)
    }

    /// Updates the member.
    #[inline]
    pub async fn member_update(
        &mut self,
        id: u64,
        url: impl Into<Vec<String>>,
    ) -> Result<MemberUpdateResponse> {
        let url = url.into();
        failover!(self, NonRepeatable, cluster, member_update, id, url)
    }

    /// Promotes the member.
    #[inline]
    pub async fn member_promote(&mut self, id: u64) -> Result<MemberPromoteResponse> {
        failover!(self, NonRepeatable, cluster, member_promote, id)
    }

    /// Lists members.
    #[inline]
    pub async fn member_list(&mut self) -> Result<MemberListResponse> {
        failover!(self, Repeatable, cluster, member_list)
    }

    /// Moves the current leader node to target node.
    #[inline]
    pub async fn move_leader(&mut self, target_id: u64) -> Result<MoveLeaderResponse> {
        failover!(self, Repeatable, maintenance, move_leader, target_id)
    }

    /// Puts a value as eligible for the election on the prefix key.
    /// Multiple sessions can participate in the election for the
    /// same prefix, but only one can be the leader at a time.
    #[inline]
    pub async fn campaign(
        &mut self,
        name: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        lease: i64,
    ) -> Result<CampaignResponse> {
        let (name, value) = (name.into(), value.into());
        failover!(self, NonRepeatable, election, campaign, name, value, lease)
    }

    /// Lets the leader announce a new value without another election.
    #[inline]
    pub async fn proclaim(
        &mut self,
        value: impl Into<Vec<u8>>,
        options: Option<ProclaimOptions>,
    ) -> Result<ProclaimResponse> {
        let value = value.into();
        failover!(self, NonRepeatable, election, proclaim, value, options)
    }

    /// Returns the leader value for the current election.
    #[inline]
    pub async fn leader(&mut self, name: impl Into<Vec<u8>>) -> Result<LeaderResponse> {
        let name = name.into();
        failover!(self, Repeatable, election, leader, name)
    }

    /// Returns a channel that reliably observes ordered leader proposals
    /// as GetResponse values on every current elected leader key.
    #[inline]
    pub async fn observe(&mut self, name: impl Into<Vec<u8>>) -> Result<ObserveStream> {
        let name = name.into();
        failover!(self, Repeatable, election, observe, name)
    }

    /// Releases election leadership and then start a new election
    #[inline]
    pub async fn resign(&mut self, option: Option<ResignOptions>) -> Result<ResignResponse> {
        failover!(self, NonRepeatable, election, resign, option)
    }

    async fn do_authenticate(
        &self,
        user: String,
        password: String,
    ) -> Result<MetadataValue<Ascii>> {
        #[cfg(not(feature = "failover"))]
        let resp = self.auth_client().authenticate(user, password).await?;

        // Authenticate is idempotent (it only mints a token), so fail it over to
        // a healthy endpoint on a transient error. This keeps an authenticated
        // connect and in-flight reauth working when the balancer routes the
        // authenticate RPC to a down node, the scenario failover targets.
        #[cfg(feature = "failover")]
        let resp = {
            use crate::failover::{classify, Decision, RetryPolicy};
            let max = self.retry.max_attempts.max(1);
            let mut last = None;
            let mut ok = None;
            for attempt in 0..max {
                let wait = self.retry.backoff(attempt);
                if !wait.is_zero() {
                    tokio::time::sleep(wait).await;
                }
                match self
                    .auth_client()
                    .authenticate(user.clone(), password.clone())
                    .await
                {
                    Ok(resp) => {
                        ok = Some(resp);
                        break;
                    }
                    Err(e) => match classify(&e, RetryPolicy::Repeatable) {
                        Decision::Retry => last = Some(e),
                        _ => return Err(e),
                    },
                }
            }
            match ok {
                Some(resp) => resp,
                None => return Err(last.expect("retry budget runs at least once")),
            }
        };

        let token = resp.token().parse()?;
        Ok(token)
    }

    /// Refresh the authentication token if the client has credentials options.
    pub async fn refresh_token(&self) -> Result<()> {
        if let Some((user, password)) = self.options.user.as_ref() {
            let token = self.do_authenticate(user.clone(), password.clone()).await?;
            self.auth_token.write_unpoisoned().replace(token);
        } else {
            let _ = self.auth_token.write_unpoisoned().take();
        }
        Ok(())
    }

    /// Updates the user credentials for the client in flight.
    ///
    /// Client will perform the authentication with the given user credentials. If successful, the
    /// authentication token will be updated in the client. Nothing happens if the authentication
    /// fails.
    ///
    /// If the user is `None`, it will remove the authentication token from the client.
    pub async fn update_user(&mut self, user: Option<(String, String)>) -> Result<()> {
        if let Some((ref name, ref password)) = user {
            let token = self.do_authenticate(name.clone(), password.clone()).await?;
            self.auth_token.write_unpoisoned().replace(token);
        } else {
            let _ = self.auth_token.write_unpoisoned().take();
        }
        self.options.user = user;
        Ok(())
    }
}

/// Options for `Connect` operation.
#[derive(Debug, Default, Clone)]
pub struct ConnectOptions {
    /// user is a pair values of name and password
    user: Option<(String, String)>,
    /// HTTP2 keep-alive: (keep_alive_interval, keep_alive_timeout)
    keep_alive: Option<(Duration, Duration)>,
    /// Whether send keep alive pings even there are no active streams.
    keep_alive_while_idle: bool,
    /// Apply a timeout to each gRPC request.
    timeout: Option<Duration>,
    /// Apply a timeout to connecting to the endpoint.
    connect_timeout: Option<Duration>,
    /// TCP keepalive.
    tcp_keepalive: Option<Duration>,
    #[cfg(feature = "tls")]
    tls: Option<TlsOptions>,
    #[cfg(feature = "tls-openssl")]
    otls: Option<OpenSslResult<OpenSslConnector>>,
    /// Require a leader to be present for the operation to complete.
    require_leader: bool,
    /// Max total attempts per unary RPC. `None` derives from the endpoint count.
    #[cfg(feature = "failover")]
    max_retries: Option<u32>,
    /// Base wait and jitter fraction between retry rounds.
    #[cfg(feature = "failover")]
    retry_backoff: Option<(Duration, f64)>,
    /// Auto-reconnect a broken watch stream. Defaults to the retry-enabled state.
    #[cfg(feature = "failover")]
    watch_reconnect: Option<bool>,
    /// Auto-reconnect a broken lease keep-alive stream. Defaults likewise.
    #[cfg(feature = "failover")]
    lease_keepalive_reconnect: Option<bool>,
}

impl ConnectOptions {
    /// name is the identifier for the distributed shared lock to be acquired.
    #[inline]
    pub fn with_user(mut self, name: impl Into<String>, password: impl Into<String>) -> Self {
        self.user = Some((name.into(), password.into()));
        self
    }

    /// Sets TLS options.
    ///
    /// Notes that this function have to work with `HTTPS` URLs.
    #[cfg_attr(docsrs, doc(cfg(feature = "tls")))]
    #[cfg(feature = "tls")]
    #[inline]
    pub fn with_tls(mut self, tls: TlsOptions) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Sets TLS options, however using the OpenSSL implementation.
    #[cfg_attr(docsrs, doc(cfg(feature = "tls-openssl")))]
    #[cfg(feature = "tls-openssl")]
    #[inline]
    pub fn with_openssl_tls(mut self, otls: OpenSslClientConfig) -> Self {
        // NOTE1: Perhaps we can unify the essential TLS config terms by something like `TlsBuilder`?
        //
        // NOTE2: we delay the checking at connection step to keep consistency with tonic, however would
        // things be better if we validate the config at here?
        self.otls = Some(otls.build());
        self
    }

    /// Enable HTTP2 keep-alive with `interval` and `timeout`.
    #[inline]
    pub fn with_keep_alive(mut self, interval: Duration, timeout: Duration) -> Self {
        self.keep_alive = Some((interval, timeout));
        self
    }

    /// Apply a timeout to each request.
    #[inline]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Apply a timeout to connecting to the endpoint.
    #[inline]
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Enable TCP keepalive.
    #[inline]
    pub fn with_tcp_keepalive(mut self, tcp_keepalive: Duration) -> Self {
        self.tcp_keepalive = Some(tcp_keepalive);
        self
    }

    /// Whether send keep alive pings even there are no active requests.
    /// If disabled, keep-alive pings are only sent while there are opened request/response streams.
    /// If enabled, pings are also sent when no streams are active.
    /// NOTE: Some implementations of gRPC server may send GOAWAY if there are too many pings.
    ///       This would be useful if you meet some error like `too many pings`.
    #[inline]
    pub fn with_keep_alive_while_idle(mut self, enabled: bool) -> Self {
        self.keep_alive_while_idle = enabled;
        self
    }

    /// Whether to enforce that a leader be present in the etcd cluster.
    #[inline]
    pub fn with_require_leader(mut self, require_leader: bool) -> Self {
        self.require_leader = require_leader;
        self
    }

    /// Sets the maximum number of attempts per unary RPC, counting the first
    /// try. On failure the client fails over to another endpoint. `0` or `1`
    /// disables retry. When unset, a default is derived from the endpoint count.
    ///
    /// Mutating RPCs are only retried when the request provably never reached a
    /// server, preserving write-at-most-once.
    ///
    /// Worst-case latency is about `max_attempts` times the per-request timeout
    /// plus backoff. The loop cannot interrupt a hung attempt, so pair failover
    /// with [`ConnectOptions::with_timeout`] to bound each try, otherwise a
    /// black-holed endpoint can stall the whole retry budget.
    #[cfg_attr(docsrs, doc(cfg(feature = "failover")))]
    #[cfg(feature = "failover")]
    #[inline]
    pub fn with_retries(mut self, max_attempts: u32) -> Self {
        self.max_retries = Some(max_attempts);
        self
    }

    /// Sets the base wait and jitter fraction between retry rounds. Defaults to
    /// 25ms and 0.10. Backoff is applied once per quorum of attempts so retries
    /// sweep a quorum of endpoints quickly, then pause.
    #[cfg_attr(docsrs, doc(cfg(feature = "failover")))]
    #[cfg(feature = "failover")]
    #[inline]
    pub fn with_retry_backoff(mut self, wait: Duration, jitter_fraction: f64) -> Self {
        self.retry_backoff = Some((wait, jitter_fraction));
        self
    }

    /// Enables or disables transparent watch-stream reconnection (default: on
    /// while retry is on). A broken watch resumes each active watch from the
    /// revision after the last one delivered.
    ///
    /// The reconnection task stops once no watches remain, so a `WatchStream`
    /// whose watches are all cancelled must be re-created rather than reused if
    /// a new watch is needed after a disconnect.
    #[cfg_attr(docsrs, doc(cfg(feature = "failover")))]
    #[cfg(feature = "failover")]
    #[inline]
    pub fn with_watch_reconnect(mut self, enabled: bool) -> Self {
        self.watch_reconnect = Some(enabled);
        self
    }

    /// Enables or disables transparent lease keep-alive reconnection (default:
    /// on while retry is on). A lease that expired during an outage surfaces as
    /// a `ttl <= 0` response.
    ///
    /// Reconnection re-primes the lease when the stream is re-established, but
    /// renewal cadence stays driven by the caller pumping
    /// [`LeaseKeeper::keep_alive`]. The client does not auto-renew, nor reap a
    /// lease that expires while the stream itself stays healthy.
    #[cfg_attr(docsrs, doc(cfg(feature = "failover")))]
    #[cfg(feature = "failover")]
    #[inline]
    pub fn with_lease_keepalive_reconnect(mut self, enabled: bool) -> Self {
        self.lease_keepalive_reconnect = Some(enabled);
        self
    }

    /// Creates a `ConnectOptions`.
    #[inline]
    pub const fn new() -> Self {
        ConnectOptions {
            user: None,
            keep_alive: None,
            keep_alive_while_idle: true,
            timeout: None,
            connect_timeout: None,
            tcp_keepalive: None,
            #[cfg(feature = "tls")]
            tls: None,
            #[cfg(feature = "tls-openssl")]
            otls: None,
            require_leader: false,
            #[cfg(feature = "failover")]
            max_retries: None,
            #[cfg(feature = "failover")]
            retry_backoff: None,
            #[cfg(feature = "failover")]
            watch_reconnect: None,
            #[cfg(feature = "failover")]
            lease_keepalive_reconnect: None,
        }
    }
}
