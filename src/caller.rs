use std::{
    future::Future,
    sync::{Arc, RwLock},
};

use tonic::{
    metadata::{Ascii, MetadataValue},
    Code,
};

use crate::{
    client::AuthToken, error::Result, intercept::InterceptedChannel, lock::RwLockExt, AuthClient,
    Error::GRpcStatus,
};

/// Describe the options for the client request caller.
#[derive(Clone)]
pub struct CallOptions {
    /// Authentication credentials if present.
    pub creds: Arc<RwLock<Option<(String, String)>>>,
    /// Whether to automatically refresh an expired token.
    /// Requires each request cloning.
    pub refresh_expired_token: bool,
}

/// Helps to build a [`ClientCaller`].
#[derive(Clone)]
pub struct ClientCallerBuilder {
    options: CallOptions,
    auth_token: AuthToken,
    auth_client: AuthClient,
    channel: InterceptedChannel,
}

impl ClientCallerBuilder {
    /// Make a new [`ClientCallerBuilder`].
    pub fn new(
        options: CallOptions,
        auth_token: AuthToken,
        auth_client: AuthClient,
        channel: InterceptedChannel,
    ) -> Self {
        Self {
            options,
            auth_token,
            auth_client,
            channel,
        }
    }

    /// Build a new [`ClientCaller`] passing an actual client implementation.
    pub fn build<T>(self, f_inner: impl FnOnce(InterceptedChannel) -> T) -> ClientCaller<T> {
        ClientCaller::new(
            f_inner(self.channel),
            self.auth_client,
            self.auth_token,
            self.options,
        )
    }
}

/// This struct is responsible for dispatching the actions required for each
/// client request. It is parameterized by the inner client implementation, so
/// it can be reused across different clients. Its main method, [`do_call`],
/// controls how a user request is performed. Currently, the only supported
/// feature is automatic authentication token refresh upon expiration.
///
/// [`do_call`]: `ClientCaller::do_call`
#[derive(Clone)]
pub struct ClientCaller<T> {
    inner: T,
    auth_client: AuthClient,
    auth_token: AuthToken,
    options: CallOptions,
    #[cfg(feature = "failover")]
    retry: crate::failover::RetryConfig,
}

impl<T> ClientCaller<T> {
    /// Make a new [`ClientCaller`].
    pub fn new(
        inner: T,
        auth_client: AuthClient,
        auth_token: AuthToken,
        options: CallOptions,
    ) -> Self {
        Self {
            inner,
            auth_client,
            auth_token,
            options,
            // Placeholder, overwritten by `set_retry` once the endpoint count is
            // known.
            #[cfg(feature = "failover")]
            retry: crate::failover::RetryConfig::disabled(),
        }
    }

    /// Installs the failover config (called once at client construction).
    #[cfg(feature = "failover")]
    pub(crate) fn set_retry(&mut self, retry: crate::failover::RetryConfig) {
        self.retry = retry;
    }

    /// Refresh the authentication token if the client has credentials options.
    pub async fn refresh_token(&self) -> Result<()> {
        let creds = self.options.creds.read_unpoisoned().clone();
        if let Some((user, password)) = creds {
            let token = self.do_authenticate(user, password).await?;
            self.auth_token.write_unpoisoned().replace(token);
        } else {
            let _ = self.auth_token.write_unpoisoned().take();
        }
        Ok(())
    }

    /// Update a user.
    ///
    /// Client will perform the authentication with the given user credentials. If successful, the
    /// authentication token will be updated in the client. Nothing happens if the authentication
    /// fails.
    ///
    /// If the user is `None`, it will remove the authentication token from the client.
    pub async fn update_user(&mut self, creds: Option<(String, String)>) -> Result<()> {
        if let Some((ref name, ref password)) = creds {
            let token = self.do_authenticate(name.clone(), password.clone()).await?;
            self.auth_token.write_unpoisoned().replace(token);
        } else {
            let _ = self.auth_token.write_unpoisoned().take();
        }
        *self.options.creds.write_unpoisoned() = creds;
        Ok(())
    }

    /// Mutate an inner client.
    pub fn with(mut self, f: impl FnOnce(T) -> T) -> Self {
        self.inner = f(self.inner);
        self
    }

    /// Performs a client request. Takes a request `req` and the function that
    /// actually sends it.
    ///
    /// # Note
    ///
    /// The arguments are separate to support scenarios in which `req` is not
    /// cloned. For example, passing a single closure would require an [`Fn`],
    /// which would likely unconditionally clone the captured request.
    pub async fn do_call<Req, C, Ret>(&mut self, req: Req, call: C) -> Result<Ret>
    where
        for<'a> C: ClientCall<&'a mut T, Req, Output = Result<Ret>>,
        Req: Clone,
    {
        let has_creds = self.options.creds.read_unpoisoned().is_some();
        if !self.options.refresh_expired_token || !has_creds {
            // Refreshing is disabled or credentials are not set.
            // Pass the request directly avoiding cloning.
            return (call)(&mut self.inner, req).await;
        }

        // Clone the request to be able to retry in the case of token expiration.
        let resp = (call)(&mut self.inner, req.clone()).await;
        match resp {
            Err(GRpcStatus(status)) if status.code() == Code::Unauthenticated => {
                // Re-authenticate and retry this query.
                self.refresh_token().await?;
                (call)(&mut self.inner, req).await
            }
            res => res,
        }
    }

    async fn do_authenticate(
        &self,
        user: String,
        password: String,
    ) -> Result<MetadataValue<Ascii>> {
        #[cfg(not(feature = "failover"))]
        let resp = self
            .auth_client
            .clone()
            .authenticate(user, password)
            .await?;

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
                    .auth_client
                    .clone()
                    .authenticate(user.clone(), password.clone())
                    .await
                {
                    Ok(resp) => {
                        ok = Some(resp);
                        break;
                    }
                    Err(e) => match classify(&e, RetryPolicy::Repeatable) {
                        Decision::Retry => {
                            tracing::warn!(
                                target: "etcd_client::failover",
                                attempt = attempt + 1,
                                max,
                                error = %e,
                                "etcd authenticate RPC failed, failing over to another endpoint",
                            );
                            last = Some(e);
                        }
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
}

/// The auxiliary trait to express an async call over the mutable client.
pub trait ClientCall<Client, Req>: Fn(Client, Req) -> Self::OutputFuture {
    type Output;
    type OutputFuture: Future<Output = <Self as ClientCall<Client, Req>>::Output>;
}

impl<F, Fut, Client, Req> ClientCall<Client, Req> for F
where
    F: Fn(Client, Req) -> Fut,
    Fut: Future,
{
    type OutputFuture = Fut;
    type Output = Fut::Output;
}
