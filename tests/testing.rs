use etcd_client::{Client, ConnectOptions, Error};

pub const DEFAULT_TEST_ENDPOINT: &str = "localhost:2379";

pub type Result<T> = std::result::Result<T, Error>;

fn with_common_options(options: ConnectOptions) -> ConnectOptions {
    // Require a leader be present -- with only a single node, this
    // should never fail.
    options.with_require_leader(true)
}

/// Get client for testing.
pub async fn get_client() -> Result<Client> {
    let options = with_common_options(ConnectOptions::default());
    Client::connect([DEFAULT_TEST_ENDPOINT], Some(options)).await
}

/// Get a testing client with auth enabled.
#[allow(dead_code)]
pub async fn get_auth_client(options: Option<ConnectOptions>) -> Result<Client> {
    let options = with_common_options(options.unwrap_or_default())
        .with_user("root".to_owned(), "rootpwd".to_owned());
    Client::connect([DEFAULT_TEST_ENDPOINT], Some(options)).await
}
