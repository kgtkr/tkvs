use serde::Deserialize;
use std::{
    error::Error,
    net::{IpAddr, Ipv4Addr},
};

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    #[serde(default = "port_default")]
    pub port: u16,
    #[serde(default = "ip_default")]
    pub ip: IpAddr,
    pub data: String,
}

// serde_envがprefixに未対応なので
#[derive(Debug, Clone, Deserialize)]
struct PrefixedAppConfig {
    tkvs: AppConfig,
}

fn port_default() -> u16 {
    50051
}

fn ip_default() -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))
}

impl AppConfig {
    pub fn from_env() -> Result<AppConfig, Box<dyn Error + Send + Sync>> {
        Ok(serde_env::from_env::<PrefixedAppConfig>()?.tkvs)
    }
}
