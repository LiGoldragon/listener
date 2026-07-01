use signal_listener::ListenerDaemonConfiguration;

use crate::{Error, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Configuration {
    inner: ListenerDaemonConfiguration,
}

impl Configuration {
    pub fn new(inner: ListenerDaemonConfiguration) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &ListenerDaemonConfiguration {
        &self.inner
    }

    pub fn into_inner(self) -> ListenerDaemonConfiguration {
        self.inner
    }

    pub fn from_rkyv_bytes(bytes: &[u8]) -> Result<Self> {
        rkyv::from_bytes::<ListenerDaemonConfiguration, rkyv::rancor::Error>(bytes)
            .map(Self::new)
            .map_err(|_| Error::ConfigurationDecode)
    }

    pub fn to_rkyv_bytes(&self) -> Result<Vec<u8>> {
        rkyv::to_bytes::<rkyv::rancor::Error>(&self.inner)
            .map(|bytes| bytes.to_vec())
            .map_err(|_| Error::ConfigurationEncode)
    }
}
