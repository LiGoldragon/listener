use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("configuration archive encode failed")]
    ConfigurationEncode,

    #[error("configuration archive decode failed")]
    ConfigurationDecode,

    #[error("write output: {0}")]
    Output(#[from] std::io::Error),

    #[error("{surface} is scaffolded but not implemented")]
    NotImplemented { surface: &'static str },
}
