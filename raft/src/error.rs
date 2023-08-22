use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Bad peer ids")]
    BadIds,
    #[error("Couldn't get state lock")]
    FailedLock,
}

pub type Result<T> = std::result::Result<T, Error>;
