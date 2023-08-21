use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Bad peer ids")]
    BadIds,
}

pub type Result<T> = std::result::Result<T, Error>;
