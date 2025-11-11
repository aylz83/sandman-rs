pub mod bed;
pub mod error;
pub mod store;
pub mod tabix;

pub trait AsyncReadSeek: tokio::io::AsyncRead + tokio::io::AsyncSeek {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncSeek + ?Sized> AsyncReadSeek for T {}
