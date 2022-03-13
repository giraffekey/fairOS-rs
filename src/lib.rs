mod client;
mod doc;
mod error;
mod filesystem;
mod kv;
mod pod;
mod user;

pub use client::Client;
pub use error::{FairOSError, FairOSPodError, FairOSUserError};
pub use filesystem::{
    Compression, DirEntry, DirInfo, FileBlock, FileEntry, FileInfo, SharedFileInfo,
};
pub use kv::{IndexType, KeyValueTable, KeyValueSeek};
pub use pod::{PodInfo, SharedPodInfo};
pub use user::{UserExport, UserInfo};

use serde::Deserialize;
