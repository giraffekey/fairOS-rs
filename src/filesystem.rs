use crate::{
    client::{MessageResponse, RequestError},
    error::{FairOSError, FairOSFileSystemError},
    Client,
};

use std::{collections::HashMap, fs, io::Read, path::Path, str::FromStr};

use bytes::Bytes;
use mime::Mime;
use multipart::client::lazy::Multipart;
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
struct DirEntryResponse {
    name: String,
    content_type: String,
    creation_time: String,
    modification_time: String,
    access_time: String,
}

#[derive(Debug, Deserialize)]
struct FileEntryResponse {
    name: String,
    content_type: String,
    size: String,
    block_size: String,
    creation_time: String,
    modification_time: String,
    access_time: String,
}

#[derive(Debug, Deserialize)]
struct DirListResponse {
    dirs: Option<Vec<DirEntryResponse>>,
    files: Option<Vec<FileEntryResponse>>,
}

#[derive(Debug, Deserialize)]
struct DirPresentResponse {
    present: bool,
}

#[derive(Debug, Deserialize)]
struct DirStatResponse {
    pod_name: String,
    dir_path: String,
    dir_name: String,
    creation_time: String,
    modification_time: String,
    access_time: String,
    no_of_directories: String,
    no_of_files: String,
}

#[derive(Debug, Deserialize)]
struct FileUploadFileNameResponse {
    file_name: String,
}

#[derive(Debug, Deserialize)]
struct FileUploadResponse {
    #[serde(rename = "Responses")]
    responses: Vec<FileUploadFileNameResponse>,
}

#[derive(Debug, Deserialize)]
struct FileShareResponse {
    file_sharing_reference: String,
}

#[derive(Debug, Deserialize)]
struct FileBlockResponse {
    name: String,
    reference: String,
    size: String,
    compressed_size: String,
}

#[derive(Debug, Deserialize)]
struct FileStatResponse {
    pod_name: String,
    file_path: String,
    file_name: String,
    content_type: String,
    file_size: String,
    block_size: String,
    compression: String,
    creation_time: String,
    modification_time: String,
    access_time: String,
    #[serde(rename = "Blocks")]
    blocks: Option<Vec<FileBlockResponse>>,
}

#[derive(Debug, Deserialize)]
struct FileReceiveResponse {
    file_name: String,
}

#[derive(Debug, Deserialize)]
struct FileReceiveInfoResponse {
    pod_name: String,
    name: String,
    content_type: String,
    size: String,
    block_size: String,
    number_of_blocks: String,
    compression: String,
    source_address: String,
    dest_address: String,
    shared_time: String,
}

#[derive(Debug)]
pub struct DirEntry {
    pub name: String,
    pub content_type: String,
    pub creation_time: u64,
    pub modification_time: u64,
    pub access_time: u64,
}

#[derive(Debug)]
pub struct FileEntry {
    pub name: String,
    pub content_type: String,
    pub size: u32,
    pub block_size: BlockSize,
    pub creation_time: u64,
    pub modification_time: u64,
    pub access_time: u64,
}

#[derive(Debug)]
pub struct DirInfo {
    pub pod: String,
    pub path: String,
    pub name: String,
    pub creation_time: u64,
    pub modification_time: u64,
    pub access_time: u64,
    pub no_of_dirs: u32,
    pub no_of_files: u32,
}

#[derive(Debug, PartialEq)]
pub enum Compression {
    Gzip,
    Snappy,
}

#[derive(Debug, PartialEq)]
pub struct FileBlock {
    pub name: String,
    pub reference: String,
    pub size: u32,
    pub compressed_size: u32,
}

#[derive(Debug)]
pub struct FileInfo {
    pub pod: String,
    pub path: String,
    pub name: String,
    pub content_type: Option<String>,
    pub size: u32,
    pub block_size: BlockSize,
    pub compression: Option<Compression>,
    pub creation_time: u64,
    pub modification_time: u64,
    pub access_time: u64,
    pub blocks: Vec<FileBlock>,
}

#[derive(Debug)]
pub struct SharedFileInfo {
    pub pod: String,
    pub name: String,
    pub content_type: Option<String>,
    pub size: u32,
    pub block_size: BlockSize,
    pub no_of_blocks: u32,
    pub compression: Option<Compression>,
    pub sender: String,
    pub receiver: String,
    pub shared_time: u64,
}

#[derive(Debug, PartialEq)]
pub enum BlockSize {
    Bytes(u32),
    Kilobytes(u32),
    Megabytes(u32),
    Gigabytes(u32),
    Terabytes(u32),
}

impl BlockSize {
    fn conversion(&self, divisor: u64) -> u32 {
        let bytes = match self {
            BlockSize::Bytes(n) => *n as u64,
            BlockSize::Kilobytes(n) => *n as u64 * 1_000,
            BlockSize::Megabytes(n) => *n as u64 * 1_000_000,
            BlockSize::Gigabytes(n) => *n as u64 * 1_000_000_000,
            BlockSize::Terabytes(n) => *n as u64 * 1_000_000_000_000,
        };
        (bytes / divisor) as u32
    }

    pub fn to_bytes(&self) -> Self {
        BlockSize::Bytes(self.conversion(1))
    }

    pub fn to_kilobytes(&self) -> Self {
        BlockSize::Kilobytes(self.conversion(1_000))
    }

    pub fn to_megabytes(&self) -> Self {
        BlockSize::Megabytes(self.conversion(1_000_000))
    }

    pub fn to_gigabytes(&self) -> Self {
        BlockSize::Gigabytes(self.conversion(1_000_000_000))
    }

    pub fn to_terabytes(&self) -> Self {
        BlockSize::Terabytes(self.conversion(1_000_000_000_000))
    }
}

impl ToString for BlockSize {
    fn to_string(&self) -> String {
        match self {
            BlockSize::Bytes(n) => format!("{}B", n),
            BlockSize::Kilobytes(n) => format!("{}K", n),
            BlockSize::Megabytes(n) => format!("{}M", n),
            BlockSize::Gigabytes(n) => format!("{}G", n),
            BlockSize::Terabytes(n) => format!("{}T", n),
        }
    }
}

impl FromStr for BlockSize {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut chars = s.chars().rev();
        let unit = chars.next().unwrap();
        let size = chars.rev().collect::<String>().parse().unwrap();
        match unit {
            'B' => Ok(BlockSize::Bytes(size)),
            'K' => Ok(BlockSize::Kilobytes(size)),
            'M' => Ok(BlockSize::Megabytes(size)),
            'G' => Ok(BlockSize::Gigabytes(size)),
            'T' => Ok(BlockSize::Terabytes(size)),
            _ => Err(()),
        }
    }
}

impl From<u64> for BlockSize {
    fn from(n: u64) -> Self {
        if n >= 1_000_000_000_000 {
            BlockSize::Terabytes((n / 1_000_000_000_000) as u32)
        } else if n >= 1_000_000_000 {
            BlockSize::Gigabytes((n / 1_000_000_000) as u32)
        } else if n >= 1_000_000 {
            BlockSize::Megabytes((n / 1_000_000) as u32)
        } else if n >= 1_000 {
            BlockSize::Kilobytes((n / 1_000) as u32)
        } else {
            BlockSize::Bytes(n as u32)
        }
    }
}

impl Client {
    pub async fn mkdir(&self, username: &str, pod: &str, path: &str) -> Result<(), FairOSError> {
        let data = json!({
            "pod_name": pod,
            "dir_path": path,
        })
        .to_string()
        .as_bytes()
        .to_vec();
        let cookie = self.cookie(username).unwrap();
        let _ = self
            .post::<MessageResponse>("/dir/mkdir", data, Some(cookie))
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::FileSystem(FairOSFileSystemError::Error),
            })?;
        Ok(())
    }

    pub async fn rmdir(&self, username: &str, pod: &str, path: &str) -> Result<(), FairOSError> {
        let data = json!({
            "pod_name": pod,
            "dir_path": path,
        })
        .to_string()
        .as_bytes()
        .to_vec();
        let cookie = self.cookie(username).unwrap();
        let _: MessageResponse =
            self.delete("/dir/rmdir", data, cookie)
                .await
                .map_err(|err| match err {
                    RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                    RequestError::Message(_) => {
                        FairOSError::FileSystem(FairOSFileSystemError::Error)
                    }
                })?;
        Ok(())
    }

    pub async fn ls(
        &self,
        username: &str,
        pod: &str,
        path: &str,
    ) -> Result<(Vec<DirEntry>, Vec<FileEntry>), FairOSError> {
        let mut query = HashMap::new();
        query.insert("pod_name", pod);
        query.insert("dir_path", path);
        let cookie = self.cookie(username).unwrap();
        let res: DirListResponse =
            self.get("/dir/ls", query, Some(cookie))
                .await
                .map_err(|err| match err {
                    RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                    RequestError::Message(_) => {
                        FairOSError::FileSystem(FairOSFileSystemError::Error)
                    }
                })?;
        let dirs = match res.dirs {
            Some(dirs) => dirs
                .iter()
                .map(|entry| DirEntry {
                    name: entry.name.clone(),
                    content_type: entry.content_type.clone(),
                    creation_time: entry.creation_time.parse().unwrap(),
                    modification_time: entry.modification_time.parse().unwrap(),
                    access_time: entry.access_time.parse().unwrap(),
                })
                .collect(),
            None => Vec::new(),
        };
        let files = match res.files {
            Some(files) => files
                .iter()
                .map(|entry| FileEntry {
                    name: entry.name.clone(),
                    content_type: entry.content_type.clone(),
                    size: entry.size.parse().unwrap(),
                    block_size: BlockSize::from(entry.block_size.parse::<u64>().unwrap()),
                    creation_time: entry.creation_time.parse().unwrap(),
                    modification_time: entry.modification_time.parse().unwrap(),
                    access_time: entry.access_time.parse().unwrap(),
                })
                .collect(),
            None => Vec::new(),
        };
        Ok((dirs, files))
    }

    pub async fn dir_exists(
        &self,
        username: &str,
        pod: &str,
        path: &str,
    ) -> Result<bool, FairOSError> {
        let mut query = HashMap::new();
        query.insert("pod_name", pod);
        query.insert("dir_path", path);
        let cookie = self.cookie(username).unwrap();
        let res: DirPresentResponse = self
            .get("/dir/present", query, Some(cookie))
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::FileSystem(FairOSFileSystemError::Error),
            })?;
        Ok(res.present)
    }

    pub async fn dir_info(
        &self,
        username: &str,
        pod: &str,
        path: &str,
    ) -> Result<DirInfo, FairOSError> {
        let mut query = HashMap::new();
        query.insert("pod_name", pod);
        query.insert("dir_path", path);
        let cookie = self.cookie(username).unwrap();
        let res: DirStatResponse =
            self.get("/dir/stat", query, Some(cookie))
                .await
                .map_err(|err| match err {
                    RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                    RequestError::Message(_) => {
                        FairOSError::FileSystem(FairOSFileSystemError::Error)
                    }
                })?;
        Ok(DirInfo {
            pod: res.pod_name,
            path: res.dir_path,
            name: res.dir_name,
            creation_time: res.creation_time.parse().unwrap(),
            modification_time: res.modification_time.parse().unwrap(),
            access_time: res.access_time.parse().unwrap(),
            no_of_dirs: res.no_of_directories.parse().unwrap(),
            no_of_files: res.no_of_files.parse().unwrap(),
        })
    }

    pub async fn upload_buffer<R: Read>(
        &self,
        username: &str,
        pod: &str,
        dir: &str,
        file_name: &str,
        buffer: R,
        mime: Mime,
        block_size: BlockSize,
        compression: Option<Compression>,
    ) -> Result<String, FairOSError> {
        let mut multipart = Multipart::new();
        multipart.add_text("pod_name", pod);
        multipart.add_text("dir_path", dir);
        multipart.add_text("block_size", block_size.to_string());
        multipart.add_stream("files", buffer, Some(file_name), Some(mime));
        let mut prepared = multipart.prepare().unwrap();
        let boundary = prepared.boundary().to_string();
        let mut body = Vec::new();
        prepared.read_to_end(&mut body).unwrap();

        let cookie = self.cookie(username).unwrap();
        let compression = match compression {
            Some(compression) => match compression {
                Compression::Gzip => Some("gzip"),
                Compression::Snappy => Some("snappy"),
            },
            None => None,
        };
        let res: FileUploadResponse = self
            .upload_multipart("/file/upload", body, boundary.as_str(), cookie, compression)
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::FileSystem(FairOSFileSystemError::Error),
            })?;
        Ok(res.responses.get(0).unwrap().file_name.clone())
    }

    pub async fn upload_file<P: AsRef<Path>>(
        &self,
        username: &str,
        pod: &str,
        dir: &str,
        local_path: P,
        block_size: BlockSize,
        compression: Option<Compression>,
    ) -> Result<String, FairOSError> {
        let mut multipart = Multipart::new();
        multipart.add_text("pod_name", pod);
        multipart.add_text("dir_path", dir);
        multipart.add_text("block_size", block_size.to_string());
        multipart.add_file("files", local_path.as_ref());
        let mut prepared = multipart.prepare().unwrap();
        let boundary = prepared.boundary().to_string();
        let mut body = Vec::new();
        prepared.read_to_end(&mut body).unwrap();

        let cookie = self.cookie(username).unwrap();
        let compression = match compression {
            Some(compression) => match compression {
                Compression::Gzip => Some("gzip"),
                Compression::Snappy => Some("snappy"),
            },
            None => None,
        };
        let res: FileUploadResponse = self
            .upload_multipart("/file/upload", body, boundary.as_str(), cookie, compression)
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::FileSystem(FairOSFileSystemError::Error),
            })?;
        Ok(res.responses.get(0).unwrap().file_name.clone())
    }

    pub async fn download_buffer(
        &self,
        username: &str,
        pod: &str,
        path: &str,
    ) -> Result<Bytes, FairOSError> {
        let mut multipart = Multipart::new();
        multipart.add_text("pod_name", pod);
        multipart.add_text("file_path", path);
        let mut prepared = multipart.prepare().unwrap();
        let boundary = prepared.boundary().to_string();
        let mut body = Vec::new();
        prepared.read_to_end(&mut body).unwrap();

        let cookie = self.cookie(username).unwrap();
        let buf = self
            .download_multipart("/file/download", body, boundary.as_str(), cookie)
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::FileSystem(FairOSFileSystemError::Error),
            })?;
        Ok(buf)
    }

    pub async fn download_file<P: AsRef<Path>>(
        &self,
        username: &str,
        pod: &str,
        path: &str,
        local_path: P,
    ) -> Result<(), FairOSError> {
        let mut multipart = Multipart::new();
        multipart.add_text("pod_name", pod);
        multipart.add_text("file_path", path);
        let mut prepared = multipart.prepare().unwrap();
        let boundary = prepared.boundary().to_string();
        let mut body = Vec::new();
        prepared.read_to_end(&mut body).unwrap();

        let cookie = self.cookie(username).unwrap();
        let buf = self
            .download_multipart("/file/download", body, boundary.as_str(), cookie)
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::FileSystem(FairOSFileSystemError::Error),
            })?;
        fs::write(local_path, buf).unwrap();

        Ok(())
    }

    pub async fn share_file(
        &self,
        username: &str,
        pod: &str,
        path: &str,
        receiver: &str,
    ) -> Result<String, FairOSError> {
        let data = json!({
            "pod_name": pod,
            "file_path": path,
            "dest_user": receiver,
        })
        .to_string()
        .as_bytes()
        .to_vec();
        let cookie = self.cookie(username).unwrap();
        let (res, _) = self
            .post::<FileShareResponse>("/file/share", data, Some(cookie))
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::FileSystem(FairOSFileSystemError::Error),
            })?;
        Ok(res.file_sharing_reference)
    }

    pub async fn rm(&self, username: &str, pod: &str, path: &str) -> Result<(), FairOSError> {
        let data = json!({
            "pod_name": pod,
            "file_path": path,
        })
        .to_string()
        .as_bytes()
        .to_vec();
        let cookie = self.cookie(username).unwrap();
        let _: MessageResponse =
            self.delete("/file/delete", data, cookie)
                .await
                .map_err(|err| match err {
                    RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                    RequestError::Message(_) => {
                        FairOSError::FileSystem(FairOSFileSystemError::Error)
                    }
                })?;
        Ok(())
    }

    pub async fn file_info(
        &self,
        username: &str,
        pod: &str,
        path: &str,
    ) -> Result<FileInfo, FairOSError> {
        let mut query = HashMap::new();
        query.insert("pod_name", pod);
        query.insert("file_path", path);
        let cookie = self.cookie(username).unwrap();
        let res: FileStatResponse =
            self.get("/file/stat", query, Some(cookie))
                .await
                .map_err(|err| match err {
                    RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                    RequestError::Message(_) => {
                        FairOSError::FileSystem(FairOSFileSystemError::Error)
                    }
                })?;
        let content_type = if res.content_type.is_empty() {
            None
        } else {
            Some(res.content_type)
        };
        let compression = match res.compression.as_str() {
            "gzip" => Some(Compression::Gzip),
            "snappy" => Some(Compression::Snappy),
            "" => None,
            _ => unreachable!(),
        };
        let blocks = match res.blocks {
            Some(blocks) => blocks
                .iter()
                .map(|entry| FileBlock {
                    name: entry.name.clone(),
                    reference: entry.reference.clone(),
                    size: entry.size.parse().unwrap(),
                    compressed_size: entry.compressed_size.parse().unwrap(),
                })
                .collect(),
            None => Vec::new(),
        };
        Ok(FileInfo {
            pod: res.pod_name,
            path: res.file_path,
            name: res.file_name,
            content_type,
            size: res.file_size.parse().unwrap(),
            block_size: BlockSize::from(res.block_size.parse::<u64>().unwrap()),
            compression,
            creation_time: res.creation_time.parse().unwrap(),
            modification_time: res.modification_time.parse().unwrap(),
            access_time: res.access_time.parse().unwrap(),
            blocks,
        })
    }

    pub async fn receive_shared_file(
        &self,
        username: &str,
        pod: &str,
        reference: &str,
        dir: &str,
    ) -> Result<String, FairOSError> {
        let mut query = HashMap::new();
        query.insert("pod_name", pod);
        query.insert("sharing_ref", reference);
        query.insert("dir_path", dir);
        let cookie = self.cookie(username).unwrap();
        let res: FileReceiveResponse = self
            .get("/file/receive", query, Some(cookie))
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::FileSystem(FairOSFileSystemError::Error),
            })?;
        Ok(res.file_name)
    }

    pub async fn shared_file_info(
        &self,
        username: &str,
        pod: &str,
        reference: &str,
    ) -> Result<SharedFileInfo, FairOSError> {
        let mut query = HashMap::new();
        query.insert("pod_name", pod);
        query.insert("sharing_ref", reference);
        let cookie = self.cookie(username).unwrap();
        let res: FileReceiveInfoResponse = self
            .get("/file/receiveinfo", query, Some(cookie))
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::FileSystem(FairOSFileSystemError::Error),
            })?;
        let content_type = if res.content_type.is_empty() {
            None
        } else {
            Some(res.content_type)
        };
        let compression = match res.compression.as_str() {
            "gzip" => Some(Compression::Gzip),
            "snappy" => Some(Compression::Snappy),
            "" => None,
            _ => unreachable!(),
        };
        Ok(SharedFileInfo {
            pod: res.pod_name,
            name: res.name,
            content_type,
            size: res.size.parse().unwrap(),
            block_size: BlockSize::from(res.block_size.parse::<u64>().unwrap()),
            no_of_blocks: res.number_of_blocks.parse().unwrap(),
            compression,
            sender: res.source_address,
            receiver: res.dest_address,
            shared_time: res.shared_time.parse().unwrap(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{Client, BlockSize, Compression};
    use bytes::Buf;
    use rand::{
        distributions::{Alphanumeric, Uniform},
        thread_rng, Rng,
    };
    use std::fs;

    fn random_name() -> String {
        thread_rng()
            .sample_iter(Alphanumeric)
            .take(8)
            .map(char::from)
            .collect()
    }

    fn random_password() -> String {
        thread_rng()
            .sample_iter(Uniform::new_inclusive(0, 255))
            .take(8)
            .map(char::from)
            .collect()
    }

    #[tokio::test]
    async fn test_mkdir_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod = random_name();
        let res = fairos.create_pod(&username, &pod, &password).await;
        assert!(res.is_ok());
        let res = fairos.mkdir(&username, &pod, "/Documents").await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_rmdir_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod = random_name();
        let res = fairos.create_pod(&username, &pod, &password).await;
        assert!(res.is_ok());
        let res = fairos.mkdir(&username, &pod, "/Documents").await;
        assert!(res.is_ok());
        let res = fairos.rmdir(&username, &pod, "/Documents").await;
        assert!(res.is_ok());
        let res = fairos.dir_exists(&username, &pod, "/Documents").await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), false);
    }

    #[tokio::test]
    async fn test_ls_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod = random_name();
        let res = fairos.create_pod(&username, &pod, &password).await;
        assert!(res.is_ok());
        let res = fairos.mkdir(&username, &pod, "/Documents").await;
        assert!(res.is_ok());
        let res = fairos.mkdir(&username, &pod, "/Music").await;
        assert!(res.is_ok());
        let res = fairos.mkdir(&username, &pod, "/Videos").await;
        assert!(res.is_ok());
        let res = fairos
            .upload_buffer(
                &username,
                &pod,
                "/",
                "todo.txt",
                "go to the store".as_bytes(),
                mime::TEXT_PLAIN,
                BlockSize::Kilobytes(1),
                Some(Compression::Gzip),
            )
            .await;
        assert!(res.is_ok());
        let res = fairos.ls(&username, &pod, "/").await;
        assert!(res.is_ok());
        let (dirs, files) = res.unwrap();
        assert_eq!(
            dirs.iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<&str>>(),
            vec!["Documents", "Music", "Videos"],
        );
        assert_eq!(
            files
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<&str>>(),
            vec!["todo.txt"],
        );
    }

    #[tokio::test]
    async fn test_dir_exists_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod = random_name();
        let res = fairos.create_pod(&username, &pod, &password).await;
        assert!(res.is_ok());
        let res = fairos.mkdir(&username, &pod, "/Documents").await;
        assert!(res.is_ok());
        let res = fairos.dir_exists(&username, &pod, "/Documents").await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), true);
        let res = fairos.dir_exists(&username, &pod, "/Music").await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), false);
    }

    #[tokio::test]
    async fn test_dir_info_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod = random_name();
        let res = fairos.create_pod(&username, &pod, &password).await;
        assert!(res.is_ok());
        let res = fairos.mkdir(&username, &pod, "/Documents").await;
        assert!(res.is_ok());
        let res = fairos.mkdir(&username, &pod, "/Documents/Text").await;
        assert!(res.is_ok());
        let res = fairos.dir_info(&username, &pod, "/Documents").await;
        assert!(res.is_ok());
        let info = res.unwrap();
        assert_eq!(info.pod, pod);
        assert_eq!(info.path, "/");
        assert_eq!(info.name, "Documents");
        assert_eq!(info.no_of_dirs, 1);
        assert_eq!(info.no_of_files, 0);
    }

    #[tokio::test]
    async fn test_upload_buffer_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod = random_name();
        let res = fairos.create_pod(&username, &pod, &password).await;
        assert!(res.is_ok());
        let res = fairos.mkdir(&username, &pod, "/Documents").await;
        assert!(res.is_ok());
        let res = fairos
            .upload_buffer(
                &username,
                &pod,
                "/Documents",
                "hello.txt",
                "hello world".as_bytes(),
                mime::TEXT_PLAIN,
                BlockSize::Kilobytes(1),
                Some(Compression::Gzip),
            )
            .await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), "hello.txt");
    }

    #[tokio::test]
    async fn test_upload_file_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod = random_name();
        let res = fairos.create_pod(&username, &pod, &password).await;
        assert!(res.is_ok());
        let res = fairos.mkdir(&username, &pod, "/Documents").await;
        assert!(res.is_ok());
        fs::write("upload.txt", "hello world").unwrap();
        let res = fairos
            .upload_file(
                &username,
                &pod,
                "/Documents",
                "upload.txt",
                BlockSize::Kilobytes(1),
                Some(Compression::Snappy),
            )
            .await;
        fs::remove_file("upload.txt").unwrap();
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), "upload.txt");
    }

    #[tokio::test]
    async fn test_download_buffer_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod = random_name();
        let res = fairos.create_pod(&username, &pod, &password).await;
        assert!(res.is_ok());
        let res = fairos.mkdir(&username, &pod, "/Documents").await;
        assert!(res.is_ok());
        let res = fairos
            .upload_buffer(
                &username,
                &pod,
                "/Documents",
                "hello.txt",
                "hello world".as_bytes(),
                mime::TEXT_PLAIN,
                BlockSize::Kilobytes(1),
                None,
            )
            .await;
        assert!(res.is_ok());
        let res = fairos
            .download_buffer(&username, &pod, "/Documents/hello.txt")
            .await;
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let mut data = [0u8; 11];
        buf.copy_to_slice(&mut data);
        assert_eq!(&data, b"hello world");
    }

    #[tokio::test]
    async fn test_download_file_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod = random_name();
        let res = fairos.create_pod(&username, &pod, &password).await;
        assert!(res.is_ok());
        let res = fairos.mkdir(&username, &pod, "/Documents").await;
        assert!(res.is_ok());
        let res = fairos
            .upload_buffer(
                &username,
                &pod,
                "/Documents",
                "hello.txt",
                "hello world".as_bytes(),
                mime::TEXT_PLAIN,
                BlockSize::Kilobytes(1),
                None,
            )
            .await;
        assert!(res.is_ok());
        let res = fairos
            .download_file(&username, &pod, "/Documents/hello.txt", "download.txt")
            .await;
        assert!(res.is_ok());
        assert_eq!(fs::read("download.txt").unwrap(), b"hello world");
        fs::remove_file("download.txt").unwrap();
    }

    #[tokio::test]
    async fn test_share_file_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let res = fairos
            .signup(&random_name(), &random_password(), None)
            .await;
        assert!(res.is_ok());
        let (receiver, _) = res.unwrap();
        let pod = random_name();
        let res = fairos.create_pod(&username, &pod, &password).await;
        assert!(res.is_ok());
        let res = fairos.mkdir(&username, &pod, "/Documents").await;
        assert!(res.is_ok());
        let res = fairos
            .upload_buffer(
                &username,
                &pod,
                "/Documents",
                "hello.txt",
                "hello world".as_bytes(),
                mime::TEXT_PLAIN,
                BlockSize::Kilobytes(1),
                None,
            )
            .await;
        assert!(res.is_ok());
        let res = fairos
            .share_file(&username, &pod, "/Documents/hello.txt", &receiver)
            .await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_rm_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod = random_name();
        let res = fairos.create_pod(&username, &pod, &password).await;
        assert!(res.is_ok());
        let res = fairos.mkdir(&username, &pod, "/Documents").await;
        assert!(res.is_ok());
        let res = fairos
            .upload_buffer(
                &username,
                &pod,
                "/Documents",
                "hello.txt",
                "hello world".as_bytes(),
                mime::TEXT_PLAIN,
                BlockSize::Kilobytes(1),
                None,
            )
            .await;
        assert!(res.is_ok());
        let res = fairos.rm(&username, &pod, "/Documents/hello.txt").await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_file_info_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod = random_name();
        let res = fairos.create_pod(&username, &pod, &password).await;
        assert!(res.is_ok());
        let res = fairos.mkdir(&username, &pod, "/Documents").await;
        assert!(res.is_ok());
        let res = fairos
            .upload_buffer(
                &username,
                &pod,
                "/Documents",
                "hello.txt",
                "hello world".as_bytes(),
                mime::TEXT_PLAIN,
                BlockSize::Kilobytes(1),
                Some(Compression::Gzip),
            )
            .await;
        assert!(res.is_ok());
        let res = fairos
            .file_info(&username, &pod, "/Documents/hello.txt")
            .await;
        assert!(res.is_ok());
        let info = res.unwrap();
        assert_eq!(info.pod, pod);
        assert_eq!(info.path, "/Documents");
        assert_eq!(info.name, "hello.txt");
        assert_eq!(info.content_type, None);
        assert_eq!(info.size, "hello world".as_bytes().len() as u32);
        assert_eq!(info.block_size, BlockSize::Kilobytes(1));
        assert_eq!(info.compression, Some(Compression::Gzip));
        assert_eq!(info.blocks.len(), 0);
    }

    #[tokio::test]
    async fn test_receive_shared_file_succeeds() {
        let mut fairos = Client::new();

        let username1 = random_name();
        let password1 = random_password();
        let res = fairos.signup(&username1, &password1, None).await;
        assert!(res.is_ok());
        let username2 = random_name();
        let password2 = random_password();
        let res = fairos.signup(&username2, &password2, None).await;
        assert!(res.is_ok());
        let (receiver, _) = res.unwrap();
        let pod = random_name();
        let res = fairos.create_pod(&username1, &pod, &password1).await;
        assert!(res.is_ok());
        let res = fairos.mkdir(&username1, &pod, "/Documents").await;
        assert!(res.is_ok());
        let res = fairos
            .upload_buffer(
                &username1,
                &pod,
                "/Documents",
                "hello.txt",
                "hello world".as_bytes(),
                mime::TEXT_PLAIN,
                BlockSize::Kilobytes(1),
                None,
            )
            .await;
        assert!(res.is_ok());
        let res = fairos
            .share_file(&username1, &pod, "/Documents/hello.txt", &receiver)
            .await;
        assert!(res.is_ok());
        let reference = res.unwrap();

        let pod = random_name();
        let res = fairos.create_pod(&username2, &pod, &password2).await;
        assert!(res.is_ok());
        let res = fairos.mkdir(&username2, &pod, "/Shared").await;
        assert!(res.is_ok());
        let res = fairos
            .receive_shared_file(&username2, &pod, &reference, "/Shared")
            .await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), "/Shared/hello.txt");
    }

    #[tokio::test]
    async fn test_shared_file_info_succeeds() {
        let mut fairos = Client::new();

        let username1 = random_name();
        let password1 = random_password();
        let res = fairos.signup(&username1, &password1, None).await;
        assert!(res.is_ok());
        let (_sender, _) = res.unwrap();
        let username2 = random_name();
        let password2 = random_password();
        let res = fairos.signup(&username2, &password2, None).await;
        assert!(res.is_ok());
        let (receiver, _) = res.unwrap();
        let pod1 = random_name();
        let res = fairos.create_pod(&username1, &pod1, &password1).await;
        assert!(res.is_ok());
        let res = fairos.mkdir(&username1, &pod1, "/Documents").await;
        assert!(res.is_ok());
        let res = fairos
            .upload_buffer(
                &username1,
                &pod1,
                "/Documents",
                "hello.txt",
                "hello world".as_bytes(),
                mime::TEXT_PLAIN,
                BlockSize::Kilobytes(1),
                None,
            )
            .await;
        assert!(res.is_ok());
        let res = fairos
            .share_file(&username1, &pod1, "/Documents/hello.txt", &receiver)
            .await;
        assert!(res.is_ok());
        let reference = res.unwrap();

        let pod2 = random_name();
        let res = fairos.create_pod(&username2, &pod2, &password2).await;
        assert!(res.is_ok());
        let res = fairos.mkdir(&username2, &pod2, "/Shared").await;
        assert!(res.is_ok());
        let res = fairos
            .receive_shared_file(&username2, &pod2, &reference, "/Shared")
            .await;
        assert!(res.is_ok());
        let res = fairos.shared_file_info(&username2, &pod2, &reference).await;
        assert!(res.is_ok());
        let info = res.unwrap();
        assert_eq!(info.pod, pod1);
        assert_eq!(info.name, "hello.txt");
        assert_eq!(info.content_type, None);
        assert_eq!(info.size, "hello world".as_bytes().len() as u32);
        assert_eq!(info.block_size, BlockSize::Kilobytes(1));
        assert_eq!(info.no_of_blocks, 1);
        assert_eq!(info.compression, None);
        // assert_eq!(info.sender, sender);
        // assert_eq!(info.receiver, receiver);
    }
}
