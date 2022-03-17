use crate::{
    client::{MessageResponse, RequestError},
    error::{FairOSError, FairOSKeyValueError},
    Client,
};

use core::pin::Pin;
use std::{collections::HashMap, io::Read, path::Path};

use futures::{
    task::{Context, Poll},
    Future, Stream,
};
use multipart::client::lazy::Multipart;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Deserialize)]
struct KvCountResponse {
    count: u32,
}

#[derive(Debug, Deserialize)]
struct KvListTableResponse {
    table_name: String,
    indexes: Vec<String>,
    r#type: String,
}

#[derive(Debug, Deserialize)]
struct KvListResponse {
    #[serde(rename = "Tables")]
    tables: Vec<KvListTableResponse>,
}

#[derive(Debug, Deserialize)]
struct KvEntryGetResponse {
    keys: Vec<String>,
    values: String,
}

#[derive(Debug, Deserialize)]
struct KvPresentResponse {
    present: bool,
}

#[derive(Debug)]
pub enum IndexType {
    Str,
    Number,
}

#[derive(Debug, PartialEq)]
pub struct KeyValueStore {
    pub name: String,
    pub indexes: Vec<String>,
}

pub struct KeyValueSeek<'a> {
    client: &'a Client,
    username: String,
    pod: String,
    store: String,
    limit: Option<u32>,
}

impl Stream for KeyValueSeek<'_> {
    type Item = (String, String);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut query = HashMap::new();
        query.insert("pod_name", self.pod.as_str());
        query.insert("table_name", self.store.as_str());
        let cookie = self.client.cookie(&self.username).unwrap();
        let mut req = self
            .client
            .get::<KvEntryGetResponse>("/kv/seek/next", query, Some(cookie));
        match unsafe { Pin::new_unchecked(&mut req) }.poll(cx) {
            Poll::Ready(res) => match res {
                Ok(res) => {
                    let key = res.keys.get(0).unwrap().clone();
                    Poll::Ready(Some((key, res.values)))
                }
                Err(_) => Poll::Ready(None),
            },
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if let Some(limit) = self.limit {
            (0, Some(limit as usize))
        } else {
            (0, None)
        }
    }
}

impl Client {
    pub async fn create_kv_store(
        &self,
        username: &str,
        pod: &str,
        name: &str,
        index_type: IndexType,
    ) -> Result<(), FairOSError> {
        let index_type = match index_type {
            IndexType::Str => "string",
            IndexType::Number => "number",
        };
        let data = json!({
            "pod_name": pod,
            "table_name": name,
            "indexType": index_type,
        })
        .to_string()
        .as_bytes()
        .to_vec();
        let cookie = self.cookie(username).unwrap();
        let _ = self
            .post::<MessageResponse>("/kv/new", data, Some(cookie))
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::KeyValue(FairOSKeyValueError::Error),
            })?;
        Ok(())
    }

    pub async fn open_kv_store(
        &self,
        username: &str,
        pod: &str,
        name: &str,
    ) -> Result<(), FairOSError> {
        let data = json!({
            "pod_name": pod,
            "table_name": name,
        })
        .to_string()
        .as_bytes()
        .to_vec();
        let cookie = self.cookie(username).unwrap();
        let _ = self
            .post::<MessageResponse>("/kv/open", data, Some(cookie))
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::KeyValue(FairOSKeyValueError::Error),
            })?;
        Ok(())
    }

    pub async fn delete_kv_store(
        &self,
        username: &str,
        pod: &str,
        name: &str,
    ) -> Result<(), FairOSError> {
        let data = json!({
            "pod_name": pod,
            "table_name": name,
        })
        .to_string()
        .as_bytes()
        .to_vec();
        let cookie = self.cookie(username).unwrap();
        let _: MessageResponse = self
            .delete("/kv/delete", data, cookie)
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::KeyValue(FairOSKeyValueError::Error),
            })?;
        Ok(())
    }

    pub async fn list_kv_stores(
        &self,
        username: &str,
        pod: &str,
    ) -> Result<Vec<KeyValueStore>, FairOSError> {
        let mut query = HashMap::new();
        query.insert("pod_name", pod);
        let cookie = self.cookie(username).unwrap();
        let res: KvListResponse = self
            .get("/kv/ls", query, Some(cookie))
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::KeyValue(FairOSKeyValueError::Error),
            })?;
        let mut stores = res
            .tables
            .iter()
            .map(|table| KeyValueStore {
                name: table.table_name.clone(),
                indexes: table.indexes.clone(),
            })
            .collect::<Vec<KeyValueStore>>();
        stores.sort_by(|a, b| a.name.partial_cmp(&b.name).unwrap());
        Ok(stores)
    }

    pub async fn put_kv_pair<T: Serialize>(
        &self,
        username: &str,
        pod: &str,
        store: &str,
        key: &str,
        value: T,
    ) -> Result<(), FairOSError> {
        let data = json!({
            "pod_name": pod,
            "table_name": store,
            "key": key,
            "value": serde_json::to_string(&value).unwrap(),
        })
        .to_string()
        .as_bytes()
        .to_vec();
        let cookie = self.cookie(username).unwrap();
        let _ = self
            .post::<MessageResponse>("/kv/entry/put", data, Some(cookie))
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::KeyValue(FairOSKeyValueError::Error),
            })?;
        Ok(())
    }

    pub async fn get_kv_pair<T: DeserializeOwned>(
        &self,
        username: &str,
        pod: &str,
        store: &str,
        key: &str,
    ) -> Result<T, FairOSError> {
        let mut query = HashMap::new();
        query.insert("pod_name", pod);
        query.insert("table_name", store);
        query.insert("key", key);
        query.insert("format", "byte-string");
        let cookie = self.cookie(username).unwrap();
        let res: KvEntryGetResponse = self
            .get("/kv/entry/get", query, Some(cookie))
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::KeyValue(FairOSKeyValueError::Error),
            })?;
        Ok(serde_json::from_slice(&base64::decode(&res.values).unwrap()).unwrap())
    }

    pub async fn delete_kv_pair(
        &self,
        username: &str,
        pod: &str,
        store: &str,
        key: &str,
    ) -> Result<(), FairOSError> {
        let data = json!({
            "pod_name": pod,
            "table_name": store,
            "key": key,
        })
        .to_string()
        .as_bytes()
        .to_vec();
        let cookie = self.cookie(username).unwrap();
        let _: MessageResponse =
            self.delete("/kv/entry/del", data, cookie)
                .await
                .map_err(|err| match err {
                    RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                    RequestError::Message(_) => FairOSError::KeyValue(FairOSKeyValueError::Error),
                })?;
        Ok(())
    }

    pub async fn count_kv_pairs(
        &self,
        username: &str,
        pod: &str,
        store: &str,
    ) -> Result<u32, FairOSError> {
        let data = json!({
            "pod_name": pod,
            "table_name": store,
        })
        .to_string()
        .as_bytes()
        .to_vec();
        let cookie = self.cookie(username).unwrap();
        let (res, _) = self
            .post::<KvCountResponse>("/kv/count", data, Some(cookie))
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::KeyValue(FairOSKeyValueError::Error),
            })?;
        Ok(res.count)
    }

    pub async fn kv_pair_exists(
        &self,
        username: &str,
        pod: &str,
        store: &str,
        key: &str,
    ) -> Result<bool, FairOSError> {
        let mut query = HashMap::new();
        query.insert("pod_name", pod);
        query.insert("table_name", store);
        query.insert("key", key);
        let cookie = self.cookie(username).unwrap();
        let res: KvPresentResponse =
            self.get("/kv/present", query, Some(cookie))
                .await
                .map_err(|err| match err {
                    RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                    RequestError::Message(_) => FairOSError::KeyValue(FairOSKeyValueError::Error),
                })?;
        Ok(res.present)
    }

    pub async fn load_csv_buffer<R: Read>(
        &self,
        username: &str,
        pod: &str,
        store: &str,
        buffer: R,
        memory: bool,
    ) -> Result<(), FairOSError> {
        let mut multipart = Multipart::new();
        multipart.add_text("pod_name", pod);
        multipart.add_text("table_name", store);
        if memory {
            multipart.add_text("memory", store);
        }
        multipart.add_stream("csv", buffer, Some("data.csv"), Some(mime::TEXT_CSV));
        let mut prepared = multipart.prepare().unwrap();
        let boundary = prepared.boundary().to_string();
        let mut body = Vec::new();
        prepared.read_to_end(&mut body).unwrap();

        let cookie = self.cookie(username).unwrap();
        let _: MessageResponse = self
            .upload_multipart("/kv/loadcsv", body, boundary.as_str(), cookie, None)
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::KeyValue(FairOSKeyValueError::Error),
            })?;
        Ok(())
    }

    pub async fn load_csv_file<P: AsRef<Path>>(
        &self,
        username: &str,
        pod: &str,
        store: &str,
        local_path: P,
        memory: bool,
    ) -> Result<(), FairOSError> {
        let mut multipart = Multipart::new();
        multipart.add_text("pod_name", pod);
        multipart.add_text("table_name", store);
        if memory {
            multipart.add_text("memory", store);
        }
        multipart.add_file("csv", local_path.as_ref());
        let mut prepared = multipart.prepare().unwrap();
        let boundary = prepared.boundary().to_string();
        let mut body = Vec::new();
        prepared.read_to_end(&mut body).unwrap();

        let cookie = self.cookie(username).unwrap();
        let _: MessageResponse = self
            .upload_multipart("/kv/loadcsv", body, boundary.as_str(), cookie, None)
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::KeyValue(FairOSKeyValueError::Error),
            })?;
        Ok(())
    }

    pub(crate) async fn kv_seek(
        &self,
        username: &str,
        pod: &str,
        store: &str,
        start_key: &str,
        end_key: Option<&str>,
        limit: Option<u32>,
    ) -> Result<KeyValueSeek<'_>, FairOSError> {
        let data = json!({
            "pod_name": pod,
            "table_name": store,
            "start_prefix": start_key,
            "end_prefix": end_key,
            "limit": limit,
        })
        .to_string()
        .as_bytes()
        .to_vec();
        let cookie = self.cookie(username).unwrap();
        let _ = self
            .post::<MessageResponse>("/kv/seek", data, Some(cookie))
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::KeyValue(FairOSKeyValueError::Error),
            })?;
        Ok(KeyValueSeek {
            client: &self,
            username: username.into(),
            pod: pod.into(),
            store: store.into(),
            limit,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{Client, IndexType, KeyValueStore};
    use futures::StreamExt;
    use rand::{
        distributions::{Alphanumeric, Uniform},
        thread_rng, Rng,
    };
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Deserialize, Serialize)]
    struct TestData {
        string: String,
        number: u32,
    }

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
    async fn test_create_kv_store_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod = random_name();
        let res = fairos.create_pod(&username, &pod, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .create_kv_store(&username, &pod, "table", IndexType::Str)
            .await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_open_kv_store_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod = random_name();
        let res = fairos.create_pod(&username, &pod, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .create_kv_store(&username, &pod, "table", IndexType::Str)
            .await;
        assert!(res.is_ok());
        let res = fairos.open_kv_store(&username, &pod, "table").await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_delete_kv_store_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod = random_name();
        let res = fairos.create_pod(&username, &pod, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .create_kv_store(&username, &pod, "table", IndexType::Str)
            .await;
        assert!(res.is_ok());
        let res = fairos.delete_kv_store(&username, &pod, "table").await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_list_kv_stores_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod = random_name();
        let res = fairos.create_pod(&username, &pod, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .create_kv_store(&username, &pod, "table1", IndexType::Str)
            .await;
        assert!(res.is_ok());
        let res = fairos
            .create_kv_store(&username, &pod, "table2", IndexType::Number)
            .await;
        assert!(res.is_ok());
        let res = fairos.list_kv_stores(&username, &pod).await;
        assert!(res.is_ok());
        assert_eq!(
            res.unwrap(),
            vec![
                KeyValueStore {
                    name: "table1".into(),
                    indexes: vec!["StringIndex".into()],
                },
                KeyValueStore {
                    name: "table2".into(),
                    indexes: vec!["StringIndex".into()],
                },
            ]
        );
    }

    #[tokio::test]
    async fn test_put_kv_pair_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod = random_name();
        let res = fairos.create_pod(&username, &pod, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .create_kv_store(&username, &pod, "table", IndexType::Str)
            .await;
        assert!(res.is_ok());
        let res = fairos.open_kv_store(&username, &pod, "table").await;
        assert!(res.is_ok());
        let res = fairos
            .put_kv_pair(
                &username,
                &pod,
                "table",
                "key",
                TestData {
                    string: "str!ng".into(),
                    number: 45,
                },
            )
            .await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_get_kv_pair_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod = random_name();
        let res = fairos.create_pod(&username, &pod, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .create_kv_store(&username, &pod, "table", IndexType::Str)
            .await;
        assert!(res.is_ok());
        let res = fairos.open_kv_store(&username, &pod, "table").await;
        assert!(res.is_ok());
        let res = fairos
            .put_kv_pair(
                &username,
                &pod,
                "table",
                "key",
                TestData {
                    string: "str!ng".into(),
                    number: 45,
                },
            )
            .await;
        assert!(res.is_ok());
        let res = fairos
            .get_kv_pair::<TestData>(&username, &pod, "table", "key")
            .await;
        assert!(res.is_ok());
        assert_eq!(
            res.unwrap(),
            TestData {
                string: "str!ng".into(),
                number: 45,
            }
        );
    }

    #[tokio::test]
    async fn test_delete_kv_pair_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod = random_name();
        let res = fairos.create_pod(&username, &pod, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .create_kv_store(&username, &pod, "table", IndexType::Str)
            .await;
        assert!(res.is_ok());
        let res = fairos.open_kv_store(&username, &pod, "table").await;
        assert!(res.is_ok());
        let res = fairos
            .put_kv_pair(&username, &pod, "table", "key", "val")
            .await;
        assert!(res.is_ok());
        let res = fairos.delete_kv_pair(&username, &pod, "table", "key").await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_count_kv_pairs_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod = random_name();
        let res = fairos.create_pod(&username, &pod, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .create_kv_store(&username, &pod, "table", IndexType::Str)
            .await;
        assert!(res.is_ok());
        let res = fairos.open_kv_store(&username, &pod, "table").await;
        assert!(res.is_ok());
        let res = fairos
            .put_kv_pair(&username, &pod, "table", "key", "val")
            .await;
        assert!(res.is_ok());
        let res = fairos
            .put_kv_pair(&username, &pod, "table", "key2", 42)
            .await;
        assert!(res.is_ok());
        let res = fairos.count_kv_pairs(&username, &pod, "table").await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_kv_pair_exists_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod = random_name();
        let res = fairos.create_pod(&username, &pod, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .create_kv_store(&username, &pod, "table", IndexType::Str)
            .await;
        assert!(res.is_ok());
        let res = fairos.open_kv_store(&username, &pod, "table").await;
        assert!(res.is_ok());
        let res = fairos
            .put_kv_pair(&username, &pod, "table", "key", "val")
            .await;
        assert!(res.is_ok());
        let res = fairos.kv_pair_exists(&username, &pod, "table", "key").await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), true);
        let res = fairos
            .kv_pair_exists(&username, &pod, "table", "key2")
            .await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), false);
    }

    // #[tokio::test]
    // async fn test_load_csv_buffer_succeeds() {
    //     let mut fairos = Client::new();
    //     let username = random_name();
    //     let password = random_password();
    //     let res = fairos.signup(&username, &password, None).await;
    //     assert!(res.is_ok());
    //     let pod = random_name();
    //     let res = fairos.create_pod(&username, &pod, &password).await;
    //     assert!(res.is_ok());
    //     let res = fairos.create_kv_store(&username, &pod, "table", IndexType::Str).await;
    //     assert!(res.is_ok());
    //     let res = fairos.open_kv_store(&username, &pod, "table").await;
    //     assert!(res.is_ok());
    // }

    // #[tokio::test]
    // async fn test_load_csv_file_succeeds() {
    //     let mut fairos = Client::new();
    //     let username = random_name();
    //     let password = random_password();
    //     let res = fairos.signup(&username, &password, None).await;
    //     assert!(res.is_ok());
    //     let pod = random_name();
    //     let res = fairos.create_pod(&username, &pod, &password).await;
    //     assert!(res.is_ok());
    //     let res = fairos.create_kv_store(&username, &pod, "table", IndexType::Str).await;
    //     assert!(res.is_ok());
    //     let res = fairos.open_kv_store(&username, &pod, "table").await;
    //     assert!(res.is_ok());
    // }

    // #[tokio::test]
    // async fn test_kv_seek_succeeds() {
    //     let mut fairos = Client::new();
    //     let username = random_name();
    //     let password = random_password();
    //     let res = fairos.signup(&username, &password, None).await;
    //     assert!(res.is_ok());
    //     let pod = random_name();
    //     let res = fairos.create_pod(&username, &pod, &password).await;
    //     assert!(res.is_ok());
    //     let res = fairos
    //         .create_kv_store(&username, &pod, "table", IndexType::Str)
    //         .await;
    //     assert!(res.is_ok());
    //     let res = fairos.open_kv_store(&username, &pod, "table").await;
    //     assert!(res.is_ok());
    //     let res = fairos
    //         .put_kv_pair(&username, &pod, "table", "abc", "def")
    //         .await;
    //     assert!(res.is_ok());
    //     let res = fairos
    //         .put_kv_pair(&username, &pod, "table", "cde", "fgh")
    //         .await;
    //     assert!(res.is_ok());
    //     let res = fairos
    //         .put_kv_pair(&username, &pod, "table", "bcd", "efg")
    //         .await;
    //     assert!(res.is_ok());
    //     let res = fairos
    //         .put_kv_pair(&username, &pod, "table", "def", "ghi")
    //         .await;
    //     assert!(res.is_ok());
    //     let res = fairos
    //         .kv_seek(&username, &pod, "table", "bcd", None, None)
    //         .await;
    //     assert!(res.is_ok());
    //     let pairs = res.unwrap().collect::<Vec<(String, String)>>().await;
    //     assert_eq!(
    //         pairs,
    //         vec![
    //             ("bcd".into(), "efg".into()),
    //             ("cde".into(), "fgh".into()),
    //             ("def".into(), "ghi".into()),
    //         ]
    //     );
    // }
}
