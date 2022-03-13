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
pub struct KeyValueTable {
    pub name: String,
    pub indexes: Vec<String>,
}

pub struct KeyValueSeek<'a> {
    client: &'a Client,
    username: String,
    pod_name: String,
    table_name: String,
    limit: Option<u32>,
}

impl Stream for KeyValueSeek<'_> {
    type Item = (String, String);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut query = HashMap::new();
        query.insert("pod_name", self.pod_name.as_str());
        query.insert("table_name", self.table_name.as_str());
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
    pub async fn kv_create_table(
        &self,
        username: &str,
        pod_name: &str,
        name: &str,
        index_type: IndexType,
    ) -> Result<(), FairOSError> {
        let index_type = match index_type {
            IndexType::Str => "string",
            IndexType::Number => "number",
        };
        let data = json!({
            "pod_name": pod_name,
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

    pub async fn kv_open_table(
        &self,
        username: &str,
        pod_name: &str,
        name: &str,
    ) -> Result<(), FairOSError> {
        let data = json!({
            "pod_name": pod_name,
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

    pub async fn kv_count_entries(
        &self,
        username: &str,
        pod_name: &str,
        name: &str,
    ) -> Result<u32, FairOSError> {
        let data = json!({
            "pod_name": pod_name,
            "table_name": name,
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

    pub async fn kv_delete_table(
        &self,
        username: &str,
        pod_name: &str,
        name: &str,
    ) -> Result<(), FairOSError> {
        let data = json!({
            "pod_name": pod_name,
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

    pub async fn kv_list_tables(
        &self,
        username: &str,
        pod_name: &str,
    ) -> Result<Vec<KeyValueTable>, FairOSError> {
        let mut query = HashMap::new();
        query.insert("pod_name", pod_name);
        let cookie = self.cookie(username).unwrap();
        let res: KvListResponse = self
            .get("/kv/ls", query, Some(cookie))
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::KeyValue(FairOSKeyValueError::Error),
            })?;
        let tables = res
            .tables
            .iter()
            .map(|table| KeyValueTable {
                name: table.table_name.clone(),
                indexes: table.indexes.clone(),
            })
            .collect();
        Ok(tables)
    }

    pub async fn kv_put_entry<T: Serialize>(
        &self,
        username: &str,
        pod_name: &str,
        table_name: &str,
        key: &str,
        value: T,
    ) -> Result<(), FairOSError> {
        let data = json!({
            "pod_name": pod_name,
            "table_name": table_name,
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

    pub async fn kv_get_entry<T: DeserializeOwned>(
        &self,
        username: &str,
        pod_name: &str,
        table_name: &str,
        key: &str,
    ) -> Result<T, FairOSError> {
        let mut query = HashMap::new();
        query.insert("pod_name", pod_name);
        query.insert("table_name", table_name);
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

    pub async fn kv_delete_entry(
        &self,
        username: &str,
        pod_name: &str,
        table_name: &str,
        key: &str,
    ) -> Result<(), FairOSError> {
        let data = json!({
            "pod_name": pod_name,
            "table_name": table_name,
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

    pub async fn kv_entry_exists(
        &self,
        username: &str,
        pod_name: &str,
        table_name: &str,
        key: &str,
    ) -> Result<bool, FairOSError> {
        let mut query = HashMap::new();
        query.insert("pod_name", pod_name);
        query.insert("table_name", table_name);
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

    pub async fn kv_load_csv_buffer<R: Read>(
        &self,
        username: &str,
        pod_name: &str,
        table_name: &str,
        buffer: R,
        memory: bool,
    ) -> Result<(), FairOSError> {
        let mut multipart = Multipart::new();
        multipart.add_text("pod_name", pod_name);
        multipart.add_text("table_name", table_name);
        if memory {
            multipart.add_text("memory", table_name);
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

    pub async fn kv_load_csv_file<P: AsRef<Path>>(
        &self,
        username: &str,
        pod_name: &str,
        table_name: &str,
        local_path: P,
        memory: bool,
    ) -> Result<(), FairOSError> {
        let mut multipart = Multipart::new();
        multipart.add_text("pod_name", pod_name);
        multipart.add_text("table_name", table_name);
        if memory {
            multipart.add_text("memory", table_name);
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

    pub async fn kv_seek(
        &self,
        username: &str,
        pod_name: &str,
        table_name: &str,
        start_key: &str,
        end_key: Option<&str>,
        limit: Option<u32>,
    ) -> Result<KeyValueSeek<'_>, FairOSError> {
        let data = json!({
            "pod_name": pod_name,
            "table_name": table_name,
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
            pod_name: pod_name.into(),
            table_name: table_name.into(),
            limit,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{Client, IndexType, KeyValueTable};
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
    async fn test_kv_create_table_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod_name = random_name();
        let res = fairos.create_pod(&username, &pod_name, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .kv_create_table(&username, &pod_name, "table", IndexType::Str)
            .await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_kv_open_table_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod_name = random_name();
        let res = fairos.create_pod(&username, &pod_name, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .kv_create_table(&username, &pod_name, "table", IndexType::Str)
            .await;
        assert!(res.is_ok());
        let res = fairos.kv_open_table(&username, &pod_name, "table").await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_kv_count_entries_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod_name = random_name();
        let res = fairos.create_pod(&username, &pod_name, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .kv_create_table(&username, &pod_name, "table", IndexType::Str)
            .await;
        assert!(res.is_ok());
        let res = fairos.kv_open_table(&username, &pod_name, "table").await;
        assert!(res.is_ok());
        let res = fairos
            .kv_put_entry(&username, &pod_name, "table", "key", "val")
            .await;
        assert!(res.is_ok());
        let res = fairos
            .kv_put_entry(&username, &pod_name, "table", "key2", 42)
            .await;
        assert!(res.is_ok());
        let res = fairos.kv_count_entries(&username, &pod_name, "table").await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_kv_delete_table_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod_name = random_name();
        let res = fairos.create_pod(&username, &pod_name, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .kv_create_table(&username, &pod_name, "table", IndexType::Str)
            .await;
        assert!(res.is_ok());
        let res = fairos.kv_delete_table(&username, &pod_name, "table").await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_kv_list_tables_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod_name = random_name();
        let res = fairos.create_pod(&username, &pod_name, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .kv_create_table(&username, &pod_name, "table1", IndexType::Str)
            .await;
        assert!(res.is_ok());
        let res = fairos
            .kv_create_table(&username, &pod_name, "table2", IndexType::Number)
            .await;
        assert!(res.is_ok());
        let res = fairos.kv_list_tables(&username, &pod_name).await;
        assert!(res.is_ok());
        let mut tables = res.unwrap();
        tables.sort_by(|a, b| a.name.partial_cmp(&b.name).unwrap());
        assert_eq!(
            tables,
            vec![
                KeyValueTable {
                    name: "table1".into(),
                    indexes: vec!["StringIndex".into()],
                },
                KeyValueTable {
                    name: "table2".into(),
                    indexes: vec!["StringIndex".into()],
                },
            ]
        );
    }

    #[tokio::test]
    async fn test_kv_put_entry_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod_name = random_name();
        let res = fairos.create_pod(&username, &pod_name, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .kv_create_table(&username, &pod_name, "table", IndexType::Str)
            .await;
        assert!(res.is_ok());
        let res = fairos.kv_open_table(&username, &pod_name, "table").await;
        assert!(res.is_ok());
        let res = fairos
            .kv_put_entry(
                &username,
                &pod_name,
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
    async fn test_kv_get_entry_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod_name = random_name();
        let res = fairos.create_pod(&username, &pod_name, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .kv_create_table(&username, &pod_name, "table", IndexType::Str)
            .await;
        assert!(res.is_ok());
        let res = fairos.kv_open_table(&username, &pod_name, "table").await;
        assert!(res.is_ok());
        let res = fairos
            .kv_put_entry(
                &username,
                &pod_name,
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
            .kv_get_entry::<TestData>(&username, &pod_name, "table", "key")
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
    async fn test_kv_delete_entry_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod_name = random_name();
        let res = fairos.create_pod(&username, &pod_name, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .kv_create_table(&username, &pod_name, "table", IndexType::Str)
            .await;
        assert!(res.is_ok());
        let res = fairos.kv_open_table(&username, &pod_name, "table").await;
        assert!(res.is_ok());
        let res = fairos
            .kv_put_entry(&username, &pod_name, "table", "key", "val")
            .await;
        assert!(res.is_ok());
        let res = fairos
            .kv_delete_entry(&username, &pod_name, "table", "key")
            .await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_kv_entry_exists_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod_name = random_name();
        let res = fairos.create_pod(&username, &pod_name, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .kv_create_table(&username, &pod_name, "table", IndexType::Str)
            .await;
        assert!(res.is_ok());
        let res = fairos.kv_open_table(&username, &pod_name, "table").await;
        assert!(res.is_ok());
        let res = fairos
            .kv_put_entry(&username, &pod_name, "table", "key", "val")
            .await;
        assert!(res.is_ok());
        let res = fairos
            .kv_entry_exists(&username, &pod_name, "table", "key")
            .await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), true);
        let res = fairos
            .kv_entry_exists(&username, &pod_name, "table", "key2")
            .await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), false);
    }

    // #[tokio::test]
    // async fn test_kv_load_csv_buffer_succeeds() {
    //     let mut fairos = Client::new();
    //     let username = random_name();
    //     let password = random_password();
    //     let res = fairos.signup(&username, &password, None).await;
    //     assert!(res.is_ok());
    //     let pod_name = random_name();
    //     let res = fairos.create_pod(&username, &pod_name, &password).await;
    //     assert!(res.is_ok());
    //     let res = fairos.kv_create_table(&username, &pod_name, "table", IndexType::Str).await;
    //     assert!(res.is_ok());
    //     let res = fairos.kv_open_table(&username, &pod_name, "table").await;
    //     assert!(res.is_ok());
    // }

    // #[tokio::test]
    // async fn test_kv_load_csv_file_succeeds() {
    //     let mut fairos = Client::new();
    //     let username = random_name();
    //     let password = random_password();
    //     let res = fairos.signup(&username, &password, None).await;
    //     assert!(res.is_ok());
    //     let pod_name = random_name();
    //     let res = fairos.create_pod(&username, &pod_name, &password).await;
    //     assert!(res.is_ok());
    //     let res = fairos.kv_create_table(&username, &pod_name, "table", IndexType::Str).await;
    //     assert!(res.is_ok());
    //     let res = fairos.kv_open_table(&username, &pod_name, "table").await;
    //     assert!(res.is_ok());
    // }

    // #[tokio::test]
    // async fn test_kv_seek_succeeds() {
    //     let mut fairos = Client::new();
    //     let username = random_name();
    //     let password = random_password();
    //     let res = fairos.signup(&username, &password, None).await;
    //     assert!(res.is_ok());
    //     let pod_name = random_name();
    //     let res = fairos.create_pod(&username, &pod_name, &password).await;
    //     assert!(res.is_ok());
    //     let res = fairos
    //         .kv_create_table(&username, &pod_name, "table", IndexType::Str)
    //         .await;
    //     assert!(res.is_ok());
    //     let res = fairos.kv_open_table(&username, &pod_name, "table").await;
    //     assert!(res.is_ok());
    //     let res = fairos
    //         .kv_put_entry(&username, &pod_name, "table", "abc", "def")
    //         .await;
    //     assert!(res.is_ok());
    //     let res = fairos
    //         .kv_put_entry(&username, &pod_name, "table", "cde", "fgh")
    //         .await;
    //     assert!(res.is_ok());
    //     let res = fairos
    //         .kv_put_entry(&username, &pod_name, "table", "bcd", "efg")
    //         .await;
    //     assert!(res.is_ok());
    //     let res = fairos
    //         .kv_put_entry(&username, &pod_name, "table", "def", "ghi")
    //         .await;
    //     assert!(res.is_ok());
    //     let res = fairos
    //         .kv_seek(&username, &pod_name, "table", "bcd", None, None)
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
