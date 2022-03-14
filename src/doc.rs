use crate::{
    client::{MessageResponse, RequestError},
    error::{FairOSDocumentError, FairOSError},
    Client,
};

use std::{collections::HashMap, io::Read, path::Path};

use multipart::client::lazy::Multipart;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
struct DocListPropertyResponse {
    name: String,
    r#type: u32,
}

#[derive(Debug, Deserialize)]
struct DocListTableResponse {
    table_name: String,
    indexes: Vec<DocListPropertyResponse>,
}

#[derive(Debug, Deserialize)]
struct DocListResponse {
    #[serde(rename = "Tables")]
    tables: Vec<DocListTableResponse>,
}

#[derive(Debug, Deserialize)]
struct DocEntryGetResponse {
    doc: String,
}

#[derive(Debug, Deserialize)]
struct DocFindResponse {
    docs: Vec<String>,
}

#[derive(Debug, PartialEq)]
pub enum FieldType {
    Str,
    Number,
    Map,
}

#[derive(Debug, PartialEq)]
pub struct DocumentTable {
    name: String,
    fields: Vec<(String, FieldType)>,
}

impl Client {
    pub async fn doc_create_table(
        &self,
        username: &str,
        pod_name: &str,
        name: &str,
        fields: Vec<(&str, FieldType)>,
        mutable: bool,
    ) -> Result<(), FairOSError> {
        let si = fields
            .iter()
            .map(|(field, field_type)| {
                format!(
                    "{}={}",
                    field,
                    match field_type {
                        FieldType::Str => "string",
                        FieldType::Number => "number",
                        FieldType::Map => "map",
                    }
                )
            })
            .collect::<Vec<String>>()
            .join(",");
        let data = json!({
            "pod_name": pod_name,
            "table_name": name,
            "si": si,
            "mutable": mutable,
        })
        .to_string()
        .as_bytes()
        .to_vec();
        let cookie = self.cookie(username).unwrap();
        let _ = self
            .post::<MessageResponse>("/doc/new", data, Some(cookie))
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::Document(FairOSDocumentError::Error),
            })?;
        Ok(())
    }

    pub async fn doc_open_table(
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
            .post::<MessageResponse>("/doc/open", data, Some(cookie))
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::Document(FairOSDocumentError::Error),
            })?;
        Ok(())
    }

    pub async fn doc_delete_table(
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
            .delete("/doc/delete", data, cookie)
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::Document(FairOSDocumentError::Error),
            })?;
        Ok(())
    }

    pub async fn doc_list_tables(
        &self,
        username: &str,
        pod_name: &str,
    ) -> Result<Vec<DocumentTable>, FairOSError> {
        let mut query = HashMap::new();
        query.insert("pod_name", pod_name);
        let cookie = self.cookie(username).unwrap();
        let res: DocListResponse =
            self.get("/doc/ls", query, Some(cookie))
                .await
                .map_err(|err| match err {
                    RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                    RequestError::Message(_) => FairOSError::Document(FairOSDocumentError::Error),
                })?;
        let mut tables = res
            .tables
            .iter()
            .map(|table| {
                let mut fields = table
                    .indexes
                    .iter()
                    .map(|prop| {
                        (
                            prop.name.clone(),
                            match prop.r#type {
                                2 => FieldType::Str,
                                3 => FieldType::Number,
                                4 => FieldType::Map,
                                _ => unreachable!(),
                            },
                        )
                    })
                    .collect::<Vec<(String, FieldType)>>();
                fields.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
                DocumentTable {
                    name: table.table_name.clone(),
                    fields,
                }
            })
            .collect::<Vec<DocumentTable>>();
        tables.sort_by(|a, b| a.name.partial_cmp(&b.name).unwrap());
        Ok(tables)
    }

    pub async fn doc_put_document<T: Serialize>(
        &self,
        username: &str,
        pod_name: &str,
        table_name: &str,
        doc: T,
    ) -> Result<String, FairOSError> {
        let id = Uuid::new_v4().to_string();
        let mut doc = json!(doc);
        doc["id"] = json!(&id);
        let data = json!({
            "pod_name": pod_name,
            "table_name": table_name,
            "doc": serde_json::to_string(&doc).unwrap(),
        })
        .to_string()
        .as_bytes()
        .to_vec();
        let cookie = self.cookie(username).unwrap();
        let _ = self
            .post::<MessageResponse>("/doc/entry/put", data, Some(cookie))
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::Document(FairOSDocumentError::Error),
            })?;
        Ok(id)
    }

    pub async fn doc_get_document<T: DeserializeOwned>(
        &self,
        username: &str,
        pod_name: &str,
        table_name: &str,
        id: &str,
    ) -> Result<T, FairOSError> {
        let mut query = HashMap::new();
        query.insert("pod_name", pod_name);
        query.insert("table_name", table_name);
        query.insert("id", id);
        let cookie = self.cookie(username).unwrap();
        let res: DocEntryGetResponse = self
            .get("/doc/entry/get", query, Some(cookie))
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::Document(FairOSDocumentError::Error),
            })?;
        Ok(serde_json::from_slice(&base64::decode(&res.doc).unwrap()).unwrap())
    }

    pub async fn doc_find_documents<T: DeserializeOwned>(
        &self,
        username: &str,
        pod_name: &str,
        table_name: &str,
        expression: &str,
        limit: Option<u32>,
    ) -> Result<Vec<T>, FairOSError> {
        let mut query = HashMap::new();
        query.insert("pod_name", pod_name);
        query.insert("table_name", table_name);
        query.insert("expr", expression);
        let limit = limit.map(|limit| limit.to_string()).unwrap_or("".into());
        if !limit.is_empty() {
            query.insert("limit", limit.as_str());
        }
        let cookie = self.cookie(username).unwrap();
        let res: DocFindResponse =
            self.get("/doc/find", query, Some(cookie))
                .await
                .map_err(|err| match err {
                    RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                    RequestError::Message(_) => FairOSError::Document(FairOSDocumentError::Error),
                })?;
        let docs = res
            .docs
            .iter()
            .map(|doc| serde_json::from_slice(&base64::decode(&doc).unwrap()).unwrap())
            .collect();
        Ok(docs)
    }

    pub async fn doc_count_documents(
        &self,
        username: &str,
        pod_name: &str,
        table_name: &str,
        expression: Option<&str>,
    ) -> Result<u32, FairOSError> {
        let data = json!({
            "pod_name": pod_name,
            "table_name": table_name,
            "expr": expression,
        })
        .to_string()
        .as_bytes()
        .to_vec();
        let cookie = self.cookie(username).unwrap();
        let (res, _) = self
            .post::<MessageResponse>("/doc/count", data, Some(cookie))
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::Document(FairOSDocumentError::Error),
            })?;
        Ok(res.message.parse().unwrap())
    }

    pub async fn doc_delete_document(
        &self,
        username: &str,
        pod_name: &str,
        table_name: &str,
        id: &str,
    ) -> Result<(), FairOSError> {
        let data = json!({
            "pod_name": pod_name,
            "table_name": table_name,
            "id": id,
        })
        .to_string()
        .as_bytes()
        .to_vec();
        let cookie = self.cookie(username).unwrap();
        let _: MessageResponse =
            self.delete("/doc/entry/del", data, cookie)
                .await
                .map_err(|err| match err {
                    RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                    RequestError::Message(_) => FairOSError::Document(FairOSDocumentError::Error),
                })?;
        Ok(())
    }

    pub async fn doc_load_json_buffer<R: Read>(
        &self,
        username: &str,
        pod_name: &str,
        table_name: &str,
        buffer: R,
    ) -> Result<(), FairOSError> {
        let mut multipart = Multipart::new();
        multipart.add_text("pod_name", pod_name);
        multipart.add_text("table_name", table_name);
        multipart.add_stream(
            "json",
            buffer,
            Some("data.json"),
            Some(mime::APPLICATION_JSON),
        );
        let mut prepared = multipart.prepare().unwrap();
        let boundary = prepared.boundary().to_string();
        let mut body = Vec::new();
        prepared.read_to_end(&mut body).unwrap();

        let cookie = self.cookie(username).unwrap();
        let _: MessageResponse = self
            .upload_multipart("/doc/loadjson", body, boundary.as_str(), cookie, None)
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::Document(FairOSDocumentError::Error),
            })?;
        Ok(())
    }

    pub async fn doc_load_json_file<P: AsRef<Path>>(
        &self,
        username: &str,
        pod_name: &str,
        table_name: &str,
        local_path: P,
    ) -> Result<(), FairOSError> {
        let mut multipart = Multipart::new();
        multipart.add_text("pod_name", pod_name);
        multipart.add_text("table_name", table_name);
        multipart.add_file("json", local_path.as_ref());
        let mut prepared = multipart.prepare().unwrap();
        let boundary = prepared.boundary().to_string();
        let mut body = Vec::new();
        prepared.read_to_end(&mut body).unwrap();

        let cookie = self.cookie(username).unwrap();
        let _: MessageResponse = self
            .upload_multipart("/doc/loadjson", body, boundary.as_str(), cookie, None)
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::Document(FairOSDocumentError::Error),
            })?;
        Ok(())
    }

    pub async fn doc_index_json(
        &self,
        username: &str,
        pod_name: &str,
        table_name: &str,
        table_file: &str,
    ) -> Result<(), FairOSError> {
        let data = json!({
            "pod_name": pod_name,
            "table_name": table_name,
            "file_name": table_file,
        })
        .to_string()
        .as_bytes()
        .to_vec();
        let cookie = self.cookie(username).unwrap();
        let _ = self
            .post::<MessageResponse>("/doc/indexjson", data, Some(cookie))
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::Document(FairOSDocumentError::Error),
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{Client, DocumentTable, FieldType};
    use futures::StreamExt;
    use rand::{
        distributions::{Alphanumeric, Uniform},
        thread_rng, Rng,
    };
    use serde::{Deserialize, Serialize};
    use std::fs;

    #[derive(Debug, PartialEq, Deserialize, Serialize)]
    struct TestData {
        s: String,
        n: u32,
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
    async fn test_doc_create_table_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod_name = random_name();
        let res = fairos.create_pod(&username, &pod_name, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .doc_create_table(
                &username,
                &pod_name,
                "table",
                vec![("s", FieldType::Str), ("n", FieldType::Number)],
                true,
            )
            .await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_doc_open_table_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod_name = random_name();
        let res = fairos.create_pod(&username, &pod_name, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .doc_create_table(
                &username,
                &pod_name,
                "table",
                vec![("s", FieldType::Str), ("n", FieldType::Number)],
                true,
            )
            .await;
        assert!(res.is_ok());
        let res = fairos.doc_open_table(&username, &pod_name, "table").await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_doc_delete_table_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod_name = random_name();
        let res = fairos.create_pod(&username, &pod_name, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .doc_create_table(
                &username,
                &pod_name,
                "table",
                vec![("s", FieldType::Str), ("n", FieldType::Number)],
                true,
            )
            .await;
        assert!(res.is_ok());
        let res = fairos.doc_delete_table(&username, &pod_name, "table").await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_doc_list_tables_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod_name = random_name();
        let res = fairos.create_pod(&username, &pod_name, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .doc_create_table(
                &username,
                &pod_name,
                "table1",
                vec![("s1", FieldType::Str), ("n2", FieldType::Number)],
                true,
            )
            .await;
        assert!(res.is_ok());
        let res = fairos
            .doc_create_table(
                &username,
                &pod_name,
                "table2",
                vec![("m", FieldType::Map)],
                true,
            )
            .await;
        assert!(res.is_ok());
        let res = fairos
            .doc_create_table(
                &username,
                &pod_name,
                "table3",
                vec![("s2", FieldType::Str), ("n1", FieldType::Number)],
                true,
            )
            .await;
        assert!(res.is_ok());
        let res = fairos.doc_list_tables(&username, &pod_name).await;
        assert!(res.is_ok());
        assert_eq!(
            res.unwrap(),
            vec![
                DocumentTable {
                    name: "table1".into(),
                    fields: vec![
                        ("id".into(), FieldType::Str),
                        ("n2".into(), FieldType::Number),
                        ("s1".into(), FieldType::Str),
                    ],
                },
                DocumentTable {
                    name: "table2".into(),
                    fields: vec![("id".into(), FieldType::Str), ("m".into(), FieldType::Map)],
                },
                DocumentTable {
                    name: "table3".into(),
                    fields: vec![
                        ("id".into(), FieldType::Str),
                        ("n1".into(), FieldType::Number),
                        ("s2".into(), FieldType::Str),
                    ],
                },
            ]
        );
    }

    #[tokio::test]
    async fn test_doc_put_document_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod_name = random_name();
        let res = fairos.create_pod(&username, &pod_name, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .doc_create_table(
                &username,
                &pod_name,
                "table",
                vec![("s", FieldType::Str), ("n", FieldType::Number)],
                true,
            )
            .await;
        assert!(res.is_ok());
        let res = fairos.doc_open_table(&username, &pod_name, "table").await;
        assert!(res.is_ok());
        let res = fairos
            .doc_put_document(
                &username,
                &pod_name,
                "table",
                TestData {
                    s: "text".into(),
                    n: 12,
                },
            )
            .await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_doc_get_document_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod_name = random_name();
        let res = fairos.create_pod(&username, &pod_name, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .doc_create_table(
                &username,
                &pod_name,
                "table",
                vec![("s", FieldType::Str), ("n", FieldType::Number)],
                true,
            )
            .await;
        assert!(res.is_ok());
        let res = fairos.doc_open_table(&username, &pod_name, "table").await;
        assert!(res.is_ok());
        let res = fairos
            .doc_put_document(
                &username,
                &pod_name,
                "table",
                TestData {
                    s: "text".into(),
                    n: 12,
                },
            )
            .await;
        assert!(res.is_ok());
        let id = res.unwrap();
        let res = fairos
            .doc_get_document::<TestData>(&username, &pod_name, "table", &id)
            .await;
        assert!(res.is_ok());
        assert_eq!(
            res.unwrap(),
            TestData {
                s: "text".into(),
                n: 12
            }
        );
    }

    #[tokio::test]
    async fn test_doc_find_documents_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod_name = random_name();
        let res = fairos.create_pod(&username, &pod_name, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .doc_create_table(
                &username,
                &pod_name,
                "table",
                vec![("s", FieldType::Str), ("n", FieldType::Number)],
                true,
            )
            .await;
        assert!(res.is_ok());
        let res = fairos.doc_open_table(&username, &pod_name, "table").await;
        assert!(res.is_ok());
        let res = fairos
            .doc_put_document(
                &username,
                &pod_name,
                "table",
                TestData {
                    s: "a".into(),
                    n: 8,
                },
            )
            .await;
        assert!(res.is_ok());
        let res = fairos
            .doc_put_document(
                &username,
                &pod_name,
                "table",
                TestData {
                    s: "a".into(),
                    n: 10,
                },
            )
            .await;
        assert!(res.is_ok());
        let res = fairos
            .doc_put_document(
                &username,
                &pod_name,
                "table",
                TestData {
                    s: "b".into(),
                    n: 12,
                },
            )
            .await;
        assert!(res.is_ok());
        let id = res.unwrap();
        let res = fairos
            .doc_find_documents::<TestData>(&username, &pod_name, "table", "n%3e9", None)
            .await;
        assert!(res.is_ok());
        assert_eq!(
            res.unwrap(),
            vec![
                TestData {
                    s: "a".into(),
                    n: 10
                },
                TestData {
                    s: "b".into(),
                    n: 12
                }
            ]
        );
        let res = fairos
            .doc_find_documents::<TestData>(&username, &pod_name, "table", "s=%22a%22", None)
            .await;
        assert!(res.is_ok());
        assert_eq!(
            res.unwrap(),
            vec![
                TestData {
                    s: "a".into(),
                    n: 8
                },
                TestData {
                    s: "a".into(),
                    n: 10
                }
            ]
        );
    }

    #[tokio::test]
    async fn test_doc_count_documents_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod_name = random_name();
        let res = fairos.create_pod(&username, &pod_name, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .doc_create_table(
                &username,
                &pod_name,
                "table",
                vec![("s", FieldType::Str), ("n", FieldType::Number)],
                true,
            )
            .await;
        assert!(res.is_ok());
        let res = fairos.doc_open_table(&username, &pod_name, "table").await;
        assert!(res.is_ok());
        let res = fairos
            .doc_put_document(
                &username,
                &pod_name,
                "table",
                TestData {
                    s: "text".into(),
                    n: 12,
                },
            )
            .await;
        assert!(res.is_ok());
        let res = fairos
            .doc_put_document(
                &username,
                &pod_name,
                "table",
                TestData {
                    s: "text".into(),
                    n: 10,
                },
            )
            .await;
        assert!(res.is_ok());
        let res = fairos
            .doc_count_documents(&username, &pod_name, "table", None)
            .await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_doc_delete_document_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod_name = random_name();
        let res = fairos.create_pod(&username, &pod_name, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .doc_create_table(
                &username,
                &pod_name,
                "table",
                vec![("s", FieldType::Str), ("n", FieldType::Number)],
                true,
            )
            .await;
        assert!(res.is_ok());
        let res = fairos.doc_open_table(&username, &pod_name, "table").await;
        assert!(res.is_ok());
        let res = fairos
            .doc_put_document(
                &username,
                &pod_name,
                "table",
                TestData {
                    s: "text".into(),
                    n: 12,
                },
            )
            .await;
        assert!(res.is_ok());
        let id = res.unwrap();
        let res = fairos
            .doc_delete_document(&username, &pod_name, "table", &id)
            .await;
        assert!(res.is_ok());
        let res = fairos
            .doc_get_document::<TestData>(&username, &pod_name, "table", &id)
            .await;
        assert!(res.is_err());
    }

    // #[tokio::test]
    // async fn test_doc_load_json_buffer_succeeds() {
    //     let mut fairos = Client::new();
    //     let username = random_name();
    //     let password = random_password();
    //     let res = fairos.signup(&username, &password, None).await;
    //     assert!(res.is_ok());
    //     let pod_name = random_name();
    //     let res = fairos.create_pod(&username, &pod_name, &password).await;
    //     assert!(res.is_ok());
    //     let res = fairos
    //         .doc_create_table(
    //             &username,
    //             &pod_name,
    //             "table",
    //             vec![("s", FieldType::Str), ("n", FieldType::Number)],
    //             true,
    //         )
    //         .await;
    //     assert!(res.is_ok());
    //     let res = fairos.doc_open_table(&username, &pod_name, "table").await;
    //     assert!(res.is_ok());
    //     let res = fairos.doc_load_json_buffer(&username, &pod_name, "table", "[{\"s\": \"text\", \"n\": 12}, {\"s\": \"text\", \"n\": 10}]".as_bytes()).await;
    //     assert!(res.is_ok());
    //     let res = fairos.doc_count_documents(&username, &pod_name, "table", None).await;
    //     assert!(res.is_ok());
    //     assert_eq!(res.unwrap(), 2);
    // }

    // #[tokio::test]
    // async fn test_doc_load_json_file_succeeds() {
    //     let mut fairos = Client::new();
    //     let username = random_name();
    //     let password = random_password();
    //     let res = fairos.signup(&username, &password, None).await;
    //     assert!(res.is_ok());
    //     let pod_name = random_name();
    //     let res = fairos.create_pod(&username, &pod_name, &password).await;
    //     assert!(res.is_ok());
    //     let res = fairos
    //         .doc_create_table(
    //             &username,
    //             &pod_name,
    //             "table",
    //             vec![("s", FieldType::Str), ("n", FieldType::Number)],
    //             true,
    //         )
    //         .await;
    //     assert!(res.is_ok());
    //     let res = fairos.doc_open_table(&username, &pod_name, "table").await;
    //     assert!(res.is_ok());
    //     fs::write("data.json", "[{\"s\": \"text\", \"n\": 12}, {\"s\": \"text\", \"n\": 10}]").unwrap();
    //     let res = fairos.doc_load_json_file(&username, &pod_name, "table", "data.json").await;
    //     assert!(res.is_ok());
    //     fs::remove_file("data.json").unwrap();
    //     let res = fairos.doc_count_documents(&username, &pod_name, "table", None).await;
    //     assert!(res.is_ok());
    //     assert_eq!(res.unwrap(), 2);
    // }

    // #[tokio::test]
    // async fn test_doc_index_json_succeeds() {
    //     let mut fairos = Client::new();
    //     let username = random_name();
    //     let password = random_password();
    //     let res = fairos.signup(&username, &password, None).await;
    //     assert!(res.is_ok());
    //     let pod_name = random_name();
    //     let res = fairos.create_pod(&username, &pod_name, &password).await;
    //     assert!(res.is_ok());
    //     let res = fairos
    //         .doc_create_table(
    //             &username,
    //             &pod_name,
    //             "table",
    //             vec![("s", FieldType::Str), ("n", FieldType::Number)],
    //             true,
    //         )
    //         .await;
    //     assert!(res.is_ok());
    //     let res = fairos.doc_open_table(&username, &pod_name, "table").await;
    //     assert!(res.is_ok());
    //     let res = fairos.doc_index_json(&username, &pod_name, "table", "table.json").await;
    //     assert!(res.is_ok());
    // }
}
