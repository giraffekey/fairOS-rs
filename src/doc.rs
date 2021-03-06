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
pub struct DocumentDatabase {
    name: String,
    fields: Vec<(String, FieldType)>,
}

#[derive(Debug)]
pub enum ExprValue {
    Str(String),
    Number(u32),
    Map,
}

impl ToString for ExprValue {
    fn to_string(&self) -> String {
        match self {
            ExprValue::Str(s) => format!("%22{}%22", s),
            ExprValue::Number(n) => n.to_string(),
            ExprValue::Map => unimplemented!(),
        }
    }
}

#[derive(Debug)]
pub enum Expr {
    All,
    Eq(String, ExprValue),
    Gt(String, ExprValue),
    Gte(String, ExprValue),
    Lt(String, ExprValue),
    Lte(String, ExprValue),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
}

impl ToString for Expr {
    fn to_string(&self) -> String {
        match self {
            Expr::All => "".into(),
            Expr::Eq(field, value) => format!("{}={}", field, value.to_string()),
            Expr::Gt(field, value) => format!("{}%3e{}", field, value.to_string()),
            Expr::Gte(field, value) => format!("{}%3e={}", field, value.to_string()),
            Expr::Lt(field, value) => format!("{}%3e{}", value.to_string(), field),
            Expr::Lte(field, value) => format!("{}%3e={}", value.to_string(), field),
            Expr::And(_a, _b) => unimplemented!(),
            Expr::Or(_a, _b) => unimplemented!(),
        }
    }
}

impl Client {
    pub async fn create_doc_database(
        &self,
        username: &str,
        pod: &str,
        name: &str,
        fields: &[(&str, FieldType)],
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
            "pod_name": pod,
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

    pub async fn open_doc_database(
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
            .post::<MessageResponse>("/doc/open", data, Some(cookie))
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::Document(FairOSDocumentError::Error),
            })?;
        Ok(())
    }

    pub async fn delete_doc_database(
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
            .delete("/doc/delete", data, cookie)
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::Document(FairOSDocumentError::Error),
            })?;
        Ok(())
    }

    pub async fn list_doc_databases(
        &self,
        username: &str,
        pod: &str,
    ) -> Result<Vec<DocumentDatabase>, FairOSError> {
        let mut query = HashMap::new();
        query.insert("pod_name", pod);
        let cookie = self.cookie(username).unwrap();
        let res: DocListResponse =
            self.get("/doc/ls", query, Some(cookie))
                .await
                .map_err(|err| match err {
                    RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                    RequestError::Message(_) => FairOSError::Document(FairOSDocumentError::Error),
                })?;
        let mut databases = res
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
                DocumentDatabase {
                    name: table.table_name.clone(),
                    fields,
                }
            })
            .collect::<Vec<DocumentDatabase>>();
        databases.sort_by(|a, b| a.name.partial_cmp(&b.name).unwrap());
        Ok(databases)
    }

    pub async fn put_document<T: Serialize>(
        &self,
        username: &str,
        pod: &str,
        database: &str,
        doc: T,
    ) -> Result<String, FairOSError> {
        let id = Uuid::new_v4().to_string();
        let mut doc = json!(doc);
        doc["id"] = json!(&id);
        let data = json!({
            "pod_name": pod,
            "table_name": database,
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

    pub async fn get_document<T: DeserializeOwned>(
        &self,
        username: &str,
        pod: &str,
        database: &str,
        id: &str,
    ) -> Result<T, FairOSError> {
        let mut query = HashMap::new();
        query.insert("pod_name", pod);
        query.insert("table_name", database);
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

    pub async fn find_documents<T: DeserializeOwned>(
        &self,
        username: &str,
        pod: &str,
        database: &str,
        expr: Expr,
        limit: Option<u32>,
    ) -> Result<Vec<T>, FairOSError> {
        let mut query = HashMap::new();
        query.insert("pod_name", pod);
        query.insert("table_name", database);
        let expr_str = expr.to_string();
        query.insert("expr", expr_str.as_str());
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

    pub async fn delete_document(
        &self,
        username: &str,
        pod: &str,
        database: &str,
        id: &str,
    ) -> Result<(), FairOSError> {
        let data = json!({
            "pod_name": pod,
            "table_name": database,
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

    pub async fn count_documents(
        &self,
        username: &str,
        pod: &str,
        database: &str,
        expr: Expr,
    ) -> Result<u32, FairOSError> {
        let data = json!({
            "pod_name": pod,
            "table_name": database,
            "expr": expr.to_string(),
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

    pub async fn load_json_buffer<R: Read>(
        &self,
        username: &str,
        pod: &str,
        database: &str,
        buffer: R,
    ) -> Result<(), FairOSError> {
        let mut multipart = Multipart::new();
        multipart.add_text("pod_name", pod);
        multipart.add_text("table_name", database);
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

    pub async fn load_json_file<P: AsRef<Path>>(
        &self,
        username: &str,
        pod: &str,
        database: &str,
        local_path: P,
    ) -> Result<(), FairOSError> {
        let mut multipart = Multipart::new();
        multipart.add_text("pod_name", pod);
        multipart.add_text("table_name", database);
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

    pub async fn index_json(
        &self,
        username: &str,
        pod: &str,
        database: &str,
        file: &str,
    ) -> Result<(), FairOSError> {
        let data = json!({
            "pod_name": pod,
            "table_name": database,
            "file_name": file,
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
    use super::{Client, DocumentDatabase, Expr, ExprValue, FieldType};
    use rand::{
        distributions::{Alphanumeric, Uniform},
        thread_rng, Rng,
    };
    use serde::{Deserialize, Serialize};

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
    async fn test_create_doc_database_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod = random_name();
        let res = fairos.create_pod(&username, &pod, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .create_doc_database(
                &username,
                &pod,
                "table",
                &[("s", FieldType::Str), ("n", FieldType::Number)],
                true,
            )
            .await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_open_doc_database_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod = random_name();
        let res = fairos.create_pod(&username, &pod, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .create_doc_database(
                &username,
                &pod,
                "table",
                &[("s", FieldType::Str), ("n", FieldType::Number)],
                true,
            )
            .await;
        assert!(res.is_ok());
        let res = fairos.open_doc_database(&username, &pod, "table").await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_delete_doc_database_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod = random_name();
        let res = fairos.create_pod(&username, &pod, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .create_doc_database(
                &username,
                &pod,
                "table",
                &[("s", FieldType::Str), ("n", FieldType::Number)],
                true,
            )
            .await;
        assert!(res.is_ok());
        let res = fairos.delete_doc_database(&username, &pod, "table").await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_list_doc_databases_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod = random_name();
        let res = fairos.create_pod(&username, &pod, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .create_doc_database(
                &username,
                &pod,
                "table1",
                &[("s1", FieldType::Str), ("n2", FieldType::Number)],
                true,
            )
            .await;
        assert!(res.is_ok());
        let res = fairos
            .create_doc_database(&username, &pod, "table2", &[("m", FieldType::Map)], true)
            .await;
        assert!(res.is_ok());
        let res = fairos
            .create_doc_database(
                &username,
                &pod,
                "table3",
                &[("s2", FieldType::Str), ("n1", FieldType::Number)],
                true,
            )
            .await;
        assert!(res.is_ok());
        let res = fairos.list_doc_databases(&username, &pod).await;
        assert!(res.is_ok());
        assert_eq!(
            res.unwrap(),
            vec![
                DocumentDatabase {
                    name: "table1".into(),
                    fields: vec![
                        ("id".into(), FieldType::Str),
                        ("n2".into(), FieldType::Number),
                        ("s1".into(), FieldType::Str),
                    ],
                },
                DocumentDatabase {
                    name: "table2".into(),
                    fields: vec![("id".into(), FieldType::Str), ("m".into(), FieldType::Map)],
                },
                DocumentDatabase {
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
    async fn test_put_document_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod = random_name();
        let res = fairos.create_pod(&username, &pod, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .create_doc_database(
                &username,
                &pod,
                "table",
                &[("s", FieldType::Str), ("n", FieldType::Number)],
                true,
            )
            .await;
        assert!(res.is_ok());
        let res = fairos.open_doc_database(&username, &pod, "table").await;
        assert!(res.is_ok());
        let res = fairos
            .put_document(
                &username,
                &pod,
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
    async fn test_get_document_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod = random_name();
        let res = fairos.create_pod(&username, &pod, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .create_doc_database(
                &username,
                &pod,
                "table",
                &[("s", FieldType::Str), ("n", FieldType::Number)],
                true,
            )
            .await;
        assert!(res.is_ok());
        let res = fairos.open_doc_database(&username, &pod, "table").await;
        assert!(res.is_ok());
        let res = fairos
            .put_document(
                &username,
                &pod,
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
            .get_document::<TestData>(&username, &pod, "table", &id)
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
    async fn test_find_documents_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod = random_name();
        let res = fairos.create_pod(&username, &pod, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .create_doc_database(
                &username,
                &pod,
                "table",
                &[("s", FieldType::Str), ("n", FieldType::Number)],
                true,
            )
            .await;
        assert!(res.is_ok());
        let res = fairos.open_doc_database(&username, &pod, "table").await;
        assert!(res.is_ok());
        let res = fairos
            .put_document(
                &username,
                &pod,
                "table",
                TestData {
                    s: "a".into(),
                    n: 8,
                },
            )
            .await;
        assert!(res.is_ok());
        let res = fairos
            .put_document(
                &username,
                &pod,
                "table",
                TestData {
                    s: "a".into(),
                    n: 10,
                },
            )
            .await;
        assert!(res.is_ok());
        let res = fairos
            .put_document(
                &username,
                &pod,
                "table",
                TestData {
                    s: "b".into(),
                    n: 12,
                },
            )
            .await;
        assert!(res.is_ok());
        let res = fairos
            .find_documents::<TestData>(&username, &pod, "table", Expr::Gt("n".into(), ExprValue::Number(9)), None)
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
            .find_documents::<TestData>(&username, &pod, "table", Expr::Eq("s".into(), ExprValue::Str("a".into())), None)
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
    async fn test_delete_document_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod = random_name();
        let res = fairos.create_pod(&username, &pod, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .create_doc_database(
                &username,
                &pod,
                "table",
                &[("s", FieldType::Str), ("n", FieldType::Number)],
                true,
            )
            .await;
        assert!(res.is_ok());
        let res = fairos.open_doc_database(&username, &pod, "table").await;
        assert!(res.is_ok());
        let res = fairos
            .put_document(
                &username,
                &pod,
                "table",
                TestData {
                    s: "text".into(),
                    n: 12,
                },
            )
            .await;
        assert!(res.is_ok());
        let id = res.unwrap();
        let res = fairos.delete_document(&username, &pod, "table", &id).await;
        assert!(res.is_ok());
        let res = fairos
            .get_document::<TestData>(&username, &pod, "table", &id)
            .await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_count_documents_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod = random_name();
        let res = fairos.create_pod(&username, &pod, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .create_doc_database(
                &username,
                &pod,
                "table",
                &[("s", FieldType::Str), ("n", FieldType::Number)],
                true,
            )
            .await;
        assert!(res.is_ok());
        let res = fairos.open_doc_database(&username, &pod, "table").await;
        assert!(res.is_ok());
        let res = fairos
            .put_document(
                &username,
                &pod,
                "table",
                TestData {
                    s: "text".into(),
                    n: 12,
                },
            )
            .await;
        assert!(res.is_ok());
        let res = fairos
            .put_document(
                &username,
                &pod,
                "table",
                TestData {
                    s: "text".into(),
                    n: 10,
                },
            )
            .await;
        assert!(res.is_ok());
        let res = fairos.count_documents(&username, &pod, "table", Expr::All).await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), 2);
    }

    // #[tokio::test]
    // async fn test_load_json_buffer_succeeds() {
    //     let mut fairos = Client::new();
    //     let username = random_name();
    //     let password = random_password();
    //     let res = fairos.signup(&username, &password, None).await;
    //     assert!(res.is_ok());
    //     let pod = random_name();
    //     let res = fairos.create_pod(&username, &pod, &password).await;
    //     assert!(res.is_ok());
    //     let res = fairos
    //         .create_doc_database(
    //             &username,
    //             &pod,
    //             "table",
    //             &[("s", FieldType::Str), ("n", FieldType::Number)],
    //             true,
    //         )
    //         .await;
    //     assert!(res.is_ok());
    //     let res = fairos.open_doc_database(&username, &pod, "table").await;
    //     assert!(res.is_ok());
    //     let res = fairos.load_json_buffer(&username, &pod, "table", "[{\"s\": \"text\", \"n\": 12}, {\"s\": \"text\", \"n\": 10}]".as_bytes()).await;
    //     assert!(res.is_ok());
    //     let res = fairos.count_documents(&username, &pod, "table", None).await;
    //     assert!(res.is_ok());
    //     assert_eq!(res.unwrap(), 2);
    // }

    // #[tokio::test]
    // async fn test_load_json_file_succeeds() {
    //     let mut fairos = Client::new();
    //     let username = random_name();
    //     let password = random_password();
    //     let res = fairos.signup(&username, &password, None).await;
    //     assert!(res.is_ok());
    //     let pod = random_name();
    //     let res = fairos.create_pod(&username, &pod, &password).await;
    //     assert!(res.is_ok());
    //     let res = fairos
    //         .create_doc_database(
    //             &username,
    //             &pod,
    //             "table",
    //             &[("s", FieldType::Str), ("n", FieldType::Number)],
    //             true,
    //         )
    //         .await;
    //     assert!(res.is_ok());
    //     let res = fairos.open_doc_database(&username, &pod, "table").await;
    //     assert!(res.is_ok());
    //     fs::write("data.json", "[{\"s\": \"text\", \"n\": 12}, {\"s\": \"text\", \"n\": 10}]").unwrap();
    //     let res = fairos.load_json_file(&username, &pod, "table", "data.json").await;
    //     assert!(res.is_ok());
    //     fs::remove_file("data.json").unwrap();
    //     let res = fairos.count_documents(&username, &pod, "table", None).await;
    //     assert!(res.is_ok());
    //     assert_eq!(res.unwrap(), 2);
    // }

    // #[tokio::test]
    // async fn test_index_json_succeeds() {
    //     let mut fairos = Client::new();
    //     let username = random_name();
    //     let password = random_password();
    //     let res = fairos.signup(&username, &password, None).await;
    //     assert!(res.is_ok());
    //     let pod = random_name();
    //     let res = fairos.create_pod(&username, &pod, &password).await;
    //     assert!(res.is_ok());
    //     let res = fairos
    //         .create_doc_database(
    //             &username,
    //             &pod,
    //             "table",
    //             &[("s", FieldType::Str), ("n", FieldType::Number)],
    //             true,
    //         )
    //         .await;
    //     assert!(res.is_ok());
    //     let res = fairos.open_doc_database(&username, &pod, "table").await;
    //     assert!(res.is_ok());
    //     let res = fairos.index_json(&username, &pod, "table", "table.json").await;
    //     assert!(res.is_ok());
    // }
}
