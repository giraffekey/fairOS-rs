use crate::{
    client::{MessageResponse, RequestError},
    Client, FairOSError, FairOSPodError,
};

use std::collections::HashMap;

use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
struct PodShareResponse {
    pod_sharing_reference: String,
}

#[derive(Debug, Deserialize)]
struct PodPresentResponse {
    present: bool,
}

#[derive(Debug, Deserialize)]
struct PodListResponse {
    pod_name: Vec<String>,
    shared_pod_name: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct PodStatResponse {
    pod_name: String,
    address: String,
}

#[derive(Debug, Deserialize)]
struct PodReceiveInfoResponse {
    pod_name: String,
    pod_address: String,
    user_name: String,
    user_address: String,
    shared_time: String,
}

#[derive(Debug)]
pub struct PodInfo {
    pub name: String,
    pub address: String,
}

#[derive(Debug)]
pub struct SharedPodInfo {
    pub name: String,
    pub address: String,
    pub username: String,
    pub user_address: String,
    pub shared_time: String,
}

impl Client {
    pub async fn create_pod(
        &self,
        username: &str,
        name: &str,
        password: &str,
    ) -> Result<(), FairOSError> {
        let data = json!({
            "pod_name": name,
            "password": password,
        })
        .to_string()
        .as_bytes()
        .to_vec();
        let cookie = self.cookie(username).unwrap();
        let _ = self
            .post::<MessageResponse>("/pod/new", data, Some(cookie))
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::Pod(FairOSPodError::Error),
            })?;
        Ok(())
    }

    pub async fn open_pod(
        &self,
        username: &str,
        name: &str,
        password: &str,
    ) -> Result<(), FairOSError> {
        let data = json!({
            "pod_name": name,
            "password": password,
        })
        .to_string()
        .as_bytes()
        .to_vec();
        let cookie = self.cookie(username).unwrap();
        let _ = self
            .post::<MessageResponse>("/pod/open", data, Some(cookie))
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::Pod(FairOSPodError::Error),
            })?;
        Ok(())
    }

    pub async fn sync_pod(&self, username: &str, name: &str) -> Result<(), FairOSError> {
        let data = json!({ "pod_name": name }).to_string().as_bytes().to_vec();
        let cookie = self.cookie(username).unwrap();
        let _ = self
            .post::<MessageResponse>("/pod/sync", data, Some(cookie))
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::Pod(FairOSPodError::Error),
            })?;
        Ok(())
    }

    pub async fn close_pod(&self, username: &str, name: &str) -> Result<(), FairOSError> {
        let data = json!({ "pod_name": name }).to_string().as_bytes().to_vec();
        let cookie = self.cookie(username).unwrap();
        let _ = self
            .post::<MessageResponse>("/pod/close", data, Some(cookie))
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::Pod(FairOSPodError::Error),
            })?;
        Ok(())
    }

    pub async fn share_pod(
        &self,
        username: &str,
        name: &str,
        password: &str,
    ) -> Result<String, FairOSError> {
        let data = json!({
            "pod_name": name,
            "password": password,
        })
        .to_string()
        .as_bytes()
        .to_vec();
        let cookie = self.cookie(username).unwrap();
        let (res, _) = self
            .post::<PodShareResponse>("/pod/share", data, Some(cookie))
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::Pod(FairOSPodError::Error),
            })?;
        Ok(res.pod_sharing_reference)
    }

    pub async fn delete_pod(
        &self,
        username: &str,
        name: &str,
        password: &str,
    ) -> Result<(), FairOSError> {
        let data = json!({
            "pod_name": name,
            "password": password,
        })
        .to_string()
        .as_bytes()
        .to_vec();
        let cookie = self.cookie(username).unwrap();
        let _: MessageResponse = self
            .delete("/pod/delete", data, cookie)
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::Pod(FairOSPodError::Error),
            })?;
        Ok(())
    }

    pub async fn pod_exists(&self, username: &str, name: &str) -> Result<bool, FairOSError> {
        let mut query = HashMap::new();
        query.insert("pod_name", name);
        let cookie = self.cookie(username).unwrap();
        let res: PodPresentResponse = self
            .get("/pod/present", query, Some(cookie))
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::Pod(FairOSPodError::Error),
            })?;
        Ok(res.present)
    }

    pub async fn list_pods(
        &self,
        username: &str,
    ) -> Result<(Vec<String>, Vec<String>), FairOSError> {
        let cookie = self.cookie(username).unwrap();
        let res: PodListResponse = self
            .get("/pod/ls", HashMap::new(), Some(cookie))
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::Pod(FairOSPodError::Error),
            })?;
        Ok((res.pod_name, res.shared_pod_name))
    }

    pub async fn pod_info(&self, username: &str, name: &str) -> Result<PodInfo, FairOSError> {
        let mut query = HashMap::new();
        query.insert("pod_name", name);
        let cookie = self.cookie(username).unwrap();
        let res: PodStatResponse =
            self.get("/pod/stat", query, Some(cookie))
                .await
                .map_err(|err| match err {
                    RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                    RequestError::Message(_) => FairOSError::Pod(FairOSPodError::Error),
                })?;
        Ok(PodInfo {
            name: res.pod_name,
            address: res.address,
        })
    }

    pub async fn receive_shared_pod(
        &self,
        username: &str,
        reference: &str,
    ) -> Result<(), FairOSError> {
        let mut query = HashMap::new();
        query.insert("sharing_ref", reference);
        let cookie = self.cookie(username).unwrap();
        let _: MessageResponse = self
            .get("/pod/receive", query, Some(cookie))
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::Pod(FairOSPodError::Error),
            })?;
        Ok(())
    }

    pub async fn shared_pod_info(
        &self,
        username: &str,
        reference: &str,
    ) -> Result<SharedPodInfo, FairOSError> {
        let mut query = HashMap::new();
        query.insert("sharing_ref", reference);
        let cookie = self.cookie(username).unwrap();
        let res: PodReceiveInfoResponse = self
            .get("/pod/receiveinfo", query, Some(cookie))
            .await
            .map_err(|err| match err {
            RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
            RequestError::Message(_) => FairOSError::Pod(FairOSPodError::Error),
        })?;
        Ok(SharedPodInfo {
            name: res.pod_name,
            address: res.pod_address,
            username: res.user_name,
            user_address: res.user_address,
            shared_time: res.shared_time,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::Client;
    use rand::{
        distributions::{Alphanumeric, Uniform},
        thread_rng, Rng,
    };

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
    async fn test_create_pod_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod_name = random_name();
        let res = fairos.create_pod(&username, &pod_name, &password).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_open_pod_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod_name = random_name();
        let res = fairos.create_pod(&username, &pod_name, &password).await;
        assert!(res.is_ok());
        let res = fairos.close_pod(&username, &pod_name).await;
        assert!(res.is_ok());
        let res = fairos.open_pod(&username, &pod_name, &password).await;
        println!("{:?}", res);
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_sync_pod_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod_name = random_name();
        let res = fairos.create_pod(&username, &pod_name, &password).await;
        assert!(res.is_ok());
        let res = fairos.sync_pod(&username, &pod_name).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_close_pod_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod_name = random_name();
        let res = fairos.create_pod(&username, &pod_name, &password).await;
        assert!(res.is_ok());
        let res = fairos.close_pod(&username, &pod_name).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_share_pod_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod_name = random_name();
        let res = fairos.create_pod(&username, &pod_name, &password).await;
        assert!(res.is_ok());
        let res = fairos.share_pod(&username, &pod_name, &password).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_delete_pod_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod_name = random_name();
        let res = fairos.create_pod(&username, &pod_name, &password).await;
        assert!(res.is_ok());
        let res = fairos.delete_pod(&username, &pod_name, &password).await;
        assert!(res.is_ok());
        let res = fairos.pod_exists(&username, &pod_name).await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), false);
    }

    #[tokio::test]
    async fn test_pod_exists_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod_name = random_name();
        let res = fairos.create_pod(&username, &pod_name, &password).await;
        assert!(res.is_ok());
        let res = fairos.pod_exists(&username, &pod_name).await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), true);
        let pod_name = random_name();
        let res = fairos.pod_exists(&username, &pod_name).await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), false);
    }

    #[tokio::test]
    async fn test_list_pods_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod_name1 = random_name();
        let res = fairos.create_pod(&username, &pod_name1, &password).await;
        assert!(res.is_ok());
        let pod_name2 = random_name();
        let res = fairos.create_pod(&username, &pod_name2, &password).await;
        assert!(res.is_ok());
        let res = fairos.list_pods(&username).await;
        assert!(res.is_ok());
        let (pods, shared_pods) = res.unwrap();
        assert_eq!(pods, vec![pod_name1, pod_name2]);
        assert_eq!(shared_pods, Vec::<String>::new());
    }

    #[tokio::test]
    async fn test_pod_info_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod_name = random_name();
        let res = fairos.create_pod(&username, &pod_name, &password).await;
        assert!(res.is_ok());
        let res = fairos.pod_info(&username, &pod_name).await;
        assert!(res.is_ok());
        let info = res.unwrap();
        assert_eq!(pod_name, info.name);
    }

    #[tokio::test]
    async fn test_receive_shared_pod_succeeds() {
        let mut fairos = Client::new();

        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let pod_name = random_name();
        let res = fairos.create_pod(&username, &pod_name, &password).await;
        assert!(res.is_ok());
        let res = fairos.share_pod(&username, &pod_name, &password).await;
        assert!(res.is_ok());
        let reference = res.unwrap();

        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let res = fairos.receive_shared_pod(&username, &reference).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_shared_pod_info_succeeds() {
        let mut fairos = Client::new();

        let username1 = random_name();
        let password = random_password();
        let res = fairos.signup(&username1, &password, None).await;
        assert!(res.is_ok());
        let (address, _) = res.unwrap();
        let pod_name = random_name();
        let res = fairos.create_pod(&username1, &pod_name, &password).await;
        assert!(res.is_ok());
        let res = fairos.share_pod(&username1, &pod_name, &password).await;
        assert!(res.is_ok());
        let reference = res.unwrap();

        let username2 = random_name();
        let password = random_password();
        let res = fairos.signup(&username2, &password, None).await;
        assert!(res.is_ok());
        let res = fairos.receive_shared_pod(&username2, &reference).await;
        assert!(res.is_ok());
        let res = fairos.shared_pod_info(&username2, &reference).await;
        assert!(res.is_ok());
        let info = res.unwrap();
        assert_eq!(info.name, pod_name);
        assert_eq!(info.username, username1);
        // assert_eq!(info.user_address, address);
    }
}
