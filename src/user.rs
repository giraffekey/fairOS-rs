use crate::{
    client::{MessageResponse, RequestError},
    Client, FairOSError, FairOSUserError,
};

use std::collections::HashMap;

use bip39::Mnemonic;
use rand::Rng;
use rand_chacha::ChaCha20Rng;
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
struct UserSignupResponse {
    address: String,
    mnemonic: Option<String>,
}

#[derive(Debug, Deserialize)]
struct UserImportResponse {
    address: String,
}

#[derive(Debug, Deserialize)]
struct UserPresentResponse {
    present: bool,
}

#[derive(Debug, Deserialize)]
struct UserIsLoggedInResponse {
    loggedin: bool,
}

#[derive(Debug, Deserialize)]
struct UserExportResponse {
    user_name: String,
    address: String,
}

#[derive(Debug, Deserialize)]
struct UserStatResponse {
    user_name: String,
    address: String,
}

#[derive(Debug)]
pub struct UserExport {
    pub username: String,
    pub address: String,
}

#[derive(Debug)]
pub struct UserInfo {
    pub username: String,
    pub address: String,
}

impl Client {
    pub fn generate_mnemonic(rng: &mut ChaCha20Rng) -> String {
        let mut entropy = [0u8; 16];
        rng.fill(&mut entropy);
        Mnemonic::from_entropy(&entropy).unwrap().to_string()
    }

    pub async fn signup(
        &mut self,
        username: &str,
        password: &str,
        mnemonic: Option<&str>,
    ) -> Result<(String, Option<String>), FairOSError> {
        let data = json!({
            "user_name": username,
            "password": password,
            "mnemonic": mnemonic,
        })
        .to_string()
        .as_bytes()
        .to_vec();
        let (res, cookie) = self
            .post::<UserSignupResponse>("/user/signup", data, None)
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(msg) => match msg.as_str() {
                    "user signup: user name already present" => {
                        FairOSError::User(FairOSUserError::UsernameAlreadyExists)
                    }
                    _ => FairOSError::User(FairOSUserError::Error),
                },
            })?;
        self.set_cookie(username, cookie.unwrap());
        Ok((res.address, res.mnemonic))
    }

    pub async fn login(&mut self, username: &str, password: &str) -> Result<(), FairOSError> {
        let data = json!({
            "user_name": username,
            "password": password,
        })
        .to_string()
        .as_bytes()
        .to_vec();
        let (_, cookie) = self
            .post::<MessageResponse>("/user/login", data, None)
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(msg) => match msg.as_str() {
                    "user login: invalid user name" => {
                        FairOSError::User(FairOSUserError::InvalidUsername)
                    }
                    "user login: invalid password" => {
                        FairOSError::User(FairOSUserError::InvalidPassword)
                    }
                    _ => FairOSError::User(FairOSUserError::Error),
                },
            })?;
        self.set_cookie(username, cookie.unwrap());
        Ok(())
    }

    pub async fn import_with_address(
        &mut self,
        username: &str,
        password: &str,
        address: &str,
    ) -> Result<String, FairOSError> {
        let data = json!({
            "user_name": username,
            "password": password,
            "address": address,
        })
        .to_string()
        .as_bytes()
        .to_vec();
        let (res, cookie) = self
            .post::<UserImportResponse>("/user/import", data, None)
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::User(FairOSUserError::Error),
            })?;
        self.set_cookie(username, cookie.unwrap());
        Ok(res.address)
    }

    pub async fn import_with_mnemonic(
        &mut self,
        username: &str,
        password: &str,
        mnemonic: &str,
    ) -> Result<String, FairOSError> {
        let data = json!({
            "user_name": username,
            "password": password,
            "mnemonic": mnemonic,
        })
        .to_string()
        .as_bytes()
        .to_vec();
        let (res, cookie) = self
            .post::<UserImportResponse>("/user/import", data, None)
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::User(FairOSUserError::Error),
            })?;
        self.set_cookie(username, cookie.unwrap());
        Ok(res.address)
    }

    pub async fn delete_user(&mut self, username: &str, password: &str) -> Result<(), FairOSError> {
        let data = json!({ "password": password })
            .to_string()
            .as_bytes()
            .to_vec();
        let cookie = self.cookie(username).unwrap();
        let _: MessageResponse =
            self.delete("/user/delete", data, cookie)
                .await
                .map_err(|err| match err {
                    RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                    RequestError::Message(_) => FairOSError::User(FairOSUserError::Error),
                })?;
        self.remove_cookie(username);
        Ok(())
    }

    pub async fn user_exists(&self, username: &str) -> Result<bool, FairOSError> {
        let mut query = HashMap::new();
        query.insert("user_name", username);
        let res: UserPresentResponse =
            self.get("/user/present", query, None)
                .await
                .map_err(|err| match err {
                    RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                    RequestError::Message(_) => FairOSError::User(FairOSUserError::Error),
                })?;
        Ok(res.present)
    }

    pub async fn is_logged_in(&self, username: &str) -> Result<bool, FairOSError> {
        let mut query = HashMap::new();
        query.insert("user_name", username);
        let res: UserIsLoggedInResponse =
            self.get("/user/isloggedin", query, None)
                .await
                .map_err(|err| match err {
                    RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                    RequestError::Message(_) => FairOSError::User(FairOSUserError::Error),
                })?;
        Ok(res.loggedin)
    }

    pub async fn logout(&mut self, username: &str) -> Result<(), FairOSError> {
        let cookie = self.cookie(username).unwrap();
        let _ = self
            .post::<MessageResponse>("/user/logout", Vec::new(), Some(cookie))
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::User(FairOSUserError::Error),
            })?;
        self.remove_cookie(username);
        Ok(())
    }

    pub async fn export_user(&self, username: &str) -> Result<UserExport, FairOSError> {
        let cookie = self.cookie(username).unwrap();
        let (res, _) = self
            .post::<UserExportResponse>("/user/export", Vec::new(), Some(cookie))
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::User(FairOSUserError::Error),
            })?;
        Ok(UserExport {
            username: res.user_name,
            address: res.address,
        })
    }

    pub async fn user_info(&self, username: &str) -> Result<UserInfo, FairOSError> {
        let cookie = self.cookie(username).unwrap();
        let res: UserStatResponse = self
            .get("/user/stat", HashMap::new(), Some(cookie))
            .await
            .map_err(|err| match err {
                RequestError::CouldNotConnect => FairOSError::CouldNotConnect,
                RequestError::Message(_) => FairOSError::User(FairOSUserError::Error),
            })?;
        Ok(UserInfo {
            username: res.user_name,
            address: res.address,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{Client, FairOSError, FairOSUserError};
    use rand_chacha::ChaCha20Rng;
    use rand::{
        distributions::{Alphanumeric, Uniform},
        thread_rng, Rng, SeedableRng,
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
    async fn test_generate_mnemonic() {
        let mut rng = ChaCha20Rng::from_entropy();
        let mnemonic = Client::generate_mnemonic(&mut rng);
        assert_eq!(mnemonic.split(" ").count(), 12);
    }

    #[tokio::test]
    async fn test_signup_with_mnemonic_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let mut rng = ChaCha20Rng::from_entropy();
        let mnemonic = Client::generate_mnemonic(&mut rng);
        let res = fairos.signup(&username, &password, Some(&mnemonic)).await;
        assert!(res.is_ok());
        let (address, mnemonic) = res.unwrap();
        assert!(!address.is_empty());
        assert!(mnemonic.is_none());
    }

    #[tokio::test]
    async fn test_signup_without_mnemonic_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let (address, mnemonic) = res.unwrap();
        assert!(!address.is_empty());
        assert!(mnemonic.is_some());
    }

    #[tokio::test]
    async fn test_signup_username_already_exists_fails() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err(),
            FairOSError::User(FairOSUserError::UsernameAlreadyExists),
        );
    }

    #[tokio::test]
    async fn test_login_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let res = fairos.login(&username, &password).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_login_invalid_username_fails() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let username = random_name();
        let res = fairos.login(&username, &password).await;
        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err(),
            FairOSError::User(FairOSUserError::InvalidUsername),
        );
    }

    #[tokio::test]
    async fn test_login_invalid_password_fails() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let password = random_password();
        let res = fairos.login(&username, &password).await;
        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err(),
            FairOSError::User(FairOSUserError::InvalidPassword),
        );
    }

    #[tokio::test]
    async fn test_import_with_address_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let (address1, _) = res.unwrap();
        let res = fairos.delete_user(&username, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .import_with_address(&username, &password, &address1)
            .await;
        assert!(res.is_ok());
        let address2 = res.unwrap();
        assert_eq!(address1, address2);
    }

    #[tokio::test]
    async fn test_import_with_mnemonic_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let (address1, mnemonic) = res.unwrap();
        assert!(mnemonic.is_some());
        let mnemonic = mnemonic.unwrap();
        let res = fairos.delete_user(&username, &password).await;
        assert!(res.is_ok());
        let res = fairos
            .import_with_mnemonic(&username, &password, &mnemonic)
            .await;
        assert!(res.is_ok());
        let address2 = res.unwrap();
        assert_eq!(address1, address2);
    }

    #[tokio::test]
    async fn test_delete_user_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let res = fairos.delete_user(&username, &password).await;
        assert!(res.is_ok());
        let res = fairos.user_exists(&username).await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), false);
    }

    #[tokio::test]
    async fn test_user_exists_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let res = fairos.user_exists(&username).await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), true);
        let username = random_name();
        let res = fairos.user_exists(&username).await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), false);
    }

    #[tokio::test]
    async fn test_is_logged_in_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let res = fairos.is_logged_in(&username).await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), true);
    }

    #[tokio::test]
    async fn test_logout_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let res = fairos.logout(&username).await;
        assert!(res.is_ok());
        let res = fairos.is_logged_in(&username).await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), false);
    }

    #[tokio::test]
    async fn test_export_user_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let (address, _) = res.unwrap();
        let res = fairos.export_user(&username).await;
        assert!(res.is_ok());
        let export = res.unwrap();
        assert_eq!(export.username, username);
        assert_eq!(export.address, address);
    }

    #[tokio::test]
    async fn test_user_info_succeeds() {
        let mut fairos = Client::new();
        let username = random_name();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let (address, _) = res.unwrap();
        let res = fairos.user_info(&username).await;
        assert!(res.is_ok());
        let info = res.unwrap();
        assert_eq!(info.username, username);
        assert_eq!(info.address, address);
    }
}
