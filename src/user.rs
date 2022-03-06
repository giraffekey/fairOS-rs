use crate::{error::FairOSError, Client};

use std::collections::HashMap;

use bip39::Mnemonic;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
struct MessageResponse {
    message: String,
    code: u32,
}

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

impl Client {
    pub fn generate_mnemonic() -> String {
        let mut rng = ChaCha20Rng::from_entropy();
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
            .await?;
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
        let (res, cookie) = self
            .post::<MessageResponse>("/user/login", data, None)
            .await?;
        if res.code == 200 && res.message == "user logged-in successfully" {
        	self.set_cookie(username, cookie.unwrap());
            Ok(())
        } else {
            Err(FairOSError::Error)
        }
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
            .await?;
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
            .await?;
        self.set_cookie(username, cookie.unwrap());
        Ok(res.address)
    }

    pub async fn username_exists(&self, username: &str) -> Result<bool, FairOSError> {
        let mut query = HashMap::new();
        query.insert("user_name", username);
        let res: UserPresentResponse = self.get("/user/present", query, None).await?;
        Ok(res.present)
    }

    pub async fn is_logged_in(&self, username: &str) -> Result<bool, FairOSError> {
        let mut query = HashMap::new();
        query.insert("user_name", username);
        let res: UserIsLoggedInResponse = self.get("/user/isloggedin", query, None).await?;
        Ok(res.loggedin)
    }

    pub async fn logout(&mut self, username: &str) -> Result<(), FairOSError> {
    	let cookie = self.cookie(username).unwrap();
        let (res, _) = self
            .post::<MessageResponse>("/user/logout", Vec::new(), Some(cookie))
            .await?;
        self.remove_cookie(username);
        Ok(())
    }

    pub async fn export_user(&self, username: &str) -> Result<(String, String), FairOSError> {
    	let cookie = self.cookie(username).unwrap();
        let (res, _) = self
            .post::<UserExportResponse>("/user/export", Vec::new(), Some(cookie))
            .await?;
        Ok((res.user_name, res.address))
    }

    pub async fn delete_user(&mut self, username: &str, password: &str) -> Result<(), FairOSError> {
        let data = json!({"password": password})
        .to_string()
        .as_bytes()
        .to_vec();
    	let cookie = self.cookie(username).unwrap();
        let res: MessageResponse = self
            .delete("/user/delete", data, cookie)
            .await?;
        self.remove_cookie(username);
        Ok(())
    }

    pub async fn user_info(&self, username: &str) -> Result<(String, String), FairOSError> {
    	let cookie = self.cookie(username).unwrap();
        let res: UserStatResponse = self.get("/user/stat", HashMap::new(), Some(cookie)).await?;
        Ok((res.user_name, res.address))
    }
}

#[cfg(test)]
mod tests {
    use super::Client;
    use rand::{
        distributions::{Alphanumeric, Uniform},
        thread_rng, Rng,
    };

    fn random_username() -> String {
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
        let mnemonic = Client::generate_mnemonic();
        assert_eq!(mnemonic.split(" ").count(), 12);
    }

    #[tokio::test]
    async fn test_signup_with_mnemonic() {
        let mut fairos = Client::new();
        let username = random_username();
        let password = random_password();
        let mnemonic = Client::generate_mnemonic();
        let res = fairos.signup(&username, &password, Some(&mnemonic)).await;
        assert!(res.is_ok());
        let (address, mnemonic) = res.unwrap();
        assert!(!address.is_empty());
        assert!(mnemonic.is_none());
    }

    #[tokio::test]
    async fn test_signup_without_mnemonic() {
        let mut fairos = Client::new();
        let username = random_username();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let (address, mnemonic) = res.unwrap();
        assert!(!address.is_empty());
        assert!(mnemonic.is_some());
    }

    #[tokio::test]
    async fn test_login_succeeds() {
        let mut fairos = Client::new();
        let username = random_username();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let res = fairos.login(&username, &password).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_import_with_address() {
        let mut fairos = Client::new();
        let username = random_username();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let (address1, _) = res.unwrap();
  		let res = fairos.delete_user(&username, &password).await;
  		assert!(res.is_ok());
        let res = fairos.import_with_address(&username, &password, &address1).await;
        assert!(res.is_ok());
        let address2 = res.unwrap();
        assert_eq!(address1, address2);
    }

    #[tokio::test]
    async fn test_import_with_mnemonic() {
        let mut fairos = Client::new();
        let username = random_username();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let (address1, mnemonic) = res.unwrap();
        assert!(mnemonic.is_some());
        let mnemonic = mnemonic.unwrap();
  		let res = fairos.delete_user(&username, &password).await;
  		assert!(res.is_ok());
        let res = fairos.import_with_mnemonic(&username, &password, &mnemonic).await;
        assert!(res.is_ok());
        let address2 = res.unwrap();
        assert_eq!(address1, address2);
    }

    #[tokio::test]
    async fn test_login_fails() {
        let mut fairos = Client::new();
        let username = random_username();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let password = random_password();
        let res = fairos.login(&username, &password).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_username_exists() {
        let mut fairos = Client::new();
        let username = random_username();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let res = fairos.username_exists(&username).await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), true);
        let username = random_username();
        let res = fairos.username_exists(&username).await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), false);
    }

    #[tokio::test]
    async fn test_is_logged_in() {
        let mut fairos = Client::new();
        let username = random_username();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let res = fairos.is_logged_in(&username).await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), true);
    }

    #[tokio::test]
    async fn test_logout() {
        let mut fairos = Client::new();
        let username = random_username();
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
    async fn test_export_user() {
        let mut fairos = Client::new();
        let username1 = random_username();
        let password = random_password();
        let res = fairos.signup(&username1, &password, None).await;
        assert!(res.is_ok());
        let (address1, _) = res.unwrap();
        let res = fairos.export_user(&username1).await;
        assert!(res.is_ok());
        let (username2, address2) = res.unwrap();
        assert_eq!(username1, username2);
        assert_eq!(address1, address2);
    }

    #[tokio::test]
    async fn test_delete_user() {
        let mut fairos = Client::new();
        let username = random_username();
        let password = random_password();
        let res = fairos.signup(&username, &password, None).await;
        assert!(res.is_ok());
        let res = fairos.delete_user(&username, &password).await;
        assert!(res.is_ok());
        let res = fairos.username_exists(&username).await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), false);
    }

    #[tokio::test]
    async fn test_user_info() {
        let mut fairos = Client::new();
        let username1 = random_username();
        let password = random_password();
        let res = fairos.signup(&username1, &password, None).await;
        assert!(res.is_ok());
        let (address1, _) = res.unwrap();
        let res = fairos.user_info(&username1).await;
        assert!(res.is_ok());
        let (username2, address2) = res.unwrap();
        assert_eq!(username1, username2);
        assert_eq!(address1, address2);
    }
}
