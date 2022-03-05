use crate::{error::FairOSError, Client};

use bip39::Mnemonic;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
pub struct UserSignupResponse {
    address: String,
    mnemonic: Option<String>,
}

#[derive(Debug, Deserialize)]
struct UserLoginResponse {
    message: String,
    code: u32,
}

impl Client {
    pub fn generate_mnemonic() -> String {
        let mut rng = ChaCha20Rng::from_entropy();
        let mut entropy = [0u8; 16];
        rng.fill(&mut entropy);
        Mnemonic::from_entropy(&entropy).unwrap().to_string()
    }

    pub async fn user_new(
        &self,
        username: &str,
        password: &str,
        mnemonic: Option<&str>,
    ) -> Result<UserSignupResponse, FairOSError> {
        let data = json!({
            "user_name": username,
            "password": password,
            "mnemonic": mnemonic,
        })
        .to_string()
        .as_bytes()
        .to_vec();
        self.post("/user/signup", data).await
    }

    pub async fn user_login(&self, username: &str, password: &str) -> Result<(), FairOSError> {
        let data = json!({
            "user_name": username,
            "password": password,
        })
        .to_string()
        .as_bytes()
        .to_vec();
        let res: UserLoginResponse = self.post("/user/login", data).await?;
        if res.message == "user logged-in successfully" {
            Ok(())
        } else {
            Err(FairOSError::Error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Client;
    use core::str;
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
    async fn test_user_new_with_mnemonic() {
        let fairos = Client::new();
        let username = random_username();
        let password = random_password();
        let mnemonic = Client::generate_mnemonic();
        let res = fairos.user_new(&username, &password, Some(&mnemonic)).await;
        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(!res.address.is_empty());
        assert!(res.mnemonic.is_none());
    }

    #[tokio::test]
    async fn test_user_new_without_mnemonic() {
        let fairos = Client::new();
        let username = random_username();
        let password = random_password();
        let res = fairos.user_new(&username, &password, None).await;
        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(!res.address.is_empty());
        assert!(res.mnemonic.is_some());
    }

    #[tokio::test]
    async fn test_user_login_succeeds() {
        let fairos = Client::new();
        let username = random_username();
        let password = random_password();
        let res = fairos.user_new(&username, &password, None).await;
        assert!(res.is_ok());
        let res = fairos.user_login(&username, &password).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_user_login_fails() {
        let fairos = Client::new();
        let username = random_username();
        let password = random_password();
        let res = fairos.user_new(&username, &password, None).await;
        assert!(res.is_ok());
        let password2 = random_password();
        let res = fairos.user_login(&username, &password2).await;
        assert!(res.is_err());
    }
}
