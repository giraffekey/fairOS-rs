use crate::error::FairOSError;

use core::{str::FromStr, time::Duration};
use std::collections::HashMap;

use hyper::header::{CONTENT_TYPE, COOKIE, SET_COOKIE};
use hyper::{client::HttpConnector, Body, Request, StatusCode, Uri};
use hyper_tls::HttpsConnector;
use serde::{de::DeserializeOwned, Deserialize};

const IDLE_TIMEOUT: u64 = 6000;
const MAX_IDLE_PER_HOST: usize = 20;

#[derive(Debug)]
pub(crate) enum RequestError {
    CouldNotConnect,
    Message(String),
}

#[derive(Debug, Deserialize)]
pub(crate) struct MessageResponse {
    pub message: String,
    pub code: u32,
}

fn is_status_ok(status: StatusCode) -> bool {
    let status = status.as_u16();
    status >= 200 && status < 300
}

pub struct Client {
    url: String,
    http_client: hyper::Client<HttpsConnector<HttpConnector>>,
    cookies: HashMap<String, String>,
}

impl Client {
    pub fn new() -> Self {
        Self::new_with_url(None)
    }

    pub fn new_with_url(server_url: Option<&str>) -> Self {
        let url = server_url.unwrap_or("http://localhost:9090/v1").to_string();

        let https = HttpsConnector::new();
        let http_client = hyper::Client::builder()
            .pool_idle_timeout(Duration::from_secs(IDLE_TIMEOUT))
            .pool_max_idle_per_host(MAX_IDLE_PER_HOST)
            .build::<_, Body>(https);

        Self {
            url,
            http_client,
            cookies: HashMap::new(),
        }
    }

    fn make_uri(&self, path: &str, query: HashMap<&str, &str>) -> Uri {
        let query = if query.is_empty() {
            "".to_string()
        } else {
            let query = query
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<String>>()
                .join("&");
            format!("?{}", query)
        };

        let uri_str = format!("{}{}{}", self.url, path, query);
        Uri::from_str(&uri_str).unwrap()
    }

    pub(crate) async fn get<T: DeserializeOwned>(
        &self,
        path: &str,
        query: HashMap<&str, &str>,
        cookie: Option<&str>,
    ) -> Result<T, RequestError> {
        let mut req = Request::builder()
            .method("GET")
            .uri(self.make_uri(path, query))
            .body(Body::from(""))
            .unwrap();
        if let Some(cookie) = cookie {
            req.headers_mut()
                .insert(COOKIE, format!("fairOS-dfs={}", cookie).parse().unwrap());
        }

        let res = self
            .http_client
            .request(req)
            .await
            .map_err(|_| RequestError::CouldNotConnect)?;
        let status_ok = is_status_ok(res.status());
        let buf = hyper::body::to_bytes(res).await.unwrap();

        if status_ok {
            Ok(serde_json::from_slice(&buf).unwrap())
        } else {
            let res: MessageResponse = serde_json::from_slice(&buf).unwrap();
            Err(RequestError::Message(res.message))
        }
    }

    pub(crate) async fn post<T: DeserializeOwned>(
        &self,
        path: &str,
        body: Vec<u8>,
        cookie: Option<&str>,
    ) -> Result<(T, Option<String>), RequestError> {
        let mut req = Request::builder()
            .method("POST")
            .uri(self.make_uri(path, HashMap::new()))
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .unwrap();
        if let Some(cookie) = cookie {
            req.headers_mut()
                .insert(COOKIE, format!("fairOS-dfs={}", cookie).parse().unwrap());
        }

        let res = self
            .http_client
            .request(req)
            .await
            .map_err(|_| RequestError::CouldNotConnect)?;

        let cookie = if let Some(cookie) = res.headers().get(SET_COOKIE) {
            let cookie_str = cookie.to_str().unwrap().to_string();
            let mut split = cookie_str.split(";").next().unwrap().split("=");
            let name = split.next().unwrap();
            let value = split.next().unwrap();
            if name == "fairOS-dfs" {
                Some(value.to_string())
            } else {
                None
            }
        } else {
            None
        };

        let status_ok = is_status_ok(res.status());
        let buf = hyper::body::to_bytes(res).await.unwrap();

        if status_ok {
            let des = serde_json::from_slice(&buf).unwrap();
            Ok((des, cookie))
        } else {
            let res: MessageResponse = serde_json::from_slice(&buf).unwrap();
            Err(RequestError::Message(res.message))
        }
    }

    pub(crate) async fn delete<T: DeserializeOwned>(
        &self,
        path: &str,
        body: Vec<u8>,
        cookie: &str,
    ) -> Result<T, RequestError> {
        let req = Request::builder()
            .method("DELETE")
            .uri(self.make_uri(path, HashMap::new()))
            .header(CONTENT_TYPE, "application/json")
            .header(COOKIE, format!("fairOS-dfs={}", cookie))
            .body(Body::from(body))
            .unwrap();

        let res = self
            .http_client
            .request(req)
            .await
            .map_err(|_| RequestError::CouldNotConnect)?;
        let status_ok = is_status_ok(res.status());
        let buf = hyper::body::to_bytes(res).await.unwrap();

        if status_ok {
            Ok(serde_json::from_slice(&buf).unwrap())
        } else {
            let res: MessageResponse = serde_json::from_slice(&buf).unwrap();
            Err(RequestError::Message(res.message))
        }
    }

    pub(crate) fn cookie(&self, username: &str) -> Option<&str> {
        if let Some(cookie) = self.cookies.get(username) {
            Some(cookie.as_str())
        } else {
            None
        }
    }

    pub(crate) fn set_cookie(&mut self, username: &str, cookie: String) {
        self.cookies.insert(username.into(), cookie);
    }

    pub(crate) fn remove_cookie(&mut self, username: &str) {
        self.cookies.remove(username);
    }
}
