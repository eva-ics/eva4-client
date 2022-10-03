use busrt::{
    ipc,
    rpc::{self, Rpc},
    QoS,
};
use eva_common::events::NodeInfo;
use eva_common::payload::{pack, unpack};
use eva_common::prelude::*;
use hyper::{client::HttpConnector, Body, Method, Request};
use hyper_tls::HttpsConnector;
use rjrpc::{JsonRpcRequest, JsonRpcResponse};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub type NodeMap = BTreeMap<String, String>;

pub type HttpClient = hyper::Client<HttpsConnector<HttpConnector>>;

static CLIENT_ITERATION: atomic::AtomicUsize = atomic::AtomicUsize::new(1);
const CT_HEADER: &str = "application/msgpack";

#[derive(Deserialize)]
pub struct SystemInfo {
    pub system_name: String,
    pub active: bool,
    #[serde(flatten)]
    pub ver: VersionInfo,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug)]
pub struct VersionInfo {
    pub build: u64,
    pub version: String,
}

impl fmt::Display for VersionInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.build, self.version)
    }
}

impl From<NodeInfo> for VersionInfo {
    fn from(ni: NodeInfo) -> Self {
        Self {
            build: ni.build,
            version: ni.version,
        }
    }
}

#[inline]
fn parse_major(ver: &str) -> EResult<u16> {
    ver.split('.').next().unwrap().parse().map_err(Into::into)
}

impl VersionInfo {
    #[inline]
    pub fn major_matches(&self, ver: &str) -> EResult<bool> {
        Ok(parse_major(ver)? == self.major()?)
    }
    #[inline]
    pub fn major(&self) -> EResult<u16> {
        parse_major(&self.version)
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct EvaCloudClient {
    system_name: String,
    client: EvaClient,
    node_map: NodeMap,
}

impl EvaCloudClient {
    pub fn new(system_name: &str, client: EvaClient, node_map: NodeMap) -> Self {
        Self {
            system_name: system_name.to_owned(),
            client,
            node_map,
        }
    }
    pub async fn get_system_info(&self, node: &str) -> EResult<SystemInfo> {
        let info: SystemInfo = self.call(node, "eva.core", "test", None).await?;
        Ok(info)
    }
    pub async fn call<T>(
        &self,
        node: &str,
        target: &str,
        method: &str,
        params: Option<Value>,
    ) -> EResult<T>
    where
        T: DeserializeOwned,
    {
        if node == ".local" || node == self.system_name {
            self.client.call(target, method, params).await
        } else {
            let mut repl_params: BTreeMap<Value, Value> = if let Some(p) = params {
                BTreeMap::deserialize(p).map_err(Error::invalid_data)?
            } else {
                BTreeMap::new()
            };
            repl_params.insert(
                Value::String("node".to_owned()),
                Value::String(node.to_owned()),
            );
            self.client
                .call(
                    self.node_map.get(node).ok_or_else(|| {
                        Error::failed(format!("no replication service mapped for {}", node))
                    })?,
                    &format!("bus::{}::{}", target, method),
                    Some(to_value(repl_params)?),
                )
                .await
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    credentials: Option<(String, String)>,
    timeout: Duration,
}

impl Config {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }
    #[inline]
    pub fn credentials(mut self, login: &str, password: &str) -> Self {
        self.credentials = Some((login.to_owned(), password.to_owned()));
        self
    }
    #[inline]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

impl Default for Config {
    #[inline]
    fn default() -> Self {
        Self {
            credentials: None,
            timeout: eva_common::DEFAULT_TIMEOUT,
        }
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct EvaClient {
    name: String,
    client: ClientKind,
    config: Config,
    token: Mutex<Option<Arc<String>>>,
    path: String,
    request_id: atomic::AtomicU32,
}

impl EvaClient {
    pub async fn connect(path: &str, base_name: &str, config: Config) -> EResult<Self> {
        if path.starts_with("http://") || path.starts_with("https://") {
            let https = HttpsConnector::new();
            let http_client: hyper::Client<_> = hyper::Client::builder()
                .pool_idle_timeout(config.timeout)
                .build(https);
            let cl = Self {
                name: base_name.to_owned(),
                client: ClientKind::Http(http_client),
                config,
                token: <_>::default(),
                path: path.to_owned(),
                request_id: atomic::AtomicU32::new(0),
            };
            if let ClientKind::Http(ref client) = cl.client {
                cl.http_login(client).await?;
            }
            Ok(cl)
        } else {
            let name = format!(
                "{}.{}.{}",
                base_name,
                std::process::id(),
                CLIENT_ITERATION.fetch_add(1, atomic::Ordering::SeqCst)
            );
            let bus = tokio::time::timeout(
                config.timeout,
                ipc::Client::connect(&ipc::Config::new(path, &name)),
            )
            .await??;
            let rpc = rpc::RpcClient::new(bus, rpc::DummyHandlers {});
            Ok(Self {
                name,
                client: ClientKind::Bus(rpc),
                config,
                token: <_>::default(),
                path: path.to_owned(),
                request_id: atomic::AtomicU32::new(0),
            })
        }
    }
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }
    pub async fn get_system_info(&self) -> EResult<SystemInfo> {
        let info: SystemInfo = self.call("eva.core", "test", None).await?;
        Ok(info)
    }
    async fn http_login(&self, client: &HttpClient) -> EResult<Arc<String>> {
        #[derive(Serialize)]
        struct LoginParams<'a> {
            u: &'a str,
            p: &'a str,
        }
        #[derive(Deserialize)]
        struct LoginPayload {
            token: String,
        }
        if let Some(ref creds) = self.config.credentials {
            let p: LoginPayload = self
                .safe_http_call(
                    client,
                    None,
                    None,
                    "login",
                    Some(to_value(LoginParams {
                        u: &creds.0,
                        p: &creds.1,
                    })?),
                )
                .await?;
            let token = Arc::new(p.token);
            self.token.lock().unwrap().replace(token.clone());
            Ok(token)
        } else {
            Err(Error::access("no credentials set"))
        }
    }
    /// # Panics
    ///
    /// Will panic if token mutex is poisoned
    pub async fn call<T>(&self, target: &str, method: &str, params: Option<Value>) -> EResult<T>
    where
        T: DeserializeOwned,
    {
        match self.client {
            ClientKind::Bus(ref c) => {
                let payload: busrt::borrow::Cow = if let Some(ref p) = params {
                    pack(p)?.into()
                } else {
                    busrt::empty_payload!()
                };
                let res = tokio::time::timeout(
                    self.config.timeout,
                    c.call(target, method, payload, QoS::Processed),
                )
                .await??;
                let result = res.payload();
                if result.is_empty() {
                    Ok(T::deserialize(Value::Unit)?)
                } else {
                    Ok(unpack(result)?)
                }
            }
            ClientKind::Http(ref client) => {
                let to: Option<Arc<String>> = self.token.lock().unwrap().clone();
                if let Some(token) = to {
                    match self
                        .safe_http_call(client, Some(&token), Some(target), method, params.clone())
                        .await
                    {
                        Err(e)
                            if e.kind() == ErrorKind::AccessDenied
                                && e.message().map_or(false, |m| m == "invalid token") =>
                        {
                            // repeat request with new token
                            let token = self.http_login(client).await?;
                            self.safe_http_call(client, Some(&token), Some(target), method, params)
                                .await
                        }
                        res => res,
                    }
                } else {
                    let token = self.http_login(client).await?;
                    self.safe_http_call(client, Some(&token), Some(target), method, params)
                        .await
                }
            }
        }
    }
    async fn safe_http_call<T>(
        &self,
        client: &HttpClient,
        token: Option<&str>,
        target: Option<&str>,
        method: &str,
        params: Option<Value>,
    ) -> EResult<T>
    where
        T: DeserializeOwned,
    {
        tokio::time::timeout(
            self.config.timeout,
            self.http_call(client, token, target, method, params),
        )
        .await?
    }
    async fn http_call<T>(
        &self,
        client: &HttpClient,
        token: Option<&str>,
        target: Option<&str>,
        method: &str,
        params: Option<Value>,
    ) -> EResult<T>
    where
        T: DeserializeOwned,
    {
        macro_rules! params_map {
            ($map: expr, $token: expr) => {{
                $map.insert(
                    Value::String("k".to_owned()),
                    Value::String($token.to_owned()),
                );
                Some(Value::Map($map))
            }};
        }
        let id = self.request_id.fetch_add(1, atomic::Ordering::SeqCst);
        let bus_method = target.map(|tgt| format!("bus::{tgt}::{method}"));
        let request = JsonRpcRequest::new(
            Some(Value::U32(id)),
            if let Some(ref m) = bus_method {
                m
            } else {
                method
            },
            if let Some(tk) = token {
                if let Some(par) = params {
                    let mut p_map: BTreeMap<Value, Value> = BTreeMap::deserialize(par)?;
                    params_map!(p_map, tk)
                } else {
                    let mut p_map = BTreeMap::new();
                    params_map!(p_map, tk)
                }
            } else {
                params
            },
            rjrpc::Encoding::MsgPack,
        );
        let http_request = Request::builder()
            .method(Method::POST)
            .header(hyper::header::CONTENT_TYPE, CT_HEADER.to_owned())
            .uri(&self.path)
            .body(Body::from(request.pack().map_err(Error::invalid_data)?))
            .map_err(Error::io)?;
        let http_res = client.request(http_request).await.map_err(Error::io)?;
        let http_res_body = hyper::body::to_bytes(http_res).await.map_err(Error::io)?;
        let res = JsonRpcResponse::unpack(&http_res_body, rjrpc::Encoding::MsgPack)
            .map_err(Error::invalid_data)?;
        if u32::try_from(res.id)? == id {
            if let Some(err) = res.error {
                Err(Error::newc(err.code.into(), err.message))
            } else if let Some(result) = res.result {
                Ok(T::deserialize(result).map_err(Error::invalid_data)?)
            } else {
                Ok(T::deserialize(Value::Unit).map_err(Error::invalid_data)?)
            }
        } else {
            Err(Error::io("invalid JRPC response: id mismatch"))
        }
    }
}

enum ClientKind {
    Bus(rpc::RpcClient),
    Http(HttpClient),
}
