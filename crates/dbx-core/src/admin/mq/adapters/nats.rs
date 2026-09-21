//! Native NATS / JetStream adapter.
//!
//! This intentionally uses the NATS text protocol over Tokio instead of adding
//! another runtime dependency. DBX already ships Tokio/serde/base64 and its CI
//! builds with Cargo.lock in --locked mode, so keeping the adapter dependency-
//! free avoids perturbing every desktop release target.
//!
//! Mapping used by the generic MQ console:
//! - Topic        -> JetStream Stream
//! - Subscription -> JetStream Consumer
//! - Send message -> NATS Subject publish

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::timeout;
use uuid::Uuid;

use crate::mq::auth::MqAuth;
use crate::mq::config::MqAdminConfig;
use crate::mq::port::MessageQueueAdmin;
use crate::mq::types::*;

const DEFAULT_NATS_PORT: u16 = 4222;
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
struct NatsEndpoint {
    host: String,
    port: u16,
    display: String,
}

pub struct NatsAdmin {
    config: MqAdminConfig,
    endpoints: Vec<NatsEndpoint>,
}

impl NatsAdmin {
    pub async fn new(config: MqAdminConfig) -> Result<Self, String> {
        if config.tls_skip_verify {
            return Err("NATS TLS skip verification is not supported by the native adapter yet".to_string());
        }
        if config.socks_proxy.is_some() {
            return Err("NATS SOCKS proxy transport is not supported by the native adapter yet".to_string());
        }
        let endpoints = nats_endpoints(&config)?;
        Ok(Self { config, endpoints })
    }

    fn request_timeout(&self) -> Duration {
        self.config.rpc_timeout().unwrap_or(Duration::from_secs(3600))
    }

    async fn connect(&self) -> Result<NatsConnection, String> {
        let mut errors = Vec::new();
        for endpoint in &self.endpoints {
            match NatsConnection::connect(
                endpoint,
                &self.config.auth,
                self.config.connect_timeout(),
                self.request_timeout(),
            )
            .await
            {
                Ok(connection) => return Ok(connection),
                Err(error) => errors.push(format!("{}: {error}", endpoint.display)),
            }
        }
        Err(format!("Failed to connect to NATS: {}", errors.join("; ")))
    }

    async fn js_request(
        &self,
        connection: &mut NatsConnection,
        subject: &str,
        payload: Value,
    ) -> Result<Value, String> {
        let body = serde_json::to_vec(&payload).map_err(|error| format!("Failed to encode JetStream request: {error}"))?;
        let response = connection.request(subject, &body, &HashMap::new()).await?;
        let value: Value = serde_json::from_slice(&response)
            .map_err(|error| format!("Invalid JetStream response from {subject}: {error}"))?;
        ensure_jetstream_ok(value)
    }

    async fn js_request_raw(
        &self,
        connection: &mut NatsConnection,
        subject: &str,
        payload: Value,
    ) -> Result<Value, String> {
        let body = serde_json::to_vec(&payload).map_err(|error| format!("Failed to encode JetStream request: {error}"))?;
        let response = connection.request(subject, &body, &HashMap::new()).await?;
        serde_json::from_slice(&response).map_err(|error| format!("Invalid JetStream response from {subject}: {error}"))
    }

    async fn stream_info(&self, connection: &mut NatsConnection, stream: &str) -> Result<Value, String> {
        validate_resource_name(stream, "stream")?;
        self.js_request(connection, &format!("$JS.API.STREAM.INFO.{stream}"), json!({})).await
    }

    async fn try_stream_info(
        &self,
        connection: &mut NatsConnection,
        stream: &str,
    ) -> Result<Option<Value>, String> {
        if validate_resource_name(stream, "stream").is_err() {
            return Ok(None);
        }
        let value = self
            .js_request_raw(connection, &format!("$JS.API.STREAM.INFO.{stream}"), json!({}))
            .await?;
        if let Some((code, description)) = jetstream_error(&value) {
            if code == 404 {
                return Ok(None);
            }
            return Err(format!("JetStream error {code}: {description}"));
        }
        Ok(Some(value))
    }

    async fn list_stream_infos(&self, connection: &mut NatsConnection) -> Result<Vec<Value>, String> {
        let mut offset = 0_u64;
        let mut streams = Vec::new();
        loop {
            let value = self
                .js_request(connection, "$JS.API.STREAM.LIST", json!({ "offset": offset }))
                .await?;
            let page = value.get("streams").and_then(Value::as_array).cloned().unwrap_or_default();
            let total = value.get("total").and_then(Value::as_u64).unwrap_or(page.len() as u64);
            let page_len = page.len() as u64;
            streams.extend(page);
            if page_len == 0 || streams.len() as u64 >= total {
                break;
            }
            offset = offset.saturating_add(page_len);
        }
        Ok(streams)
    }

    async fn list_consumer_infos(
        &self,
        connection: &mut NatsConnection,
        stream: &str,
    ) -> Result<Vec<Value>, String> {
        validate_resource_name(stream, "stream")?;
        let mut offset = 0_u64;
        let mut consumers = Vec::new();
        loop {
            let value = self
                .js_request(
                    connection,
                    &format!("$JS.API.CONSUMER.LIST.{stream}"),
                    json!({ "offset": offset }),
                )
                .await?;
            let page = value.get("consumers").and_then(Value::as_array).cloned().unwrap_or_default();
            let total = value.get("total").and_then(Value::as_u64).unwrap_or(page.len() as u64);
            let page_len = page.len() as u64;
            consumers.extend(page);
            if page_len == 0 || consumers.len() as u64 >= total {
                break;
            }
            offset = offset.saturating_add(page_len);
        }
        Ok(consumers)
    }

    async fn consumer_info(
        &self,
        connection: &mut NatsConnection,
        stream: &str,
        consumer: &str,
    ) -> Result<Value, String> {
        validate_resource_name(stream, "stream")?;
        validate_resource_name(consumer, "consumer")?;
        self.js_request(
            connection,
            &format!("$JS.API.CONSUMER.INFO.{stream}.{consumer}"),
            json!({}),
        )
        .await
    }
}

fn nats_capabilities(jetstream: bool) -> MqCapabilities {
    MqCapabilities {
        supports_subscriptions: jetstream,
        supports_create_subscription: jetstream,
        supports_send_message: true,
        ..Default::default()
    }
}

fn nats_endpoints(config: &MqAdminConfig) -> Result<Vec<NatsEndpoint>, String> {
    if let Some(override_endpoint) = &config.connect_override {
        return Ok(vec![NatsEndpoint {
            host: override_endpoint.host.clone(),
            port: override_endpoint.port,
            display: format!("{}:{}", override_endpoint.host, override_endpoint.port),
        }]);
    }

    let mut values = Vec::new();
    match config.extra.get("servers") {
        Some(Value::String(value)) => values.extend(split_server_list(value)),
        Some(Value::Array(items)) => {
            values.extend(items.iter().filter_map(Value::as_str).flat_map(split_server_list));
        }
        Some(_) => return Err("NATS extra.servers must be a string or string array".to_string()),
        None => {}
    }
    if values.is_empty() && !config.admin_url.trim().is_empty() {
        values.extend(split_server_list(&config.admin_url));
    }
    if values.is_empty() {
        return Err("NATS server list is empty".to_string());
    }

    values.into_iter().map(|value| parse_nats_endpoint(&value)).collect()
}

fn split_server_list(value: &str) -> Vec<String> {
    value
        .split(|ch| matches!(ch, ',' | ';' | '\n' | '\r'))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect()
}

fn parse_nats_endpoint(value: &str) -> Result<NatsEndpoint, String> {
    let normalized = if value.contains("://") {
        value.to_string()
    } else {
        format!("nats://{value}")
    };
    let parsed = reqwest::Url::parse(&normalized).map_err(|error| format!("Invalid NATS server '{value}': {error}"))?;
    if parsed.scheme() != "nats" {
        return Err(format!(
            "NATS server '{}' uses unsupported scheme '{}'; the first native adapter release supports nats:// TCP endpoints",
            value,
            parsed.scheme()
        ));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err("Put NATS credentials in the DBX authentication fields instead of the server URL".to_string());
    }
    let host = parsed.host_str().ok_or_else(|| format!("NATS server '{value}' has no host"))?.to_string();
    let port = parsed.port().unwrap_or(DEFAULT_NATS_PORT);
    Ok(NatsEndpoint {
        host,
        port,
        display: value.to_string(),
    })
}

fn validate_resource_name(value: &str, kind: &str) -> Result<(), String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(format!("NATS {kind} name is empty"));
    }
    if value.chars().any(|ch| ch.is_whitespace() || matches!(ch, '.' | '*' | '>' | '/' | '\\')) {
        return Err(format!("NATS {kind} name '{value}' contains characters that cannot be used in a JetStream API token"));
    }
    Ok(())
}

fn validate_publish_subject(subject: &str) -> Result<(), String> {
    let subject = subject.trim();
    if subject.is_empty() {
        return Err("NATS publish subject is empty".to_string());
    }
    if subject.chars().any(char::is_whitespace) || subject.contains('*') || subject.contains('>') {
        return Err(format!("NATS publish subject '{subject}' is invalid"));
    }
    if subject.split('.').any(str::is_empty) {
        return Err(format!("NATS publish subject '{subject}' contains an empty token"));
    }
    Ok(())
}

fn subject_matches(pattern: &str, subject: &str) -> bool {
    let mut pattern_tokens = pattern.split('.');
    let mut subject_tokens = subject.split('.');
    loop {
        match (pattern_tokens.next(), subject_tokens.next()) {
            (Some(">"), _) => return true,
            (Some("*"), Some(_)) => {}
            (Some(left), Some(right)) if left == right => {}
            (None, None) => return true,
            _ => return false,
        }
    }
}

fn stream_publish_subject(info: &Value, stream_name: &str) -> Result<String, String> {
    let subjects = info
        .pointer("/config/subjects")
        .and_then(Value::as_array)
        .map(|items| items.iter().filter_map(Value::as_str).collect::<Vec<_>>())
        .unwrap_or_default();

    if subjects.iter().any(|pattern| subject_matches(pattern, stream_name)) {
        return Ok(stream_name.to_string());
    }
    if let Some(subject) = subjects.iter().copied().find(|subject| !subject.contains('*') && !subject.contains('>')) {
        return Ok(subject.to_string());
    }
    Err(format!(
        "JetStream stream '{stream_name}' only has wildcard subjects. Enter a concrete NATS subject in the message target field."
    ))
}

fn jetstream_error(value: &Value) -> Option<(u64, String)> {
    let error = value.get("error")?;
    Some((
        error.get("code").and_then(Value::as_u64).unwrap_or(500),
        error
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or("unknown JetStream error")
            .to_string(),
    ))
}

fn ensure_jetstream_ok(value: Value) -> Result<Value, String> {
    if let Some((code, description)) = jetstream_error(&value) {
        return Err(format!("JetStream error {code}: {description}"));
    }
    Ok(value)
}

fn stream_to_topic_info(value: &Value) -> TopicInfo {
    let name = value
        .pointer("/config/name")
        .and_then(Value::as_str)
        .or_else(|| value.get("name").and_then(Value::as_str))
        .unwrap_or_default()
        .to_string();
    TopicInfo {
        name: name.clone(),
        short_name: name,
        persistent: true,
        message_count: value.pointer("/state/messages").and_then(Value::as_i64),
        consumer_count: value.pointer("/state/consumer_count").and_then(Value::as_i64),
        state: Some("stream".to_string()),
        ..Default::default()
    }
}

fn consumer_to_subscription(value: &Value) -> SubscriptionInfo {
    let name = value
        .get("name")
        .and_then(Value::as_str)
        .or_else(|| value.pointer("/config/durable_name").and_then(Value::as_str))
        .unwrap_or_default()
        .to_string();
    let pending = value.get("num_pending").and_then(Value::as_i64).unwrap_or(0);
    let ack_pending = value.get("num_ack_pending").and_then(Value::as_i64).unwrap_or(0);
    let push = value.pointer("/config/deliver_subject").and_then(Value::as_str).is_some();
    SubscriptionInfo {
        name,
        sub_type: if push { "push" } else { "pull" }.to_string(),
        msg_backlog: pending.saturating_add(ack_pending),
        backlog_unavailable: Some(false),
        ..Default::default()
    }
}

struct NatsConnection {
    io: BufReader<TcpStream>,
    info: Value,
    next_sid: u64,
    read_timeout: Duration,
}

impl NatsConnection {
    async fn connect(
        endpoint: &NatsEndpoint,
        auth: &MqAuth,
        connect_timeout: Duration,
        read_timeout: Duration,
    ) -> Result<Self, String> {
        let stream = timeout(connect_timeout, TcpStream::connect((endpoint.host.as_str(), endpoint.port)))
            .await
            .map_err(|_| format!("connect timed out after {}s", connect_timeout.as_secs()))?
            .map_err(|error| format!("TCP connect failed: {error}"))?;
        stream.set_nodelay(true).map_err(|error| format!("Failed to configure NATS socket: {error}"))?;

        let mut connection = Self {
            io: BufReader::new(stream),
            info: Value::Null,
            next_sid: 0,
            read_timeout: if read_timeout.is_zero() { DEFAULT_REQUEST_TIMEOUT } else { read_timeout },
        };

        let first = connection.read_line().await?;
        let info_text = first
            .strip_prefix("INFO ")
            .ok_or_else(|| format!("Expected NATS INFO greeting, received: {first}"))?;
        connection.info =
            serde_json::from_str(info_text).map_err(|error| format!("Invalid NATS INFO greeting: {error}"))?;

        if connection.info.get("tls_required").and_then(Value::as_bool).unwrap_or(false) {
            return Err("This NATS server requires TLS; TLS endpoints are not supported by the first native adapter release".to_string());
        }

        let mut connect = json!({
            "verbose": false,
            "pedantic": false,
            "tls_required": false,
            "name": "DBX",
            "lang": "rust",
            "version": env!("CARGO_PKG_VERSION"),
            "protocol": 1,
            "echo": true,
            "headers": true,
            "no_responders": true
        });
        match auth {
            MqAuth::None => {}
            MqAuth::Token { token } => {
                connect["auth_token"] = Value::String(token.clone());
            }
            MqAuth::Basic { username, password } => {
                connect["user"] = Value::String(username.clone());
                connect["pass"] = Value::String(password.clone());
            }
            MqAuth::ApiKey { .. } | MqAuth::OAuth2 { .. } => {
                return Err("NATS supports DBX auth modes none, token, and basic in this adapter release".to_string());
            }
        }

        connection
            .write_all(format!("CONNECT {}\r\nPING\r\n", connect).as_bytes())
            .await?;
        connection.wait_for_pong().await?;
        Ok(connection)
    }

    async fn read_line(&mut self) -> Result<String, String> {
        let mut line = String::new();
        let count = timeout(self.read_timeout, self.io.read_line(&mut line))
            .await
            .map_err(|_| format!("NATS read timed out after {}s", self.read_timeout.as_secs()))?
            .map_err(|error| format!("NATS read failed: {error}"))?;
        if count == 0 {
            return Err("NATS connection closed by server".to_string());
        }
        Ok(line.trim_end_matches(['\r', '\n']).to_string())
    }

    async fn write_all(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.io
            .get_mut()
            .write_all(bytes)
            .await
            .map_err(|error| format!("NATS write failed: {error}"))?;
        self.io
            .get_mut()
            .flush()
            .await
            .map_err(|error| format!("NATS flush failed: {error}"))
    }

    async fn write_control(&mut self, value: &str) -> Result<(), String> {
        self.write_all(value.as_bytes()).await
    }

    async fn wait_for_pong(&mut self) -> Result<(), String> {
        loop {
            let line = self.read_line().await?;
            if line == "PONG" {
                return Ok(());
            }
            if line == "PING" {
                self.write_control("PONG\r\n").await?;
                continue;
            }
            if let Some(info) = line.strip_prefix("INFO ") {
                self.info = serde_json::from_str(info).map_err(|error| format!("Invalid async NATS INFO: {error}"))?;
                continue;
            }
            if line == "+OK" {
                continue;
            }
            if let Some(error) = line.strip_prefix("-ERR") {
                return Err(format!("NATS server error: {}", error.trim().trim_matches('\'')));
            }
        }
    }

    async fn flush(&mut self) -> Result<(), String> {
        self.write_control("PING\r\n").await?;
        self.wait_for_pong().await
    }

    async fn request(
        &mut self,
        subject: &str,
        payload: &[u8],
        headers: &HashMap<String, String>,
    ) -> Result<Vec<u8>, String> {
        validate_publish_subject(subject)?;
        self.next_sid = self.next_sid.saturating_add(1);
        let sid = self.next_sid;
        let inbox = format!("_INBOX.DBX.{}", Uuid::new_v4().simple());

        let mut command = format!("SUB {inbox} {sid}\r\nUNSUB {sid} 1\r\n").into_bytes();
        append_publish_command(&mut command, subject, Some(&inbox), payload, headers)?;
        self.write_all(&command).await?;

        loop {
            let line = self.read_line().await?;
            if line == "PING" {
                self.write_control("PONG\r\n").await?;
                continue;
            }
            if line == "+OK" {
                continue;
            }
            if let Some(info) = line.strip_prefix("INFO ") {
                self.info = serde_json::from_str(info).map_err(|error| format!("Invalid async NATS INFO: {error}"))?;
                continue;
            }
            if let Some(error) = line.strip_prefix("-ERR") {
                return Err(format!("NATS server error: {}", error.trim().trim_matches('\'')));
            }
            if line.starts_with("MSG ") {
                let fields: Vec<&str> = line.split_whitespace().collect();
                if fields.len() != 4 && fields.len() != 5 {
                    return Err(format!("Invalid NATS MSG header: {line}"));
                }
                let message_sid = fields[2].parse::<u64>().map_err(|_| format!("Invalid NATS SID in: {line}"))?;
                let size = fields.last().and_then(|value| value.parse::<usize>().ok()).ok_or_else(|| {
                    format!("Invalid NATS message size in: {line}")
                })?;
                let body = self.read_frame(size).await?;
                if message_sid == sid {
                    return Ok(body);
                }
                continue;
            }
            if line.starts_with("HMSG ") {
                let fields: Vec<&str> = line.split_whitespace().collect();
                if fields.len() != 5 && fields.len() != 6 {
                    return Err(format!("Invalid NATS HMSG header: {line}"));
                }
                let message_sid = fields[2].parse::<u64>().map_err(|_| format!("Invalid NATS SID in: {line}"))?;
                let header_len = fields[fields.len() - 2]
                    .parse::<usize>()
                    .map_err(|_| format!("Invalid NATS header size in: {line}"))?;
                let total_len = fields[fields.len() - 1]
                    .parse::<usize>()
                    .map_err(|_| format!("Invalid NATS total size in: {line}"))?;
                let frame = self.read_frame(total_len).await?;
                if message_sid != sid {
                    continue;
                }
                let header_bytes = frame.get(..header_len).ok_or("Invalid NATS HMSG header length")?;
                let header_text = String::from_utf8_lossy(header_bytes);
                if header_text.starts_with("NATS/1.0 503") {
                    return Err("NATS request has no responders (JetStream may be disabled)".to_string());
                }
                return Ok(frame.get(header_len..).unwrap_or_default().to_vec());
            }
        }
    }

    async fn publish(
        &mut self,
        subject: &str,
        payload: &[u8],
        headers: &HashMap<String, String>,
    ) -> Result<(), String> {
        validate_publish_subject(subject)?;
        let mut command = Vec::new();
        append_publish_command(&mut command, subject, None, payload, headers)?;
        self.write_all(&command).await?;
        self.flush().await
    }

    async fn read_frame(&mut self, size: usize) -> Result<Vec<u8>, String> {
        let mut body = vec![0_u8; size];
        timeout(self.read_timeout, self.io.read_exact(&mut body))
            .await
            .map_err(|_| format!("NATS payload read timed out after {}s", self.read_timeout.as_secs()))?
            .map_err(|error| format!("NATS payload read failed: {error}"))?;
        let mut crlf = [0_u8; 2];
        self.io
            .read_exact(&mut crlf)
            .await
            .map_err(|error| format!("NATS payload terminator read failed: {error}"))?;
        if crlf != *b"\r\n" {
            return Err("Invalid NATS payload terminator".to_string());
        }
        Ok(body)
    }
}

fn append_publish_command(
    output: &mut Vec<u8>,
    subject: &str,
    reply: Option<&str>,
    payload: &[u8],
    headers: &HashMap<String, String>,
) -> Result<(), String> {
    if headers.is_empty() {
        match reply {
            Some(reply) => output.extend_from_slice(format!("PUB {subject} {reply} {}\r\n", payload.len()).as_bytes()),
            None => output.extend_from_slice(format!("PUB {subject} {}\r\n", payload.len()).as_bytes()),
        }
        output.extend_from_slice(payload);
        output.extend_from_slice(b"\r\n");
        return Ok(());
    }

    let mut header_block = b"NATS/1.0\r\n".to_vec();
    for (name, value) in headers {
        if name.trim().is_empty()
            || name.contains(':')
            || name.contains('\r')
            || name.contains('\n')
            || value.contains('\r')
            || value.contains('\n')
        {
            return Err(format!("Invalid NATS header: {name}"));
        }
        header_block.extend_from_slice(format!("{}: {}\r\n", name.trim(), value).as_bytes());
    }
    header_block.extend_from_slice(b"\r\n");
    let total_len = header_block.len().saturating_add(payload.len());
    match reply {
        Some(reply) => output.extend_from_slice(
            format!("HPUB {subject} {reply} {} {total_len}\r\n", header_block.len()).as_bytes(),
        ),
        None => output.extend_from_slice(format!("HPUB {subject} {} {total_len}\r\n", header_block.len()).as_bytes()),
    }
    output.extend_from_slice(&header_block);
    output.extend_from_slice(payload);
    output.extend_from_slice(b"\r\n");
    Ok(())
}

#[async_trait]
impl MessageQueueAdmin for NatsAdmin {
    fn capabilities(&self) -> MqCapabilities {
        nats_capabilities(true)
    }

    fn system_kind(&self) -> MqSystemKind {
        MqSystemKind::Nats
    }

    async fn test_connection(&self) -> Result<MqClusterInfo, String> {
        let mut connection = self.connect().await?;
        let info = connection.info.clone();
        let jetstream_info = match self.js_request_raw(&mut connection, "$JS.API.INFO", json!({})).await {
            Ok(value) if jetstream_error(&value).is_none() => Some(value),
            _ => None,
        };
        let jetstream = jetstream_info.is_some();
        Ok(MqClusterInfo {
            system_kind: MqSystemKind::Nats,
            server_version: info.get("version").and_then(Value::as_str).map(str::to_string),
            resolved_profile: if jetstream { "nats-core+jetstream" } else { "nats-core" }.to_string(),
            version_detection: "probed".to_string(),
            capabilities: nats_capabilities(jetstream),
            extra: json!({
                "server": info,
                "jetstream": jetstream,
                "jetstreamAccount": jetstream_info,
            }),
        })
    }

    async fn list_tenants(&self) -> Result<Vec<TenantInfo>, String> {
        Ok(Vec::new())
    }

    async fn get_tenant(&self, _name: &str) -> Result<TenantInfo, String> {
        Err("NATS does not use the DBX tenant model".to_string())
    }

    async fn create_tenant(&self, _name: &str, _cfg: TenantConfig) -> Result<(), String> {
        Err("NATS tenant management is not supported".to_string())
    }

    async fn update_tenant(&self, _name: &str, _cfg: TenantConfig) -> Result<(), String> {
        Err("NATS tenant management is not supported".to_string())
    }

    async fn delete_tenant(&self, _name: &str, _force: bool) -> Result<(), String> {
        Err("NATS tenant management is not supported".to_string())
    }

    async fn list_namespaces(&self, _tenant: &str) -> Result<Vec<NamespaceInfo>, String> {
        Ok(Vec::new())
    }

    async fn create_namespace(&self, _ns: &NamespaceRef, _cfg: NamespaceConfig) -> Result<(), String> {
        Err("NATS namespace management is not supported".to_string())
    }

    async fn delete_namespace(&self, _ns: &NamespaceRef, _force: bool) -> Result<(), String> {
        Err("NATS namespace management is not supported".to_string())
    }

    async fn get_namespace_policies(&self, _ns: &NamespaceRef) -> Result<Value, String> {
        Err("NATS namespace policies are not supported".to_string())
    }

    async fn list_topics(&self, _ns: &NamespaceRef, _opts: ListTopicsOpts) -> Result<Vec<TopicInfo>, String> {
        let mut connection = self.connect().await?;
        Ok(self
            .list_stream_infos(&mut connection)
            .await?
            .iter()
            .map(stream_to_topic_info)
            .collect())
    }

    async fn create_topic(&self, topic: &TopicRef, partitions: Option<u32>) -> Result<(), String> {
        if partitions.unwrap_or(1) > 1 {
            return Err("NATS JetStream streams are not partitioned".to_string());
        }
        let name = topic.topic.trim();
        validate_resource_name(name, "stream")?;
        let mut connection = self.connect().await?;
        self.js_request(
            &mut connection,
            &format!("$JS.API.STREAM.CREATE.{name}"),
            json!({ "name": name, "subjects": [name] }),
        )
        .await?;
        Ok(())
    }

    async fn delete_topic(&self, topic: &TopicRef, _force: bool) -> Result<(), String> {
        let name = topic.topic.trim();
        validate_resource_name(name, "stream")?;
        let mut connection = self.connect().await?;
        self.js_request(
            &mut connection,
            &format!("$JS.API.STREAM.DELETE.{name}"),
            json!({}),
        )
        .await?;
        Ok(())
    }

    async fn update_partitions(&self, _topic: &TopicRef, _partitions: u32) -> Result<(), String> {
        Err("NATS JetStream streams are not partitioned".to_string())
    }

    async fn get_topic_stats(&self, topic: &TopicRef) -> Result<TopicStats, String> {
        let mut connection = self.connect().await?;
        let info = self.stream_info(&mut connection, topic.topic.trim()).await?;
        let messages = info.pointer("/state/messages").and_then(Value::as_i64).unwrap_or(0);
        let bytes = info.pointer("/state/bytes").and_then(Value::as_i64).unwrap_or(0);
        let consumers = info.pointer("/state/consumer_count").and_then(Value::as_u64).unwrap_or(0) as u32;
        Ok(TopicStats {
            storage_size: bytes,
            backlog_size: bytes,
            msg_in_counter: messages,
            subscription_count: consumers,
            rates_unavailable: true,
            raw: info,
            ..Default::default()
        })
    }

    async fn get_topic_internal_stats(&self, topic: &TopicRef) -> Result<Value, String> {
        let mut connection = self.connect().await?;
        self.stream_info(&mut connection, topic.topic.trim()).await
    }

    async fn list_subscriptions(&self, topic: &TopicRef) -> Result<Vec<SubscriptionInfo>, String> {
        let stream = topic.topic.trim();
        let mut connection = self.connect().await?;
        Ok(self
            .list_consumer_infos(&mut connection, stream)
            .await?
            .iter()
            .map(consumer_to_subscription)
            .collect())
    }

    async fn create_subscription(&self, topic: &TopicRef, sub: &str, pos: ResetPosition) -> Result<(), String> {
        let stream = topic.topic.trim();
        validate_resource_name(stream, "stream")?;
        validate_resource_name(sub, "consumer")?;
        let deliver_policy = match pos {
            ResetPosition::Earliest => "all",
            ResetPosition::Latest => "new",
            _ => return Err("NATS consumer creation currently supports earliest/latest start positions".to_string()),
        };
        let mut connection = self.connect().await?;
        self.js_request(
            &mut connection,
            &format!("$JS.API.CONSUMER.CREATE.{stream}.{sub}"),
            json!({
                "stream_name": stream,
                "config": {
                    "name": sub,
                    "durable_name": sub,
                    "deliver_policy": deliver_policy,
                    "ack_policy": "explicit",
                    "replay_policy": "instant"
                }
            }),
        )
        .await?;
        Ok(())
    }

    async fn delete_subscription(&self, topic: &TopicRef, sub: &str, _force: bool) -> Result<(), String> {
        let stream = topic.topic.trim();
        validate_resource_name(stream, "stream")?;
        validate_resource_name(sub, "consumer")?;
        let mut connection = self.connect().await?;
        self.js_request(
            &mut connection,
            &format!("$JS.API.CONSUMER.DELETE.{stream}.{sub}"),
            json!({ "stream_name": stream, "name": sub }),
        )
        .await?;
        Ok(())
    }

    async fn skip_messages(&self, _topic: &TopicRef, _sub: &str, _count: SkipCount) -> Result<(), String> {
        Err("NATS skip messages is not supported in this adapter release".to_string())
    }

    async fn reset_cursor(&self, _topic: &TopicRef, _sub: &str, _pos: ResetPosition) -> Result<(), String> {
        Err("NATS consumer cursor reset is not supported in this adapter release".to_string())
    }

    async fn clear_backlog(&self, _topic: &TopicRef, _sub: &str) -> Result<(), String> {
        Err("NATS consumer backlog clearing is not supported in this adapter release".to_string())
    }

    async fn peek_messages(
        &self,
        _topic: &TopicRef,
        _sub: &str,
        _count: u32,
        _options: PeekMessagesOptions,
    ) -> Result<PeekMessagesResult, String> {
        Err("NATS message peeking is not supported in this adapter release".to_string())
    }

    async fn expire_messages(&self, _topic: &TopicRef, _sub: &str, _expire_seconds: i64) -> Result<(), String> {
        Err("NATS consumer message expiry is not supported in this adapter release".to_string())
    }

    async fn list_producers(&self, _topic: &TopicRef) -> Result<Vec<ProducerInfo>, String> {
        Ok(Vec::new())
    }

    async fn list_consumers(&self, topic: &TopicRef, sub: &str) -> Result<Vec<ConsumerInfo>, String> {
        let mut connection = self.connect().await?;
        let info = self.consumer_info(&mut connection, topic.topic.trim(), sub).await?;
        Ok(vec![ConsumerInfo {
            consumer_name: sub.to_string(),
            available_permits: info.get("num_pending").and_then(Value::as_i64).unwrap_or(0),
            client_version: "NATS JetStream".to_string(),
            ..Default::default()
        }])
    }

    async fn unload_topic(&self, _topic: &TopicRef) -> Result<(), String> {
        Err("NATS stream unload is not supported".to_string())
    }

    async fn set_publish_rate(&self, _scope: &PolicyScope, _rate: PublishRate) -> Result<(), String> {
        Err("NATS publish rate policy is not supported".to_string())
    }

    async fn set_dispatch_rate(&self, _scope: &PolicyScope, _rate: DispatchRate) -> Result<(), String> {
        Err("NATS dispatch rate policy is not supported".to_string())
    }

    async fn set_subscribe_rate(&self, _scope: &PolicyScope, _rate: SubscribeRate) -> Result<(), String> {
        Err("NATS subscribe rate policy is not supported".to_string())
    }

    async fn set_backlog_quota(&self, _scope: &PolicyScope, _quota: BacklogQuota) -> Result<(), String> {
        Err("NATS backlog quota policy is not supported".to_string())
    }

    async fn set_retention(&self, _scope: &PolicyScope, _retention: RetentionPolicy) -> Result<(), String> {
        Err("NATS retention editing is not supported in this adapter release".to_string())
    }

    async fn get_effective_policies(&self, _scope: &PolicyScope) -> Result<Value, String> {
        Err("NATS policy inspection is not supported in this adapter release".to_string())
    }

    async fn grant_permission(
        &self,
        _scope: &PolicyScope,
        _role: &str,
        _actions: Vec<AuthAction>,
    ) -> Result<(), String> {
        Err("NATS account permission editing is not supported".to_string())
    }

    async fn revoke_permission(&self, _scope: &PolicyScope, _role: &str) -> Result<(), String> {
        Err("NATS account permission editing is not supported".to_string())
    }

    async fn list_permissions(&self, _scope: &PolicyScope) -> Result<PermissionMap, String> {
        Err("NATS account permission listing is not supported".to_string())
    }

    async fn get_backlog(&self, topic: &TopicRef, sub: Option<&str>) -> Result<BacklogStats, String> {
        let stream = topic.topic.trim();
        let mut connection = self.connect().await?;
        if let Some(consumer) = sub {
            let info = self.consumer_info(&mut connection, stream, consumer).await?;
            let pending = info.get("num_pending").and_then(Value::as_i64).unwrap_or(0);
            let ack_pending = info.get("num_ack_pending").and_then(Value::as_i64).unwrap_or(0);
            return Ok(BacklogStats {
                msg_backlog: pending.saturating_add(ack_pending),
                backlog_size: 0,
                ..Default::default()
            });
        }
        let info = self.stream_info(&mut connection, stream).await?;
        Ok(BacklogStats {
            msg_backlog: info.pointer("/state/messages").and_then(Value::as_i64).unwrap_or(0),
            backlog_size: info.pointer("/state/bytes").and_then(Value::as_i64).unwrap_or(0),
            ..Default::default()
        })
    }

    async fn get_cluster_info(&self) -> Result<ClusterInfo, String> {
        let connection = self.connect().await?;
        let info = connection.info;
        let host = info.get("host").and_then(Value::as_str).unwrap_or_default().to_string();
        let port = info.get("port").and_then(Value::as_i64).unwrap_or(DEFAULT_NATS_PORT as i64) as i32;
        let name = info
            .get("server_name")
            .and_then(Value::as_str)
            .or_else(|| info.get("server_id").and_then(Value::as_str))
            .map(str::to_string);
        Ok(ClusterInfo {
            cluster_id: info.get("cluster").and_then(Value::as_str).map(str::to_string),
            broker_count: 1,
            brokers: vec![BrokerNode {
                id: 0,
                host,
                port,
                broker_name: name,
                role: Some("server".to_string()),
                ..Default::default()
            }],
            raw: info,
            ..Default::default()
        })
    }

    async fn raw_request(&self, _req: MqRawRequest) -> Result<MqRawResponse, String> {
        Err("NATS raw admin requests are not exposed by this adapter".to_string())
    }

    async fn send_message(&self, req: SendMessageRequest) -> Result<SendMessageResponse, String> {
        let payload = base64::engine::general_purpose::STANDARD
            .decode(req.payload_base64.as_bytes())
            .map_err(|error| format!("Invalid base64 NATS payload: {error}"))?;
        let requested = req.topic.trim();
        validate_publish_subject(requested)?;
        let mut connection = self.connect().await?;

        if let Some(stream_info) = self.try_stream_info(&mut connection, requested).await? {
            let subject = stream_publish_subject(&stream_info, requested)?;
            validate_publish_subject(&subject)?;
            let ack = connection.request(&subject, &payload, &req.headers).await?;
            let ack: Value =
                serde_json::from_slice(&ack).map_err(|error| format!("Invalid JetStream publish acknowledgement: {error}"))?;
            let ack = ensure_jetstream_ok(ack)?;
            return Ok(SendMessageResponse {
                topic: subject,
                partition: 0,
                offset: ack.get("seq").and_then(Value::as_i64).unwrap_or(0),
                timestamp: None,
            });
        }

        connection.publish(requested, &payload, &req.headers).await?;
        Ok(SendMessageResponse {
            topic: requested.to_string(),
            partition: 0,
            offset: 0,
            timestamp: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_server_lists_and_default_port() {
        let endpoint = parse_nats_endpoint("127.0.0.1").expect("endpoint");
        assert_eq!(endpoint.host, "127.0.0.1");
        assert_eq!(endpoint.port, 4222);
        assert_eq!(split_server_list("a:4222, b:4223\nc:4224").len(), 3);
    }

    #[test]
    fn subject_matching_supports_nats_wildcards() {
        assert!(subject_matches("device.*.state", "device.a.state"));
        assert!(subject_matches("device.>", "device.a.state"));
        assert!(!subject_matches("device.*.state", "device.a.b.state"));
    }

    #[test]
    fn stream_publish_target_prefers_a_concrete_subject() {
        let info = json!({
            "config": {
                "name": "EVENTS",
                "subjects": ["events.>", "events.direct"]
            }
        });
        assert_eq!(stream_publish_subject(&info, "EVENTS").unwrap(), "events.direct");
    }
}
