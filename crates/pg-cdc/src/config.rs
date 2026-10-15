use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use secrecy::SecretString;
use serde::Deserialize;
use serde::de::{self, Deserializer, Visitor};
use serde_json::{Map, Value};

use crate::leader::POLL_INTERVAL;
use crate::order::OrderBy;

pub const ENV_PREFIX: &str = "PGCDC__";
pub const DEFAULT_LANES: usize = 64;
pub const MAX_LANES: usize = 4096;
pub const DEFAULT_MAX_INFLIGHT_BYTES: u64 = 256 << 20;
pub const DEFAULT_SINK_CONCURRENCY: usize = 32;
pub const MAX_ATTRIBUTE_COLUMNS: usize = 7;
pub const DEFAULT_PARK_AFTER: Duration = Duration::from_secs(30);
pub const DEFAULT_MAX_PARKED_EVENTS: u64 = 10_000_000;
pub const DEFAULT_STORE_CONCURRENCY: usize = 256;
pub const MAX_CONCURRENCY: usize = 4096;
pub const DEFAULT_LEASE_TIMEOUT: Duration = Duration::from_secs(6);
pub const DEFAULT_FENCE_DELAY: Duration = Duration::from_secs(10);
pub const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_TRACE_SAMPLE_RATIO: f64 = 0.01;
pub const DEFAULT_ADMIN_LISTEN: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9187);
const RESERVED_ATTRIBUTES: [&str; 3] = ["schema", "table", "op"];

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(deserialize_with = "de_string")]
    pub pipeline: String,
    pub source: SourceConfig,
    #[serde(default)]
    pub delivery: DeliveryConfig,
    pub sink: SinkConfig,
    pub state_store: StateStoreConfig,
    #[serde(default)]
    pub payload_store: Option<PayloadStoreConfig>,
    #[serde(default)]
    pub leadership: LeadershipConfig,
    #[serde(default)]
    pub admin: AdminConfig,
    #[serde(default)]
    pub telemetry: TelemetryConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    #[serde(deserialize_with = "de_string")]
    pub host: String,
    #[serde(default = "default_port", deserialize_with = "de_port")]
    pub port: u16,
    #[serde(deserialize_with = "de_string")]
    pub database: String,
    #[serde(deserialize_with = "de_string")]
    pub user: String,
    pub credentials: Credentials,
    #[serde(default)]
    pub tls: TlsSection,
    #[serde(deserialize_with = "de_string")]
    pub slot: String,
    #[serde(deserialize_with = "de_string")]
    pub publication: String,
    #[serde(default = "default_status_interval", deserialize_with = "de_duration")]
    pub status_interval: Duration,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Credentials {
    Password {
        password: Option<SecretString>,
    },
    AwsRdsIam {
        #[serde(default, deserialize_with = "de_opt_string")]
        region: Option<String>,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsSection {
    #[serde(default)]
    pub mode: TlsMode,
    #[serde(default, deserialize_with = "de_opt_path")]
    pub ca_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TlsMode {
    Disable,
    Require,
    VerifyCa,
    #[default]
    VerifyFull,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryConfig {
    #[serde(default, deserialize_with = "de_order_by")]
    pub order_by: OrderBy,
    #[serde(default = "default_lanes", deserialize_with = "de_count")]
    pub lanes: usize,
    #[serde(default = "default_max_inflight_bytes", deserialize_with = "de_size")]
    pub max_inflight_bytes: u64,
    #[serde(default = "default_park_after", deserialize_with = "de_duration")]
    pub park_after: Duration,
    #[serde(default = "default_max_parked_events", deserialize_with = "de_u64")]
    pub max_parked_events: u64,
}

impl Default for DeliveryConfig {
    fn default() -> Self {
        Self {
            order_by: OrderBy::default(),
            lanes: DEFAULT_LANES,
            max_inflight_bytes: DEFAULT_MAX_INFLIGHT_BYTES,
            park_after: DEFAULT_PARK_AFTER,
            max_parked_events: DEFAULT_MAX_PARKED_EVENTS,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeadershipConfig {
    #[serde(default = "default_lease_timeout", deserialize_with = "de_duration")]
    pub lease_timeout: Duration,
    #[serde(default = "default_fence_delay", deserialize_with = "de_duration")]
    pub fence_delay: Duration,
    #[serde(default = "default_drain_timeout", deserialize_with = "de_duration")]
    pub drain_timeout: Duration,
}

impl Default for LeadershipConfig {
    fn default() -> Self {
        Self {
            lease_timeout: DEFAULT_LEASE_TIMEOUT,
            fence_delay: DEFAULT_FENCE_DELAY,
            drain_timeout: DEFAULT_DRAIN_TIMEOUT,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConfig {
    #[serde(default = "default_admin_listen", deserialize_with = "de_socket_addr")]
    pub listen: SocketAddr,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            listen: DEFAULT_ADMIN_LISTEN,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelemetryConfig {
    #[serde(default, deserialize_with = "de_opt_string")]
    pub otlp_endpoint: Option<String>,
    #[serde(default = "default_trace_sample_ratio", deserialize_with = "de_ratio")]
    pub trace_sample_ratio: f64,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            otlp_endpoint: None,
            trace_sample_ratio: DEFAULT_TRACE_SAMPLE_RATIO,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum StateStoreConfig {
    S3(S3StoreConfig),
    Filesystem(FilesystemStoreConfig),
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3StoreConfig {
    #[serde(deserialize_with = "de_string")]
    pub bucket: String,
    #[serde(default, deserialize_with = "de_string")]
    pub prefix: String,
    #[serde(default, deserialize_with = "de_opt_string")]
    pub region: Option<String>,
    #[serde(default = "default_store_concurrency", deserialize_with = "de_count")]
    pub max_concurrency: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilesystemStoreConfig {
    #[serde(deserialize_with = "de_path")]
    pub path: PathBuf,
    #[serde(default = "default_store_concurrency", deserialize_with = "de_count")]
    pub max_concurrency: usize,
}

impl StateStoreConfig {
    pub fn max_concurrency(&self) -> usize {
        match self {
            Self::S3(s3) => s3.max_concurrency,
            Self::Filesystem(filesystem) => filesystem.max_concurrency,
        }
    }

    fn validate(&self) -> Result<(), ConfigError> {
        check_concurrency("state_store.max_concurrency", self.max_concurrency())?;
        match self {
            Self::S3(s3) => validate_bucket("state_store.bucket", &s3.bucket),
            Self::Filesystem(filesystem) if filesystem.path.as_os_str().is_empty() => {
                Err(ConfigError::InvalidValue {
                    field: "state_store.path",
                    value: String::new(),
                    reason: "must not be empty",
                })
            }
            Self::Filesystem(_) => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum PayloadStoreConfig {
    S3(S3PayloadConfig),
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3PayloadConfig {
    #[serde(deserialize_with = "de_string")]
    pub bucket: String,
    #[serde(default, deserialize_with = "de_string")]
    pub prefix: String,
    #[serde(default, deserialize_with = "de_opt_string")]
    pub region: Option<String>,
}

fn check_concurrency(field: &'static str, value: usize) -> Result<(), ConfigError> {
    if (1..=MAX_CONCURRENCY).contains(&value) {
        return Ok(());
    }
    Err(ConfigError::InvalidValue {
        field,
        value: value.to_string(),
        reason: "must be from 1 to 4096",
    })
}

fn validate_bucket(field: &'static str, bucket: &str) -> Result<(), ConfigError> {
    if bucket.is_empty() || bucket.contains('/') {
        return Err(ConfigError::InvalidValue {
            field,
            value: bucket.to_owned(),
            reason: "must be an S3 bucket name",
        });
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SinkConfig {
    Stdout(BasicSinkConfig),
    Discard(BasicSinkConfig),
    Sns(SnsConfig),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BasicSinkConfig {
    #[serde(default = "default_sink_concurrency", deserialize_with = "de_count")]
    pub max_concurrency: usize,
}

impl Default for BasicSinkConfig {
    fn default() -> Self {
        Self {
            max_concurrency: DEFAULT_SINK_CONCURRENCY,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnsConfig {
    #[serde(deserialize_with = "de_string")]
    pub topic_arn: String,
    #[serde(default, deserialize_with = "de_opt_string")]
    pub region: Option<String>,
    #[serde(default = "default_sink_concurrency", deserialize_with = "de_count")]
    pub max_concurrency: usize,
    #[serde(default, deserialize_with = "de_string_list")]
    pub attribute_columns: Vec<String>,
    #[serde(default, deserialize_with = "de_fair_queue_group")]
    pub fair_queue_group: Option<String>,
}

impl SinkConfig {
    pub fn max_concurrency(&self) -> usize {
        match self {
            Self::Stdout(basic) | Self::Discard(basic) => basic.max_concurrency,
            Self::Sns(sns) => sns.max_concurrency,
        }
    }

    pub fn attribute_columns(&self) -> &[String] {
        match self {
            Self::Stdout(_) | Self::Discard(_) => &[],
            Self::Sns(sns) => &sns.attribute_columns,
        }
    }
}

impl SnsConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        let parts: Vec<&str> = self.topic_arn.split(':').collect();
        if parts.len() != 6 || parts.first() != Some(&"arn") || parts.get(2) != Some(&"sns") {
            return Err(ConfigError::InvalidValue {
                field: "sink.topic_arn",
                value: self.topic_arn.clone(),
                reason: "must be an SNS topic ARN",
            });
        }
        if self.attribute_columns.len() > MAX_ATTRIBUTE_COLUMNS {
            return Err(ConfigError::InvalidValue {
                field: "sink.attribute_columns",
                value: self.attribute_columns.join(","),
                reason: "SNS allows 10 message attributes and schema, table and op use 3",
            });
        }
        for (index, column) in self.attribute_columns.iter().enumerate() {
            let duplicate = self
                .attribute_columns
                .iter()
                .take(index)
                .any(|earlier| earlier == column);
            if duplicate
                || RESERVED_ATTRIBUTES.contains(&column.as_str())
                || !valid_attribute_name(column)
            {
                return Err(ConfigError::InvalidValue {
                    field: "sink.attribute_columns",
                    value: column.clone(),
                    reason: "must be distinct SNS attribute names other than schema, table and op",
                });
            }
        }
        if let Some(column) = &self.fair_queue_group
            && !self.attribute_columns.contains(column)
        {
            return Err(ConfigError::InvalidValue {
                field: "sink.fair_queue_group",
                value: column.clone(),
                reason: "the column must also be listed in sink.attribute_columns",
            });
        }
        Ok(())
    }
}

fn valid_attribute_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    (1..=256).contains(&name.len())
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        && !name.starts_with('.')
        && !name.ends_with('.')
        && !name.contains("..")
        && !lower.starts_with("aws.")
        && !lower.starts_with("amazon.")
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("cannot read {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("invalid YAML: {0}")]
    Yaml(String),
    #[error("invalid configuration: {0}")]
    Invalid(#[from] serde_json::Error),
    #[error(
        "source.credentials.password must come from PGCDC__SOURCE__CREDENTIALS__PASSWORD, not the config file"
    )]
    PasswordInFile,
    #[error("password credentials need PGCDC__SOURCE__CREDENTIALS__PASSWORD")]
    MissingPassword,
    #[error("environment variable {0} does not name a configuration key")]
    BadEnvKey(String),
    #[error("{field} {value:?} is invalid: {reason}")]
    InvalidValue {
        field: &'static str,
        value: String,
        reason: &'static str,
    },
}

pub fn load(
    path: &Path,
    env: impl IntoIterator<Item = (String, String)>,
) -> Result<Config, ConfigError> {
    let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
        path: path.to_owned(),
        source,
    })?;
    parse(&text, env)
}

pub fn parse(
    yaml: &str,
    env: impl IntoIterator<Item = (String, String)>,
) -> Result<Config, ConfigError> {
    let mut options = serde_saphyr::Options::default();
    options.with_snippet = false;
    options.strict_booleans = true;
    let mut tree: Value = serde_saphyr::from_str_with_options(yaml, options)
        .map_err(|error| ConfigError::Yaml(error.to_string()))?;
    if tree.pointer("/source/credentials/password").is_some() {
        return Err(ConfigError::PasswordInFile);
    }
    for (name, value) in env {
        let Some(path) = name.strip_prefix(ENV_PREFIX) else {
            continue;
        };
        let keys: Vec<String> = path.split("__").map(str::to_ascii_lowercase).collect();
        if keys.iter().any(String::is_empty)
            || set_path(&mut tree, &keys, Value::String(value)).is_none()
        {
            return Err(ConfigError::BadEnvKey(name));
        }
    }
    let config: Config = serde_json::from_value(tree)?;
    config.validate()?;
    Ok(config)
}

fn set_path(tree: &mut Value, keys: &[String], value: Value) -> Option<()> {
    let (last, parents) = keys.split_last()?;
    let mut node = tree;
    for key in parents {
        if node.is_null() {
            *node = Value::Object(Map::new());
        }
        node = node
            .as_object_mut()?
            .entry(key.clone())
            .or_insert(Value::Null);
    }
    if node.is_null() {
        *node = Value::Object(Map::new());
    }
    node.as_object_mut()?.insert(last.clone(), value);
    Some(())
}

pub fn parse_size(text: &str) -> Option<u64> {
    let split = text.find(|c: char| !c.is_ascii_digit())?;
    let (digits, unit) = text.split_at_checked(split)?;
    let amount: u64 = digits.parse().ok()?;
    let scale: u64 = match unit {
        "B" => 1,
        "KiB" => 1 << 10,
        "MiB" => 1 << 20,
        "GiB" => 1 << 30,
        _ => return None,
    };
    amount.checked_mul(scale)
}

pub fn parse_duration(text: &str) -> Option<Duration> {
    let split = text.find(|c: char| !c.is_ascii_digit())?;
    let (digits, unit) = text.split_at_checked(split)?;
    let amount: u64 = digits.parse().ok()?;
    match unit {
        "ms" => Some(Duration::from_millis(amount)),
        "s" => Some(Duration::from_secs(amount)),
        "m" => amount.checked_mul(60).map(Duration::from_secs),
        "h" => amount.checked_mul(3600).map(Duration::from_secs),
        _ => None,
    }
}

impl Config {
    fn validate(&self) -> Result<(), ConfigError> {
        let slot_char =
            |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_';
        let pipeline_char = |byte: u8| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-';
        check_name(
            "pipeline",
            &self.pipeline,
            pipeline_char,
            "must be 1 to 63 letters, digits, '_' or '-'",
        )?;
        let slot_reason = "must be 1 to 63 lowercase letters, digits or underscores";
        check_name("source.slot", &self.source.slot, slot_char, slot_reason)?;
        check_name(
            "source.publication",
            &self.source.publication,
            slot_char,
            slot_reason,
        )?;
        for (field, value) in [
            ("source.host", &self.source.host),
            ("source.database", &self.source.database),
            ("source.user", &self.source.user),
        ] {
            if value.is_empty() {
                return Err(ConfigError::InvalidValue {
                    field,
                    value: value.clone(),
                    reason: "must not be empty",
                });
            }
        }
        if self.source.status_interval.is_zero() {
            return Err(ConfigError::InvalidValue {
                field: "source.status_interval",
                value: "0".to_owned(),
                reason: "must be greater than zero",
            });
        }
        if matches!(
            self.source.credentials,
            Credentials::Password { password: None }
        ) {
            return Err(ConfigError::MissingPassword);
        }
        self.delivery.validate()?;
        self.leadership.validate()?;
        self.state_store.validate()?;
        self.telemetry.validate()?;
        if let Some(PayloadStoreConfig::S3(payload)) = &self.payload_store {
            validate_bucket("payload_store.bucket", &payload.bucket)?;
        }
        check_concurrency("sink.max_concurrency", self.sink.max_concurrency())?;
        match &self.sink {
            SinkConfig::Sns(sns) => sns.validate(),
            SinkConfig::Stdout(_) | SinkConfig::Discard(_) => Ok(()),
        }
    }
}

impl DeliveryConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if !(1..=MAX_LANES).contains(&self.lanes) {
            return Err(ConfigError::InvalidValue {
                field: "delivery.lanes",
                value: self.lanes.to_string(),
                reason: "must be from 1 to 4096",
            });
        }
        if self.max_inflight_bytes == 0 {
            return Err(ConfigError::InvalidValue {
                field: "delivery.max_inflight_bytes",
                value: "0".to_owned(),
                reason: "must be greater than zero",
            });
        }
        if self.park_after.is_zero() {
            return Err(ConfigError::InvalidValue {
                field: "delivery.park_after",
                value: "0".to_owned(),
                reason: "must be greater than zero",
            });
        }
        Ok(())
    }
}

impl LeadershipConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.lease_timeout <= POLL_INTERVAL {
            return Err(invalid_duration(
                "leadership.lease_timeout",
                self.lease_timeout,
                "must be greater than the 2s lease check interval",
            ));
        }
        if self.fence_delay <= self.lease_timeout {
            return Err(invalid_duration(
                "leadership.fence_delay",
                self.fence_delay,
                "must be greater than leadership.lease_timeout",
            ));
        }
        if self.drain_timeout.is_zero() {
            return Err(invalid_duration(
                "leadership.drain_timeout",
                self.drain_timeout,
                "must be greater than zero",
            ));
        }
        Ok(())
    }
}

impl TelemetryConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if !(0.0..=1.0).contains(&self.trace_sample_ratio) {
            return Err(ConfigError::InvalidValue {
                field: "telemetry.trace_sample_ratio",
                value: self.trace_sample_ratio.to_string(),
                reason: "must be a number from 0 to 1",
            });
        }
        match self.otlp_endpoint.as_deref() {
            Some(endpoint) if !endpoint.is_empty() && !is_http_url(endpoint) => {
                Err(ConfigError::InvalidValue {
                    field: "telemetry.otlp_endpoint",
                    value: endpoint.to_owned(),
                    reason: "must be an http:// or https:// URL",
                })
            }
            _ => Ok(()),
        }
    }
}

fn is_http_url(text: &str) -> bool {
    let Some(rest) = text
        .strip_prefix("http://")
        .or_else(|| text.strip_prefix("https://"))
    else {
        return false;
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    let host_port = authority.rsplit('@').next().unwrap_or_default();
    let host = if host_port.starts_with('[') {
        host_port
    } else {
        host_port.split(':').next().unwrap_or_default()
    };
    !host.is_empty()
}

fn invalid_duration(field: &'static str, value: Duration, reason: &'static str) -> ConfigError {
    ConfigError::InvalidValue {
        field,
        value: format!("{}ms", value.as_millis()),
        reason,
    }
}

fn check_name(
    field: &'static str,
    value: &str,
    allowed: impl Fn(u8) -> bool,
    reason: &'static str,
) -> Result<(), ConfigError> {
    if value.is_empty() || value.len() > 63 || !value.bytes().all(allowed) {
        return Err(ConfigError::InvalidValue {
            field,
            value: value.to_owned(),
            reason,
        });
    }
    Ok(())
}

fn default_port() -> u16 {
    5432
}

fn default_status_interval() -> Duration {
    Duration::from_secs(10)
}

fn default_lanes() -> usize {
    DEFAULT_LANES
}

fn default_max_inflight_bytes() -> u64 {
    DEFAULT_MAX_INFLIGHT_BYTES
}

fn default_sink_concurrency() -> usize {
    DEFAULT_SINK_CONCURRENCY
}

fn default_park_after() -> Duration {
    DEFAULT_PARK_AFTER
}

fn default_max_parked_events() -> u64 {
    DEFAULT_MAX_PARKED_EVENTS
}

fn default_lease_timeout() -> Duration {
    DEFAULT_LEASE_TIMEOUT
}

fn default_fence_delay() -> Duration {
    DEFAULT_FENCE_DELAY
}

fn default_drain_timeout() -> Duration {
    DEFAULT_DRAIN_TIMEOUT
}

fn default_admin_listen() -> SocketAddr {
    DEFAULT_ADMIN_LISTEN
}

fn default_trace_sample_ratio() -> f64 {
    DEFAULT_TRACE_SAMPLE_RATIO
}

fn default_store_concurrency() -> usize {
    DEFAULT_STORE_CONCURRENCY
}

fn de_string_list<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<String>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum ListField {
        List(Vec<StringField>),
        Text(StringField),
    }

    Ok(match ListField::deserialize(deserializer)? {
        ListField::List(items) => items.into_iter().map(|item| item.0).collect(),
        ListField::Text(text) => text
            .0
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
            .map(str::to_owned)
            .collect(),
    })
}

fn de_fair_queue_group<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<String>, D::Error> {
    let Some(text) = de_opt_string(deserializer)? else {
        return Ok(None);
    };
    match text.strip_prefix("column:") {
        Some(column) if !column.is_empty() => Ok(Some(column.to_owned())),
        _ => Err(de::Error::custom(format!(
            "fair_queue_group {text:?} must be column:<name>"
        ))),
    }
}

fn de_order_by<'de, D: Deserializer<'de>>(deserializer: D) -> Result<OrderBy, D::Error> {
    de_string(deserializer)?.parse().map_err(de::Error::custom)
}

fn de_size<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    let text = de_string(deserializer)?;
    parse_size(&text).ok_or_else(|| {
        de::Error::custom(format!(
            "invalid size {text:?}; use an integer with B, KiB, MiB or GiB"
        ))
    })
}

fn de_count<'de, D: Deserializer<'de>>(deserializer: D) -> Result<usize, D::Error> {
    let text = de_string(deserializer)?;
    text.parse()
        .map_err(|_| de::Error::custom(format!("{text:?} is not a whole number")))
}

fn de_u64<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    let text = de_string(deserializer)?;
    text.parse()
        .map_err(|_| de::Error::custom(format!("{text:?} is not a whole number")))
}

fn de_duration<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
    let text = de_string(deserializer)?;
    parse_duration(&text).ok_or_else(|| {
        de::Error::custom(format!(
            "invalid duration {text:?}; use an integer with ms, s, m or h"
        ))
    })
}

fn de_ratio<'de, D: Deserializer<'de>>(deserializer: D) -> Result<f64, D::Error> {
    struct RatioVisitor;

    impl RatioVisitor {
        fn not_a_number<E: de::Error>(text: &str) -> E {
            E::custom(format!(
                "telemetry.trace_sample_ratio {text:?} must be a number from 0 to 1"
            ))
        }
    }

    impl Visitor<'_> for RatioVisitor {
        type Value = f64;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a number from 0 to 1")
        }

        fn visit_f64<E: de::Error>(self, value: f64) -> Result<f64, E> {
            Ok(value)
        }

        fn visit_u64<E: de::Error>(self, value: u64) -> Result<f64, E> {
            u32::try_from(value)
                .map(f64::from)
                .map_err(|_| Self::not_a_number(&value.to_string()))
        }

        fn visit_i64<E: de::Error>(self, value: i64) -> Result<f64, E> {
            i32::try_from(value)
                .map(f64::from)
                .map_err(|_| Self::not_a_number(&value.to_string()))
        }

        fn visit_str<E: de::Error>(self, value: &str) -> Result<f64, E> {
            value.trim().parse().map_err(|_| Self::not_a_number(value))
        }
    }

    deserializer.deserialize_any(RatioVisitor)
}

fn de_socket_addr<'de, D: Deserializer<'de>>(deserializer: D) -> Result<SocketAddr, D::Error> {
    let text = de_string(deserializer)?;
    SocketAddr::from_str(&text).map_err(|_| {
        de::Error::custom(format!(
            "admin.listen {text:?} must be an IP address and port such as 127.0.0.1:9187"
        ))
    })
}

fn de_string<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    struct StringVisitor;

    impl Visitor<'_> for StringVisitor {
        type Value = String;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a string")
        }

        fn visit_str<E: de::Error>(self, value: &str) -> Result<String, E> {
            Ok(value.to_owned())
        }

        fn visit_string<E: de::Error>(self, value: String) -> Result<String, E> {
            Ok(value)
        }

        fn visit_u64<E: de::Error>(self, value: u64) -> Result<String, E> {
            Ok(value.to_string())
        }

        fn visit_i64<E: de::Error>(self, value: i64) -> Result<String, E> {
            Ok(value.to_string())
        }

        fn visit_f64<E: de::Error>(self, value: f64) -> Result<String, E> {
            Ok(value.to_string())
        }

        fn visit_bool<E: de::Error>(self, value: bool) -> Result<String, E> {
            Ok(value.to_string())
        }
    }

    deserializer.deserialize_any(StringVisitor)
}

fn de_opt_string<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<String>, D::Error> {
    Option::<StringField>::deserialize(deserializer).map(|field| field.map(|field| field.0))
}

fn de_path<'de, D: Deserializer<'de>>(deserializer: D) -> Result<PathBuf, D::Error> {
    de_string(deserializer).map(PathBuf::from)
}

fn de_opt_path<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<PathBuf>, D::Error> {
    de_opt_string(deserializer).map(|text| text.map(PathBuf::from))
}

struct StringField(String);

impl<'de> Deserialize<'de> for StringField {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        de_string(deserializer).map(Self)
    }
}

fn de_port<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u16, D::Error> {
    struct PortVisitor;

    impl Visitor<'_> for PortVisitor {
        type Value = u16;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a TCP port number")
        }

        fn visit_u64<E: de::Error>(self, value: u64) -> Result<u16, E> {
            u16::try_from(value).map_err(|_| E::custom(format!("port {value} is out of range")))
        }

        fn visit_i64<E: de::Error>(self, value: i64) -> Result<u16, E> {
            u16::try_from(value).map_err(|_| E::custom(format!("port {value} is out of range")))
        }

        fn visit_str<E: de::Error>(self, value: &str) -> Result<u16, E> {
            value
                .parse()
                .map_err(|_| E::custom(format!("port {value:?} is not a number from 0 to 65535")))
        }
    }

    deserializer.deserialize_any(PortVisitor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::ExposeSecret;

    const MINIMAL: &str = "
pipeline: orders
source:
  host: db.internal
  database: app
  user: pgcdc
  credentials:
    type: password
  slot: pgcdc_orders
  publication: pgcdc_orders
sink:
  type: stdout
state_store:
  type: filesystem
  path: /var/lib/pg-cdc
";

    fn env(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    fn with_password() -> Vec<(String, String)> {
        env(&[("PGCDC__SOURCE__CREDENTIALS__PASSWORD", "s3cr3t pass")])
    }

    #[test]
    fn parses_minimal_password_config_with_defaults() {
        let config = parse(MINIMAL, with_password()).unwrap();
        assert_eq!(config.pipeline, "orders");
        assert_eq!(config.source.host, "db.internal");
        assert_eq!(config.source.port, 5432);
        assert_eq!(config.source.tls.mode, TlsMode::VerifyFull);
        assert_eq!(config.source.tls.ca_file, None);
        assert_eq!(config.source.status_interval, Duration::from_secs(10));
        assert_eq!(config.sink, SinkConfig::Stdout(BasicSinkConfig::default()));
        let Credentials::Password {
            password: Some(password),
        } = &config.source.credentials
        else {
            panic!("expected a password");
        };
        assert_eq!(password.expose_secret(), "s3cr3t pass");
    }

    #[test]
    fn parses_aws_rds_iam_with_tls_and_region() {
        let yaml = MINIMAL
            .replace("type: password", "type: aws_rds_iam\n    region: us-east-1")
            .replace(
                "  slot:",
                "  tls:\n    mode: verify-ca\n    ca_file: /etc/pg-cdc/rds.pem\n  slot:",
            );
        let config = parse(&yaml, env(&[])).unwrap();
        assert!(
            matches!(&config.source.credentials, Credentials::AwsRdsIam { region: Some(region) } if region == "us-east-1")
        );
        assert_eq!(config.source.tls.mode, TlsMode::VerifyCa);
        assert_eq!(
            config.source.tls.ca_file,
            Some(PathBuf::from("/etc/pg-cdc/rds.pem"))
        );
    }

    #[test]
    fn environment_overrides_any_key() {
        let mut vars = with_password();
        vars.extend(env(&[
            ("PGCDC__SOURCE__PORT", "6543"),
            ("PGCDC__SOURCE__TLS__MODE", "require"),
            ("PGCDC__SOURCE__STATUS_INTERVAL", "250ms"),
            ("PGCDC__PIPELINE", "billing"),
            ("HOME", "/root"),
            ("PGCDCX__SOURCE__PORT", "1"),
        ]));
        let config = parse(MINIMAL, vars).unwrap();
        assert_eq!(config.source.port, 6543);
        assert_eq!(config.source.tls.mode, TlsMode::Require);
        assert_eq!(config.source.status_interval, Duration::from_millis(250));
        assert_eq!(config.pipeline, "billing");
    }

    #[test]
    fn rejects_password_in_file() {
        let yaml = MINIMAL.replace("type: password", "type: password\n    password: hunter2");
        assert!(matches!(
            parse(&yaml, with_password()),
            Err(ConfigError::PasswordInFile)
        ));
    }

    #[test]
    fn password_credentials_need_the_environment_password() {
        assert!(matches!(
            parse(MINIMAL, env(&[])),
            Err(ConfigError::MissingPassword)
        ));
    }

    #[test]
    fn rejects_unknown_keys_and_types() {
        let unknown_key = MINIMAL.replace("  slot:", "  sslmode: require\n  slot:");
        assert!(matches!(
            parse(&unknown_key, with_password()),
            Err(ConfigError::Invalid(_))
        ));
        let unknown_sink = MINIMAL.replace("type: stdout", "type: kafka");
        assert!(matches!(
            parse(&unknown_sink, with_password()),
            Err(ConfigError::Invalid(_))
        ));
        assert!(matches!(
            parse("pipeline: [", with_password()),
            Err(ConfigError::Yaml(_))
        ));
    }

    #[test]
    fn yaml_errors_do_not_echo_file_contents() {
        for yaml in [
            "source:\n  credentials:\n    password: leak-me\n  host: [\n",
            "pipeline: orders\nsource:\n  credentials:\n    password: leak-me\n   bad: indent\n",
            "password: leak-me\n\t- : :\n",
        ] {
            let Err(ConfigError::Yaml(text)) = parse(yaml, env(&[])) else {
                panic!("expected a YAML error for {yaml:?}");
            };
            assert!(!text.contains("leak-me"), "{text}");
        }
    }

    #[test]
    fn accepts_scalar_looking_values_for_string_fields() {
        let yaml = "
pipeline: 2024
source:
  host: 127.1
  database: 1234
  user: 12345
  credentials:
    type: aws_rds_iam
    region: 42
  tls:
    ca_file: 7
  slot: 123
  publication: true
  status_interval: 5s
sink:
  type: stdout
state_store:
  type: s3
  bucket: 2026
  prefix: 10
";
        let config = parse(yaml, env(&[])).unwrap();
        assert_eq!(config.pipeline, "2024");
        assert_eq!(
            config.state_store,
            StateStoreConfig::S3(S3StoreConfig {
                bucket: "2026".to_owned(),
                prefix: "10".to_owned(),
                region: None,
                max_concurrency: DEFAULT_STORE_CONCURRENCY,
            })
        );
        assert_eq!(config.source.host, "127.1");
        assert_eq!(config.source.database, "1234");
        assert_eq!(config.source.user, "12345");
        assert_eq!(config.source.slot, "123");
        assert_eq!(config.source.publication, "true");
        assert_eq!(config.source.tls.ca_file, Some(PathBuf::from("7")));
        assert!(
            matches!(&config.source.credentials, Credentials::AwsRdsIam { region: Some(region) } if region == "42")
        );
    }

    #[test]
    fn yes_and_no_stay_strings() {
        let yaml = MINIMAL.replace("user: pgcdc", "user: on");
        assert_eq!(parse(&yaml, with_password()).unwrap().source.user, "on");
    }

    #[test]
    fn rejects_non_scalar_values_for_string_fields() {
        let yaml = MINIMAL.replace("pipeline: orders", "pipeline: [orders]");
        assert!(matches!(
            parse(&yaml, with_password()),
            Err(ConfigError::Invalid(_))
        ));
    }

    #[test]
    fn rejects_unsafe_names() {
        for slot in ["Orders", "orders-x", "a b", "o'r", "", &"s".repeat(64)] {
            let yaml = MINIMAL.replace("slot: pgcdc_orders", &format!("slot: \"{slot}\""));
            assert!(
                matches!(
                    parse(&yaml, with_password()),
                    Err(ConfigError::InvalidValue {
                        field: "source.slot",
                        ..
                    })
                ),
                "slot {slot:?}"
            );
        }
        let publication = MINIMAL.replace("publication: pgcdc_orders", "publication: \"pub;drop\"");
        assert!(matches!(
            parse(&publication, with_password()),
            Err(ConfigError::InvalidValue {
                field: "source.publication",
                ..
            })
        ));
        let pipeline = MINIMAL.replace("pipeline: orders", "pipeline: \"orders/../x\"");
        assert!(matches!(
            parse(&pipeline, with_password()),
            Err(ConfigError::InvalidValue {
                field: "pipeline",
                ..
            })
        ));
    }

    #[test]
    fn rejects_bad_env_keys_and_values() {
        let mut empty_segment = with_password();
        empty_segment.push(("PGCDC__SOURCE____PORT".to_owned(), "1".to_owned()));
        assert!(matches!(
            parse(MINIMAL, empty_segment),
            Err(ConfigError::BadEnvKey(_))
        ));
        let mut scalar_parent = with_password();
        scalar_parent.push(("PGCDC__PIPELINE__NAME".to_owned(), "x".to_owned()));
        assert!(matches!(
            parse(MINIMAL, scalar_parent),
            Err(ConfigError::BadEnvKey(_))
        ));
        let mut bad_port = with_password();
        bad_port.push(("PGCDC__SOURCE__PORT".to_owned(), "70000".to_owned()));
        assert!(matches!(
            parse(MINIMAL, bad_port),
            Err(ConfigError::Invalid(_))
        ));
        let mut zero_interval = with_password();
        zero_interval.push(("PGCDC__SOURCE__STATUS_INTERVAL".to_owned(), "0s".to_owned()));
        assert!(matches!(
            parse(MINIMAL, zero_interval),
            Err(ConfigError::InvalidValue {
                field: "source.status_interval",
                ..
            })
        ));
    }

    #[test]
    fn parses_durations_with_units() {
        assert_eq!(parse_duration("250ms"), Some(Duration::from_millis(250)));
        assert_eq!(parse_duration("30s"), Some(Duration::from_secs(30)));
        assert_eq!(parse_duration("5m"), Some(Duration::from_secs(300)));
        assert_eq!(parse_duration("2h"), Some(Duration::from_secs(7200)));
        for bad in [
            "",
            "10",
            "s",
            "1.5s",
            "5d",
            "-1s",
            "1 s",
            "99999999999999999999s",
        ] {
            assert_eq!(parse_duration(bad), None, "{bad:?}");
        }
    }

    const SNS: &str =
        "sink:\n  type: sns\n  topic_arn: arn:aws:sns:us-east-1:123456789012:orders.fifo\n";

    fn sns(extra: &str) -> String {
        MINIMAL.replace("sink:\n  type: stdout\n", &format!("{SNS}{extra}"))
    }

    #[test]
    fn parses_the_sns_sink_with_defaults() {
        let config = parse(&sns(""), with_password()).unwrap();
        assert_eq!(
            config.sink,
            SinkConfig::Sns(SnsConfig {
                topic_arn: "arn:aws:sns:us-east-1:123456789012:orders.fifo".to_owned(),
                region: None,
                max_concurrency: 32,
                attribute_columns: Vec::new(),
                fair_queue_group: None,
            })
        );
        assert_eq!(config.sink.max_concurrency(), 32);
        assert_eq!(config.sink.attribute_columns(), &[] as &[String]);
    }

    #[test]
    fn parses_every_sns_setting() {
        let yaml = sns(
            "  region: eu-west-1\n  max_concurrency: 8\n  attribute_columns: [tenant_id, region]\n  fair_queue_group: column:tenant_id\n",
        );
        let config = parse(&yaml, with_password()).unwrap();
        let SinkConfig::Sns(sns) = &config.sink else {
            panic!("expected sns");
        };
        assert_eq!(sns.region.as_deref(), Some("eu-west-1"));
        assert_eq!(sns.max_concurrency, 8);
        assert_eq!(sns.attribute_columns, ["tenant_id", "region"]);
        assert_eq!(sns.fair_queue_group.as_deref(), Some("tenant_id"));
        assert_eq!(config.sink.attribute_columns(), ["tenant_id", "region"]);
    }

    #[test]
    fn environment_overrides_sns_settings_and_lists() {
        let mut vars = with_password();
        vars.extend(env(&[
            ("PGCDC__SINK__ATTRIBUTE_COLUMNS", "tenant_id, region"),
            ("PGCDC__SINK__MAX_CONCURRENCY", "4"),
            (
                "PGCDC__SINK__TOPIC_ARN",
                "arn:aws:sns:us-east-1:123456789012:other",
            ),
        ]));
        let config = parse(&sns(""), vars).unwrap();
        let SinkConfig::Sns(sns) = &config.sink else {
            panic!("expected sns");
        };
        assert_eq!(sns.attribute_columns, ["tenant_id", "region"]);
        assert_eq!(sns.max_concurrency, 4);
        assert_eq!(sns.topic_arn, "arn:aws:sns:us-east-1:123456789012:other");
    }

    #[test]
    fn rejects_invalid_sns_settings() {
        for (extra, field) in [
            ("  max_concurrency: 0\n", "sink.max_concurrency"),
            (
                "  attribute_columns: [a, b, c, d, e, f, g, h]\n",
                "sink.attribute_columns",
            ),
            ("  attribute_columns: [op]\n", "sink.attribute_columns"),
            (
                "  attribute_columns: [tenant, tenant]\n",
                "sink.attribute_columns",
            ),
            (
                "  attribute_columns: [\"has space\"]\n",
                "sink.attribute_columns",
            ),
            (
                "  attribute_columns: [AWS.thing]\n",
                "sink.attribute_columns",
            ),
            ("  attribute_columns: [a..b]\n", "sink.attribute_columns"),
            (
                "  fair_queue_group: column:tenant\n",
                "sink.fair_queue_group",
            ),
        ] {
            let error = parse(&sns(extra), with_password()).unwrap_err();
            assert!(
                matches!(&error, ConfigError::InvalidValue { field: actual, .. } if *actual == field),
                "{extra}: {error}"
            );
        }
        let bad_arn = sns("").replace(
            "arn:aws:sns:us-east-1:123456789012:orders.fifo",
            "arn:aws:sqs:us-east-1:123456789012:orders",
        );
        assert!(matches!(
            parse(&bad_arn, with_password()),
            Err(ConfigError::InvalidValue {
                field: "sink.topic_arn",
                ..
            })
        ));
        for extra in ["  fair_queue_group: tenant\n", "  topic: x\n"] {
            assert!(
                matches!(
                    parse(&sns(extra), with_password()),
                    Err(ConfigError::Invalid(_))
                ),
                "{extra}"
            );
        }
        let missing_arn = MINIMAL.replace("type: stdout", "type: sns");
        assert!(matches!(
            parse(&missing_arn, with_password()),
            Err(ConfigError::Invalid(_))
        ));
    }

    #[test]
    fn parses_the_discard_sink() {
        let yaml = MINIMAL.replace("type: stdout", "type: discard");
        let config = parse(&yaml, with_password()).unwrap();
        assert_eq!(config.sink, SinkConfig::Discard(BasicSinkConfig::default()));
        assert_eq!(config.sink.max_concurrency(), DEFAULT_SINK_CONCURRENCY);
        assert_eq!(
            SinkConfig::Stdout(BasicSinkConfig::default()).max_concurrency(),
            32
        );
    }

    #[test]
    fn every_sink_takes_max_concurrency_from_the_file_or_the_environment() {
        for sink in ["stdout", "discard"] {
            let yaml = MINIMAL.replace(
                "type: stdout\n",
                &format!("type: {sink}\n  max_concurrency: 8\n"),
            );
            let config = parse(&yaml, with_password()).unwrap();
            assert_eq!(config.sink.max_concurrency(), 8, "{sink}");

            let mut vars = with_password();
            vars.extend(env(&[("PGCDC__SINK__MAX_CONCURRENCY", "3")]));
            let config = parse(&MINIMAL.replace("stdout", sink), vars).unwrap();
            assert_eq!(config.sink.max_concurrency(), 3, "{sink}");

            for bad in ["0", "4097", "18446744073709551615"] {
                let invalid = MINIMAL.replace(
                    "type: stdout\n",
                    &format!("type: {sink}\n  max_concurrency: {bad}\n"),
                );
                assert!(
                    matches!(
                        parse(&invalid, with_password()),
                        Err(ConfigError::InvalidValue {
                            field: "sink.max_concurrency",
                            ..
                        })
                    ),
                    "{sink} {bad}"
                );
            }
            let most = MINIMAL.replace(
                "type: stdout\n",
                &format!("type: {sink}\n  max_concurrency: 4096\n"),
            );
            assert_eq!(
                parse(&most, with_password())
                    .unwrap()
                    .sink
                    .max_concurrency(),
                MAX_CONCURRENCY,
                "{sink}"
            );

            let unknown =
                MINIMAL.replace("type: stdout\n", &format!("type: {sink}\n  topic_arn: x\n"));
            assert!(
                matches!(
                    parse(&unknown, with_password()),
                    Err(ConfigError::Invalid(_))
                ),
                "{sink}"
            );
        }
    }

    #[test]
    fn delivery_defaults_match_the_spec() {
        let config = parse(MINIMAL, with_password()).unwrap();
        assert_eq!(config.delivery, DeliveryConfig::default());
        assert_eq!(config.delivery.order_by, OrderBy::Key);
        assert_eq!(config.delivery.lanes, 64);
        assert_eq!(config.delivery.max_inflight_bytes, 256 * 1024 * 1024);
        assert_eq!(config.delivery.park_after, Duration::from_secs(30));
        assert_eq!(config.delivery.max_parked_events, 10_000_000);
    }

    #[test]
    fn parses_delivery_settings() {
        let yaml = MINIMAL.replace(
            "sink:",
            "delivery:\n  order_by: column:tenant_id\n  lanes: 8\n  max_inflight_bytes: 64MiB\n  park_after: 2m\n  max_parked_events: 0\nsink:",
        );
        let config = parse(&yaml, with_password()).unwrap();
        assert_eq!(
            config.delivery,
            DeliveryConfig {
                order_by: OrderBy::Column("tenant_id".to_owned()),
                lanes: 8,
                max_inflight_bytes: 64 << 20,
                park_after: Duration::from_secs(120),
                max_parked_events: 0,
            }
        );
    }

    #[test]
    fn environment_overrides_delivery_settings() {
        let mut vars = with_password();
        vars.extend(env(&[
            ("PGCDC__DELIVERY__ORDER_BY", "pipeline"),
            ("PGCDC__DELIVERY__LANES", "2"),
            ("PGCDC__DELIVERY__MAX_INFLIGHT_BYTES", "1GiB"),
            ("PGCDC__DELIVERY__PARK_AFTER", "500ms"),
            ("PGCDC__DELIVERY__MAX_PARKED_EVENTS", "25"),
        ]));
        let config = parse(MINIMAL, vars).unwrap();
        assert_eq!(config.delivery.order_by, OrderBy::Pipeline);
        assert_eq!(config.delivery.lanes, 2);
        assert_eq!(config.delivery.max_inflight_bytes, 1 << 30);
        assert_eq!(config.delivery.park_after, Duration::from_millis(500));
        assert_eq!(config.delivery.max_parked_events, 25);
    }

    #[test]
    fn parses_sizes_with_binary_units() {
        assert_eq!(parse_size("10B"), Some(10));
        assert_eq!(parse_size("512KiB"), Some(512 * 1024));
        assert_eq!(parse_size("256MiB"), Some(256 * 1024 * 1024));
        assert_eq!(parse_size("2GiB"), Some(2 * 1024 * 1024 * 1024));
        for bad in [
            "",
            "10",
            "MiB",
            "1.5MiB",
            "10MB",
            "10mib",
            "10 MiB",
            "-1B",
            "99999999999999GiB",
        ] {
            assert_eq!(parse_size(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn rejects_invalid_delivery_settings() {
        let delivery = |body: &str| MINIMAL.replace("sink:", &format!("delivery:\n{body}\nsink:"));
        for body in [
            "  order_by: random",
            "  max_inflight_bytes: 10MB",
            "  lanes: many",
            "  park_after: 30",
            "  max_parked_events: -1",
            "  max_parked_events: lots",
        ] {
            assert!(
                matches!(
                    parse(&delivery(body), with_password()),
                    Err(ConfigError::Invalid(_))
                ),
                "{body}"
            );
        }
        for (body, field) in [
            ("  lanes: 0", "delivery.lanes"),
            ("  lanes: 4097", "delivery.lanes"),
            ("  max_inflight_bytes: 0MiB", "delivery.max_inflight_bytes"),
            ("  park_after: 0s", "delivery.park_after"),
        ] {
            let error = parse(&delivery(body), with_password()).unwrap_err();
            assert!(
                matches!(&error, ConfigError::InvalidValue { field: actual, .. } if *actual == field),
                "{body}: {error}"
            );
        }
    }

    const FILESYSTEM_STORE: &str = "state_store:\n  type: filesystem\n  path: /var/lib/pg-cdc\n";

    fn stores(sections: &str) -> String {
        MINIMAL.replace(FILESYSTEM_STORE, sections)
    }

    #[test]
    fn parses_the_filesystem_state_store_with_defaults() {
        let config = parse(MINIMAL, with_password()).unwrap();
        assert_eq!(
            config.state_store,
            StateStoreConfig::Filesystem(FilesystemStoreConfig {
                path: PathBuf::from("/var/lib/pg-cdc"),
                max_concurrency: 256,
            })
        );
        assert_eq!(config.state_store.max_concurrency(), 256);
        assert_eq!(config.payload_store, None);
    }

    #[test]
    fn parses_the_s3_state_and_payload_stores() {
        let yaml = stores(
            "state_store:\n  type: s3\n  bucket: pgcdc-state\n  prefix: orders/\n  region: eu-west-1\n  max_concurrency: 8\npayload_store:\n  type: s3\n  bucket: pgcdc-payloads\n  prefix: orders/\n  region: eu-west-1\n",
        );
        let config = parse(&yaml, with_password()).unwrap();
        assert_eq!(
            config.state_store,
            StateStoreConfig::S3(S3StoreConfig {
                bucket: "pgcdc-state".to_owned(),
                prefix: "orders/".to_owned(),
                region: Some("eu-west-1".to_owned()),
                max_concurrency: 8,
            })
        );
        assert_eq!(
            config.payload_store,
            Some(PayloadStoreConfig::S3(S3PayloadConfig {
                bucket: "pgcdc-payloads".to_owned(),
                prefix: "orders/".to_owned(),
                region: Some("eu-west-1".to_owned()),
            }))
        );
        let minimal = stores(
            "state_store:\n  type: s3\n  bucket: b\npayload_store:\n  type: s3\n  bucket: p\n",
        );
        let config = parse(&minimal, with_password()).unwrap();
        assert_eq!(
            config.state_store,
            StateStoreConfig::S3(S3StoreConfig {
                bucket: "b".to_owned(),
                prefix: String::new(),
                region: None,
                max_concurrency: DEFAULT_STORE_CONCURRENCY,
            })
        );
    }

    #[test]
    fn the_state_store_is_required_and_can_come_from_the_environment() {
        assert!(matches!(
            parse(&stores(""), with_password()),
            Err(ConfigError::Invalid(_))
        ));
        let mut vars = with_password();
        vars.extend(env(&[
            ("PGCDC__STATE_STORE__TYPE", "s3"),
            ("PGCDC__STATE_STORE__BUCKET", "from-env"),
            ("PGCDC__STATE_STORE__MAX_CONCURRENCY", "4"),
        ]));
        let config = parse(&stores(""), vars).unwrap();
        assert_eq!(
            config.state_store,
            StateStoreConfig::S3(S3StoreConfig {
                bucket: "from-env".to_owned(),
                prefix: String::new(),
                region: None,
                max_concurrency: 4,
            })
        );
    }

    #[test]
    fn rejects_invalid_store_settings() {
        for (sections, field) in [
            (
                "state_store:\n  type: filesystem\n  path: /x\n  max_concurrency: 0\n",
                "state_store.max_concurrency",
            ),
            (
                "state_store:\n  type: filesystem\n  path: /x\n  max_concurrency: 4097\n",
                "state_store.max_concurrency",
            ),
            (
                "state_store:\n  type: s3\n  bucket: b\n  max_concurrency: 18446744073709551615\n",
                "state_store.max_concurrency",
            ),
            (
                "state_store:\n  type: filesystem\n  path: \"\"\n",
                "state_store.path",
            ),
            (
                "state_store:\n  type: s3\n  bucket: \"\"\n",
                "state_store.bucket",
            ),
            (
                "state_store:\n  type: s3\n  bucket: a/b\n",
                "state_store.bucket",
            ),
            (
                "state_store:\n  type: s3\n  bucket: b\npayload_store:\n  type: s3\n  bucket: \"\"\n",
                "payload_store.bucket",
            ),
        ] {
            let error = parse(&stores(sections), with_password()).unwrap_err();
            assert!(
                matches!(&error, ConfigError::InvalidValue { field: actual, .. } if *actual == field),
                "{sections}: {error}"
            );
        }
        for sections in [
            "state_store:\n  type: postgres\n",
            "state_store:\n  type: s3\n",
            "state_store:\n  type: s3\n  bucket: b\n  path: /x\n",
            "state_store:\n  type: filesystem\n  path: /x\npayload_store:\n  type: filesystem\n  path: /y\n",
            "state_store:\n  type: filesystem\n  path: /x\npayload_store:\n  type: s3\n",
        ] {
            assert!(
                matches!(
                    parse(&stores(sections), with_password()),
                    Err(ConfigError::Invalid(_))
                ),
                "{sections}"
            );
        }
    }

    fn leadership(body: &str) -> String {
        format!("{MINIMAL}leadership:\n{body}\n")
    }

    fn admin(body: &str) -> String {
        format!("{MINIMAL}admin:\n{body}\n")
    }

    fn invalid_field(yaml: &str) -> &'static str {
        match parse(yaml, with_password()) {
            Err(ConfigError::InvalidValue { field, .. }) => field,
            other => panic!("expected an invalid value, got {other:?}"),
        }
    }

    #[test]
    fn leadership_and_admin_default_to_the_spec_values() {
        let config = parse(MINIMAL, with_password()).unwrap();
        assert_eq!(config.leadership, LeadershipConfig::default());
        assert_eq!(config.leadership.lease_timeout, Duration::from_secs(6));
        assert_eq!(config.leadership.fence_delay, Duration::from_secs(10));
        assert_eq!(config.leadership.drain_timeout, Duration::from_secs(5));
        assert_eq!(config.admin, AdminConfig::default());
        assert_eq!(config.admin.listen, "127.0.0.1:9187".parse().unwrap());
        assert_eq!(config.admin.listen, DEFAULT_ADMIN_LISTEN);
    }

    #[test]
    fn leadership_timers_and_admin_listen_parse_from_yaml_and_environment() {
        let yaml = format!(
            "{}admin:\n  listen: 192.168.1.5:9300\n",
            leadership("  lease_timeout: 4s\n  fence_delay: 4500ms\n  drain_timeout: 1s")
        );
        let config = parse(&yaml, with_password()).unwrap();
        assert_eq!(
            config.leadership,
            LeadershipConfig {
                lease_timeout: Duration::from_secs(4),
                fence_delay: Duration::from_millis(4500),
                drain_timeout: Duration::from_secs(1),
            }
        );
        assert_eq!(config.admin.listen, "192.168.1.5:9300".parse().unwrap());

        let mut vars = with_password();
        vars.extend(env(&[
            ("PGCDC__LEADERSHIP__FENCE_DELAY", "7s"),
            ("PGCDC__ADMIN__LISTEN", "0.0.0.0:9000"),
        ]));
        let config = parse(&yaml, vars).unwrap();
        assert_eq!(config.leadership.lease_timeout, Duration::from_secs(4));
        assert_eq!(config.leadership.fence_delay, Duration::from_secs(7));
        assert_eq!(config.leadership.drain_timeout, Duration::from_secs(1));
        assert_eq!(config.admin.listen, "0.0.0.0:9000".parse().unwrap());
    }

    #[test]
    fn fence_delay_must_exceed_lease_timeout() {
        for fence_delay in ["6s", "5s", "1ms"] {
            let yaml = leadership(&format!("  fence_delay: {fence_delay}"));
            assert_eq!(
                invalid_field(&yaml),
                "leadership.fence_delay",
                "{fence_delay}"
            );
        }
        let error = parse(&leadership("  fence_delay: 6s"), with_password()).unwrap_err();
        assert_eq!(
            error.to_string(),
            "leadership.fence_delay \"6000ms\" is invalid: must be greater than leadership.lease_timeout"
        );
        parse(&leadership("  fence_delay: 6001ms"), with_password()).unwrap();
    }

    #[test]
    fn lease_timeout_must_exceed_the_check_interval() {
        assert_eq!(
            invalid_field(&leadership("  lease_timeout: 2s")),
            "leadership.lease_timeout"
        );
        assert_eq!(
            invalid_field(&leadership("  lease_timeout: 1s")),
            "leadership.lease_timeout"
        );
        let error = parse(&leadership("  lease_timeout: 2s"), with_password()).unwrap_err();
        assert_eq!(
            error.to_string(),
            "leadership.lease_timeout \"2000ms\" is invalid: must be greater than the 2s lease check interval"
        );
        let config = parse(
            &leadership("  lease_timeout: 2001ms\n  fence_delay: 3s"),
            with_password(),
        )
        .unwrap();
        assert_eq!(config.leadership.lease_timeout, Duration::from_millis(2001));
    }

    #[test]
    fn lease_timeout_is_checked_before_fence_delay() {
        let yaml = leadership("  lease_timeout: 2s\n  fence_delay: 1s\n  drain_timeout: 0s");
        assert_eq!(invalid_field(&yaml), "leadership.lease_timeout");
        let yaml = leadership("  fence_delay: 1s\n  drain_timeout: 0s");
        assert_eq!(invalid_field(&yaml), "leadership.fence_delay");
    }

    #[test]
    fn drain_timeout_must_be_positive() {
        assert_eq!(
            invalid_field(&leadership("  drain_timeout: 0s")),
            "leadership.drain_timeout"
        );
        let error = parse(&leadership("  drain_timeout: 0s"), with_password()).unwrap_err();
        assert_eq!(
            error.to_string(),
            "leadership.drain_timeout \"0ms\" is invalid: must be greater than zero"
        );
        parse(&leadership("  drain_timeout: 1ms"), with_password()).unwrap();
    }

    #[test]
    fn admin_listen_must_be_an_ip_and_port() {
        for listen in ["localhost:9187", "9187", "127.0.0.1", "db.internal:80"] {
            let error = parse(&admin(&format!("  listen: {listen}")), with_password()).unwrap_err();
            assert!(
                matches!(error, ConfigError::Invalid(_)),
                "{listen}: {error}"
            );
            let text = error.to_string();
            assert!(text.contains("admin.listen"), "{text}");
            assert!(
                text.contains("must be an IP address and port such as 127.0.0.1:9187"),
                "{text}"
            );
        }
        let config = parse(&admin("  listen: \"[::1]:9187\""), with_password()).unwrap();
        assert_eq!(config.admin.listen, "[::1]:9187".parse().unwrap());
        let config = parse(&admin("  listen: 127.0.0.1:0"), with_password()).unwrap();
        assert_eq!(config.admin.listen.port(), 0);
    }

    #[test]
    fn unknown_leadership_and_admin_keys_are_rejected() {
        for yaml in [leadership("  lease: 6s"), admin("  port: 1")] {
            assert!(matches!(
                parse(&yaml, with_password()),
                Err(ConfigError::Invalid(_))
            ));
        }
    }

    #[test]
    fn password_is_redacted_in_debug() {
        let config = parse(MINIMAL, with_password()).unwrap();
        let debug = format!("{config:?}");
        assert!(!debug.contains("s3cr3t"), "{debug}");
    }

    #[test]
    fn load_reads_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pg-cdc.yaml");
        std::fs::write(&path, MINIMAL).unwrap();
        assert_eq!(load(&path, with_password()).unwrap().pipeline, "orders");
        assert!(matches!(
            load(&dir.path().join("missing.yaml"), with_password()),
            Err(ConfigError::Read { .. })
        ));
    }

    fn telemetry(body: &str) -> String {
        format!("{MINIMAL}telemetry:\n{body}\n")
    }

    #[test]
    fn telemetry_defaults_to_disabled_with_a_one_percent_sample() {
        let config = parse(MINIMAL, with_password()).unwrap();
        assert_eq!(config.telemetry, TelemetryConfig::default());
        assert_eq!(config.telemetry.otlp_endpoint, None);
        assert_eq!(config.telemetry.trace_sample_ratio, 0.01);
        assert_eq!(DEFAULT_TRACE_SAMPLE_RATIO, 0.01);
    }

    #[test]
    fn telemetry_parses_from_yaml_and_environment() {
        let yaml = telemetry("  otlp_endpoint: http://collector:4317\n  trace_sample_ratio: 1.0");
        let config = parse(&yaml, with_password()).unwrap();
        assert_eq!(
            config.telemetry,
            TelemetryConfig {
                otlp_endpoint: Some("http://collector:4317".to_owned()),
                trace_sample_ratio: 1.0,
            }
        );
        let mut overrides = with_password();
        overrides.extend(env(&[
            ("PGCDC__TELEMETRY__TRACE_SAMPLE_RATIO", "0.5"),
            (
                "PGCDC__TELEMETRY__OTLP_ENDPOINT",
                "https://otel.internal:4317",
            ),
        ]));
        let config = parse(&yaml, overrides).unwrap();
        assert_eq!(
            config.telemetry,
            TelemetryConfig {
                otlp_endpoint: Some("https://otel.internal:4317".to_owned()),
                trace_sample_ratio: 0.5,
            }
        );
        let integer = parse(&telemetry("  trace_sample_ratio: 1"), with_password()).unwrap();
        assert_eq!(integer.telemetry.trace_sample_ratio, 1.0);
        let from_environment = parse(
            MINIMAL,
            [
                with_password(),
                env(&[("PGCDC__TELEMETRY__OTLP_ENDPOINT", "http://env:4317")]),
            ]
            .concat(),
        )
        .unwrap();
        assert_eq!(
            from_environment.telemetry.otlp_endpoint.as_deref(),
            Some("http://env:4317")
        );
    }

    #[test]
    fn invalid_telemetry_settings_are_rejected() {
        for (body, field) in [
            ("  trace_sample_ratio: 1.5", "telemetry.trace_sample_ratio"),
            ("  trace_sample_ratio: -0.1", "telemetry.trace_sample_ratio"),
            ("  trace_sample_ratio: NaN", "telemetry.trace_sample_ratio"),
            ("  otlp_endpoint: collector:4317", "telemetry.otlp_endpoint"),
            ("  otlp_endpoint: ftp://x", "telemetry.otlp_endpoint"),
            ("  otlp_endpoint: http://", "telemetry.otlp_endpoint"),
            (
                "  otlp_endpoint: \"https://:4317\"",
                "telemetry.otlp_endpoint",
            ),
        ] {
            assert_eq!(invalid_field(&telemetry(body)), field, "{body}");
        }
        let text = parse(&telemetry("  trace_sample_ratio: abc"), with_password())
            .unwrap_err()
            .to_string();
        assert!(text.contains("telemetry.trace_sample_ratio"), "{text}");
        assert!(matches!(
            parse(&telemetry("  endpoint: x"), with_password()),
            Err(ConfigError::Invalid(_))
        ));
        let empty = parse(&telemetry("  otlp_endpoint: \"\""), with_password()).unwrap();
        assert_eq!(
            crate::telemetry::plan(&empty.telemetry, None),
            crate::telemetry::TelemetryPlan::Disabled
        );
    }
}
