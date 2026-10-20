use std::cell::RefCell;
use std::collections::BTreeSet;
use std::fmt;
use std::marker::PhantomData;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::LazyLock;
use std::time::Duration;

use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use serde::de::value::{MapAccessDeserializer, StrDeserializer, StringDeserializer};
use serde::de::{
    self, DeserializeOwned, DeserializeSeed, Deserializer, EnumAccess, MapAccess, VariantAccess,
    Visitor,
};
use serde::forward_to_deserialize_any;
use serde_json::{Map, Value};

use crate::leader::POLL_INTERVAL;
use crate::order::OrderBy;
use crate::state::s3::S3_ATTEMPT_TIMEOUT;

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
pub const DEFAULT_FENCE_DELAY: Duration = Duration::from_secs(16);
pub const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_TRACE_SAMPLE_RATIO: f64 = 0.01;
pub const DEFAULT_SENTRY_SAMPLE_RATE: f64 = 1.0;
pub const DEFAULT_ADMIN_LISTEN: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9187);
const RESERVED_ATTRIBUTES: [&str; 3] = ["schema", "table", "op"];
const TAG: &str = "type";
const TAGGED: &str = "$pg_cdc::tagged";

#[derive(Debug, Clone, Deserialize)]
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
    #[serde(skip)]
    pub unknown_keys: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
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

#[derive(Debug, Clone)]
pub enum Credentials {
    Password { password: Option<SecretString> },
    AwsRdsIam { region: Option<String> },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct TlsSection {
    #[serde(default)]
    pub mode: TlsMode,
    #[serde(default, deserialize_with = "de_opt_path")]
    pub ca_file: Option<PathBuf>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum CredentialsType {
    Password,
    AwsRdsIam,
}

#[derive(Deserialize)]
struct PasswordFields {
    #[serde(default)]
    password: Option<SecretString>,
}

#[derive(Deserialize)]
struct AwsRdsIamFields {
    #[serde(default, deserialize_with = "de_opt_string")]
    region: Option<String>,
}

impl Tagged for Credentials {
    type Tag = CredentialsType;

    fn from_fields<'de, D: Deserializer<'de>>(
        tag: CredentialsType,
        fields: D,
    ) -> Result<Self, D::Error> {
        Ok(match tag {
            CredentialsType::Password => Self::Password {
                password: PasswordFields::deserialize(fields)?.password,
            },
            CredentialsType::AwsRdsIam => Self::AwsRdsIam {
                region: AwsRdsIamFields::deserialize(fields)?.region,
            },
        })
    }
}

impl<'de> Deserialize<'de> for Credentials {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        de_tagged(deserializer)
    }
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
pub struct TelemetryConfig {
    #[serde(default, deserialize_with = "de_opt_string")]
    pub otlp_endpoint: Option<String>,
    #[serde(default = "default_trace_sample_ratio", deserialize_with = "de_ratio")]
    pub trace_sample_ratio: f64,
    #[serde(default)]
    pub sentry: SentryConfig,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            otlp_endpoint: None,
            trace_sample_ratio: DEFAULT_TRACE_SAMPLE_RATIO,
            sentry: SentryConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SentryConfig {
    #[serde(default)]
    pub dsn: Option<SecretString>,
    #[serde(default, deserialize_with = "de_opt_string")]
    pub environment: Option<String>,
    #[serde(
        default = "default_sentry_sample_rate",
        deserialize_with = "de_sample_rate"
    )]
    pub sample_rate: f64,
}

impl Default for SentryConfig {
    fn default() -> Self {
        Self {
            dsn: None,
            environment: None,
            sample_rate: DEFAULT_SENTRY_SAMPLE_RATE,
        }
    }
}

impl PartialEq for SentryConfig {
    fn eq(&self, other: &Self) -> bool {
        let dsn = |config: &Self| {
            config
                .dsn
                .as_ref()
                .map(|dsn| dsn.expose_secret().to_owned())
        };
        dsn(self) == dsn(other)
            && self.environment == other.environment
            && self.sample_rate == other.sample_rate
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StateStoreConfig {
    S3(S3StoreConfig),
    Filesystem(FilesystemStoreConfig),
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
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
pub struct FilesystemStoreConfig {
    #[serde(deserialize_with = "de_path")]
    pub path: PathBuf,
    #[serde(default = "default_store_concurrency", deserialize_with = "de_count")]
    pub max_concurrency: usize,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum StateStoreType {
    S3,
    Filesystem,
}

impl Tagged for StateStoreConfig {
    type Tag = StateStoreType;

    fn from_fields<'de, D: Deserializer<'de>>(
        tag: StateStoreType,
        fields: D,
    ) -> Result<Self, D::Error> {
        match tag {
            StateStoreType::S3 => S3StoreConfig::deserialize(fields).map(Self::S3),
            StateStoreType::Filesystem => {
                FilesystemStoreConfig::deserialize(fields).map(Self::Filesystem)
            }
        }
    }
}

impl<'de> Deserialize<'de> for StateStoreConfig {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        de_tagged(deserializer)
    }
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PayloadStoreConfig {
    S3(S3PayloadConfig),
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum PayloadStoreType {
    S3,
}

impl Tagged for PayloadStoreConfig {
    type Tag = PayloadStoreType;

    fn from_fields<'de, D: Deserializer<'de>>(
        tag: PayloadStoreType,
        fields: D,
    ) -> Result<Self, D::Error> {
        match tag {
            PayloadStoreType::S3 => S3PayloadConfig::deserialize(fields).map(Self::S3),
        }
    }
}

impl<'de> Deserialize<'de> for PayloadStoreConfig {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        de_tagged(deserializer)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkConfig {
    Stdout(BasicSinkConfig),
    Discard(BasicSinkConfig),
    Sns(SnsConfig),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
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

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum SinkType {
    Stdout,
    Discard,
    Sns,
}

impl Tagged for SinkConfig {
    type Tag = SinkType;

    fn from_fields<'de, D: Deserializer<'de>>(tag: SinkType, fields: D) -> Result<Self, D::Error> {
        match tag {
            SinkType::Stdout => BasicSinkConfig::deserialize(fields).map(Self::Stdout),
            SinkType::Discard => BasicSinkConfig::deserialize(fields).map(Self::Discard),
            SinkType::Sns => SnsConfig::deserialize(fields).map(Self::Sns),
        }
    }
}

impl<'de> Deserialize<'de> for SinkConfig {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        de_tagged(deserializer)
    }
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
    #[error("environment variable {0} has an empty key between its __ separators")]
    BadEnvKey(String),
    #[error("configuration keys cannot be mapped to environment variables: {0}")]
    Schema(String),
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
    let known = KNOWN_PATHS
        .as_ref()
        .map_err(|error| ConfigError::Schema(error.clone()))?;
    let mut unknown_keys = Vec::new();
    for (name, value) in env {
        let Some(path) = name.strip_prefix(ENV_PREFIX) else {
            continue;
        };
        let keys: Vec<String> = path.split("__").map(str::to_ascii_lowercase).collect();
        if keys.iter().any(String::is_empty) {
            return Err(ConfigError::BadEnvKey(name));
        }
        match unknown_prefix(known, &keys) {
            Some(prefix) => unknown_keys.push(prefix),
            None => set_path(&mut tree, &keys, Value::String(value)),
        }
    }
    tag_first(&mut tree);
    let mut config: Config =
        serde_ignored::deserialize(tree, |path| unknown_keys.push(dotted(&path)))?;
    config.validate()?;
    unknown_keys.sort();
    unknown_keys.dedup();
    config.unknown_keys = unknown_keys;
    Ok(config)
}

fn tag_first(node: &mut Value) {
    match node {
        Value::Object(map) => {
            if let Some(tag) = map.shift_remove(TAG) {
                map.shift_insert(0, TAG.to_owned(), tag);
            }
            map.values_mut().for_each(tag_first);
        }
        Value::Array(items) => items.iter_mut().for_each(tag_first),
        _ => {}
    }
}

fn dotted(path: &serde_ignored::Path<'_>) -> String {
    let mut keys = Vec::new();
    let mut node = path;
    loop {
        node = match node {
            serde_ignored::Path::Root => break,
            serde_ignored::Path::Map { parent, key } => {
                keys.push(key.clone());
                parent
            }
            serde_ignored::Path::Seq { parent, index } => {
                keys.push(index.to_string());
                parent
            }
            serde_ignored::Path::Some { parent }
            | serde_ignored::Path::NewtypeStruct { parent }
            | serde_ignored::Path::NewtypeVariant { parent } => parent,
        };
    }
    keys.reverse();
    keys.join(".")
}

struct FieldsSeed<T: Tagged>(T::Tag);

impl<'de, T: Tagged> DeserializeSeed<'de> for FieldsSeed<T> {
    type Value = T;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<T, D::Error> {
        T::from_fields(self.0, deserializer)
    }
}

trait Tagged: Sized {
    type Tag: DeserializeOwned;

    fn from_fields<'de, D: Deserializer<'de>>(tag: Self::Tag, fields: D) -> Result<Self, D::Error>;
}

fn de_tagged<'de, T: Tagged, D: Deserializer<'de>>(deserializer: D) -> Result<T, D::Error> {
    struct TaggedVisitor<T>(PhantomData<T>);

    impl<'de, T: Tagged> Visitor<'de> for TaggedVisitor<T> {
        type Value = T;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a map with a type key")
        }

        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<T, A::Error> {
            if map.next_key::<String>()?.as_deref() != Some(TAG) {
                return Err(de::Error::missing_field(TAG));
            }
            let tag: String = map.next_value()?;
            let tag = T::Tag::deserialize(StringDeserializer::<A::Error>::new(tag))?;
            T::from_fields(tag, MapAccessDeserializer::new(map))
        }

        fn visit_enum<A: EnumAccess<'de>>(self, data: A) -> Result<T, A::Error> {
            let (tag, fields) = data.variant()?;
            fields.newtype_variant_seed(FieldsSeed::<T>(tag))
        }
    }

    deserializer.deserialize_struct(TAGGED, &[TAG], TaggedVisitor(PhantomData))
}

fn set_path(tree: &mut Value, keys: &[String], value: Value) {
    let mut node = tree;
    for key in keys {
        if node.is_null() {
            *node = Value::Object(Map::new());
        }
        let Some(map) = node.as_object_mut() else {
            return;
        };
        node = map.entry(key.clone()).or_insert(Value::Null);
    }
    *node = value;
}

fn unknown_prefix(known: &BTreeSet<Vec<String>>, keys: &[String]) -> Option<String> {
    let mut prefix = Vec::new();
    for key in keys {
        prefix.push(key.clone());
        if !known.contains(&prefix) {
            return Some(prefix.join("."));
        }
    }
    None
}

static KNOWN_PATHS: LazyLock<Result<BTreeSet<Vec<String>>, String>> =
    LazyLock::new(known_paths::<Config>);

fn known_paths<T: DeserializeOwned>() -> Result<BTreeSet<Vec<String>>, String> {
    let schema = RefCell::new(Schema::default());
    let mut next = vec![Vec::new()];
    while let Some(route) = next.pop() {
        let probe = Probe {
            done: Vec::new(),
            rest: &route,
            path: Vec::new(),
            schema: &schema,
        };
        let _ = T::deserialize(probe);
        next.append(&mut schema.borrow_mut().pending);
    }
    let schema = schema.into_inner();
    if let Some(path) = schema.unsupported.first() {
        return Err(format!(
            "{path} is not a plain struct or a tagged section, for example it uses #[serde(flatten)] or is a map"
        ));
    }
    if schema.known.is_empty() {
        return Err("no configuration keys were found".to_owned());
    }
    Ok(schema.known)
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Step {
    Key(&'static str),
    Variant(&'static str),
}

#[derive(Default)]
struct Schema {
    known: BTreeSet<Vec<String>>,
    routes: BTreeSet<Vec<Step>>,
    pending: Vec<Vec<Step>>,
    unsupported: Vec<String>,
}

impl Schema {
    fn visit(&mut self, done: &[Step], step: Step) {
        let mut route = done.to_vec();
        route.push(step);
        if self.routes.insert(route.clone()) {
            self.pending.push(route);
        }
    }
}

struct Probe<'a> {
    done: Vec<Step>,
    rest: &'a [Step],
    path: Vec<&'static str>,
    schema: &'a RefCell<Schema>,
}

impl<'a> Probe<'a> {
    fn advance(&self, step: &Step, rest: &'a [Step], key: Option<&'static str>) -> Self {
        let mut path = self.path.clone();
        path.extend(key);
        let mut done = self.done.clone();
        done.push(step.clone());
        Self {
            done,
            rest,
            path,
            schema: self.schema,
        }
    }

    fn unsupported<T>(&self) -> Result<T, de::value::Error> {
        let path = if self.path.is_empty() {
            "the root".to_owned()
        } else {
            self.path.join(".")
        };
        self.schema.borrow_mut().unsupported.push(path);
        leaf()
    }

    fn know(&self, key: &str) {
        let mut path: Vec<String> = self.path.iter().map(|&key| key.to_owned()).collect();
        path.push(key.to_owned());
        self.schema.borrow_mut().known.insert(path);
    }
}

fn leaf<T>() -> Result<T, de::value::Error> {
    Err(de::Error::custom("leaf"))
}

impl<'de> Deserializer<'de> for Probe<'_> {
    type Error = de::value::Error;

    fn deserialize_any<V: Visitor<'de>>(self, _: V) -> Result<V::Value, Self::Error> {
        leaf()
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_some(self)
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        if name == TAGGED {
            return match self.rest.first() {
                None | Some(Step::Variant(_)) => {
                    self.know(TAG);
                    visitor.visit_enum(self)
                }
                Some(Step::Key(_)) => self.unsupported(),
            };
        }
        match self.rest.split_first() {
            None => {
                for &field in fields {
                    self.know(field);
                    self.schema.borrow_mut().visit(&self.done, Step::Key(field));
                }
                leaf()
            }
            Some((step @ &Step::Key(key), rest)) => {
                visitor.visit_map(ProbeEntry(Some((key, self.advance(step, rest, Some(key))))))
            }
            Some((Step::Variant(_), _)) => self.unsupported(),
        }
    }

    fn deserialize_map<V: Visitor<'de>>(self, _: V) -> Result<V::Value, Self::Error> {
        self.unsupported()
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf unit
        unit_struct seq tuple tuple_struct enum identifier ignored_any
    }
}

struct ProbeEntry<'a>(Option<(&'static str, Probe<'a>)>);

impl<'de> MapAccess<'de> for ProbeEntry<'_> {
    type Error = de::value::Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, Self::Error> {
        match &self.0 {
            Some((key, _)) => seed.deserialize(StrDeserializer::new(key)).map(Some),
            None => Ok(None),
        }
    }

    fn next_value_seed<S: DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> Result<S::Value, Self::Error> {
        match self.0.take() {
            Some((_, probe)) => seed.deserialize(probe),
            None => leaf(),
        }
    }
}

impl<'de> EnumAccess<'de> for Probe<'_> {
    type Error = de::value::Error;
    type Variant = Self;

    fn variant_seed<S: DeserializeSeed<'de>>(
        self,
        seed: S,
    ) -> Result<(S::Value, Self), Self::Error> {
        let tag = seed.deserialize(ProbeTag(&self))?;
        let Some((step, rest)) = self.rest.split_first() else {
            return leaf();
        };
        Ok((tag, self.advance(step, rest, None)))
    }
}

impl<'de> VariantAccess<'de> for Probe<'_> {
    type Error = de::value::Error;

    fn unit_variant(self) -> Result<(), Self::Error> {
        leaf()
    }

    fn newtype_variant_seed<S: DeserializeSeed<'de>>(
        self,
        seed: S,
    ) -> Result<S::Value, Self::Error> {
        seed.deserialize(self)
    }

    fn tuple_variant<V: Visitor<'de>>(self, _: usize, _: V) -> Result<V::Value, Self::Error> {
        leaf()
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        _: &'static [&'static str],
        _: V,
    ) -> Result<V::Value, Self::Error> {
        leaf()
    }
}

struct ProbeTag<'p, 'a>(&'p Probe<'a>);

impl<'de> Deserializer<'de> for ProbeTag<'_, '_> {
    type Error = de::value::Error;

    fn deserialize_any<V: Visitor<'de>>(self, _: V) -> Result<V::Value, Self::Error> {
        leaf()
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        let probe = self.0;
        match probe.rest.first() {
            None => {
                for &variant in variants {
                    probe
                        .schema
                        .borrow_mut()
                        .visit(&probe.done, Step::Variant(variant));
                }
                leaf()
            }
            Some(&Step::Variant(variant)) => visitor.visit_enum(StrDeserializer::new(variant)),
            Some(Step::Key(_)) => probe.unsupported(),
        }
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf option
        unit unit_struct newtype_struct seq tuple tuple_struct map struct identifier ignored_any
    }
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
        if matches!(self.state_store, StateStoreConfig::S3(_))
            && self.leadership.fence_delay
                < self
                    .leadership
                    .lease_timeout
                    .saturating_add(S3_ATTEMPT_TIMEOUT)
        {
            return Err(invalid_duration(
                "leadership.fence_delay",
                self.leadership.fence_delay,
                "must be at least leadership.lease_timeout plus the 10s S3 attempt timeout when the state store is S3",
            ));
        }
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
                return Err(ConfigError::InvalidValue {
                    field: "telemetry.otlp_endpoint",
                    value: endpoint.to_owned(),
                    reason: "must be an http:// or https:// URL",
                });
            }
            _ => {}
        }
        self.sentry.validate()
    }
}

impl SentryConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if !(0.0..=1.0).contains(&self.sample_rate) {
            return Err(ConfigError::InvalidValue {
                field: "telemetry.sentry.sample_rate",
                value: self.sample_rate.to_string(),
                reason: "must be a number from 0 to 1",
            });
        }
        match self.dsn.as_ref().map(ExposeSecret::expose_secret) {
            Some(dsn) if !dsn.is_empty() && !is_http_url(dsn) => Err(ConfigError::InvalidValue {
                field: "telemetry.sentry.dsn",
                value: "[redacted]".to_owned(),
                reason: "must be an http:// or https:// Sentry DSN",
            }),
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

fn default_sentry_sample_rate() -> f64 {
    DEFAULT_SENTRY_SAMPLE_RATE
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
    deserializer.deserialize_any(RatioVisitor("telemetry.trace_sample_ratio"))
}

fn de_sample_rate<'de, D: Deserializer<'de>>(deserializer: D) -> Result<f64, D::Error> {
    deserializer.deserialize_any(RatioVisitor("telemetry.sentry.sample_rate"))
}

struct RatioVisitor(&'static str);

impl RatioVisitor {
    fn not_a_number<E: de::Error>(&self, text: &str) -> E {
        E::custom(format!("{} {text:?} must be a number from 0 to 1", self.0))
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
            .map_err(|_| self.not_a_number(&value.to_string()))
    }

    fn visit_i64<E: de::Error>(self, value: i64) -> Result<f64, E> {
        i32::try_from(value)
            .map(f64::from)
            .map_err(|_| self.not_a_number(&value.to_string()))
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<f64, E> {
        value.trim().parse().map_err(|_| self.not_a_number(value))
    }
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
    fn rejects_unknown_types_and_malformed_yaml() {
        for yaml in [
            MINIMAL.replace("type: stdout", "type: kafka"),
            MINIMAL.replace("type: stdout", "type: {stdout: null}"),
            MINIMAL.replace("type: password", "type: kerberos"),
            MINIMAL.replace("type: password", "type: {password: null}"),
            MINIMAL.replace("type: filesystem", "type: nfs"),
            MINIMAL.replace("type: filesystem", "type: {filesystem: null}"),
            format!("{MINIMAL}payload_store:\n  type: gcs\n  bucket: b\n"),
            format!("{MINIMAL}payload_store:\n  type: {{s3: null}}\n  bucket: b\n"),
        ] {
            assert!(
                matches!(parse(&yaml, with_password()), Err(ConfigError::Invalid(_))),
                "{yaml}"
            );
        }
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
        assert!(matches!(
            parse(&sns("  fair_queue_group: tenant\n"), with_password()),
            Err(ConfigError::Invalid(_))
        ));
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
            assert_eq!(
                parse(&unknown, with_password()).unwrap().unknown_keys,
                ["sink.topic_arn"],
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
        assert_eq!(config.leadership.fence_delay, Duration::from_secs(16));
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
    fn an_s3_state_store_needs_a_fence_delay_covering_the_lease_and_one_s3_attempt() {
        let s3 = |timers: &str| {
            format!(
                "{}leadership:\n{timers}\n",
                stores("state_store:\n  type: s3\n  bucket: b\n")
            )
        };
        let error = parse(
            &s3("  lease_timeout: 6s\n  fence_delay: 15999ms"),
            with_password(),
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "leadership.fence_delay \"15999ms\" is invalid: must be at least leadership.lease_timeout plus the 10s S3 attempt timeout when the state store is S3"
        );
        let config = parse(
            &s3("  lease_timeout: 6s\n  fence_delay: 16s"),
            with_password(),
        )
        .unwrap();
        assert_eq!(config.leadership.fence_delay, Duration::from_secs(16));
        let defaults = parse(
            &stores("state_store:\n  type: s3\n  bucket: b\n"),
            with_password(),
        )
        .unwrap();
        assert!(
            defaults.leadership.fence_delay
                >= defaults.leadership.lease_timeout + S3_ATTEMPT_TIMEOUT
        );
        parse(
            &leadership("  lease_timeout: 6s\n  fence_delay: 7s"),
            with_password(),
        )
        .unwrap();
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

    fn unknown_keys(yaml: &str, vars: &[(&str, &str)]) -> Vec<String> {
        let mut all = with_password();
        all.extend(env(vars));
        parse(yaml, all).unwrap().unknown_keys
    }

    #[test]
    fn the_minimal_config_has_no_unknown_keys() {
        assert_eq!(unknown_keys(MINIMAL, &[]), [] as [&str; 0]);
    }

    #[test]
    fn unknown_keys_in_every_section_are_ignored_and_named_by_their_dotted_path() {
        let s3_store = "state_store:\n  type: s3\n  bucket: b\n";
        for (yaml, key) in [
            (
                format!("{MINIMAL}future_section:\n  enabled: true\n"),
                "future_section",
            ),
            (
                MINIMAL.replace("sink:", "delivery:\n  order_by_tables: orders\nsink:"),
                "delivery.order_by_tables",
            ),
            (
                MINIMAL.replace("  slot:", "  sslmode: require\n  slot:"),
                "source.sslmode",
            ),
            (
                MINIMAL.replace("type: password", "type: password\n    region: x"),
                "source.credentials.region",
            ),
            (
                MINIMAL.replace("  slot:", "  tls:\n    verify: no\n  slot:"),
                "source.tls.verify",
            ),
            (sns("  topic: x\n"), "sink.topic"),
            (
                stores(&format!("{s3_store}  path: /x\n")),
                "state_store.path",
            ),
            (
                stores(&format!(
                    "{s3_store}payload_store:\n  type: s3\n  bucket: p\n  kms_key: k\n"
                )),
                "payload_store.kms_key",
            ),
            (leadership("  lease: 6s"), "leadership.lease"),
            (admin("  port: 1"), "admin.port"),
            (telemetry("  endpoint: x"), "telemetry.endpoint"),
            (
                telemetry("  sentry:\n    traces: 1"),
                "telemetry.sentry.traces",
            ),
        ] {
            assert_eq!(unknown_keys(&yaml, &[]), [key], "{yaml}");
        }
    }

    #[test]
    fn unknown_environment_keys_are_ignored_and_named_by_their_dotted_path() {
        for (vars, key) in [
            (
                vec![("PGCDC__DELIVERY__ORDER_BY_TABLES", "orders")],
                "delivery.order_by_tables",
            ),
            (vec![("PGCDC__FUTURE__ENABLED", "true")], "future"),
            (vec![("PGCDC__PIPELINE__NAME", "x")], "pipeline.name"),
            (vec![("PGCDC__SINK__TOPIC", "x")], "sink.topic"),
            (
                vec![("PGCDC__SOURCE__CREDENTIALS__REGION", "us-east-1")],
                "source.credentials.region",
            ),
        ] {
            assert_eq!(unknown_keys(MINIMAL, &vars), [key], "{vars:?}");
        }
    }

    #[test]
    fn a_state_store_type_set_after_its_fields_still_finds_unknown_keys() {
        let vars = [
            ("PGCDC__STATE_STORE__BUCKET", "from-env"),
            ("PGCDC__STATE_STORE__PATH", "/x"),
            ("PGCDC__STATE_STORE__TYPE", "s3"),
        ];
        let mut all = with_password();
        all.extend(env(&vars));
        let config = parse(&stores(""), all).unwrap();
        assert_eq!(config.unknown_keys, ["state_store.path"]);
        assert!(matches!(&config.state_store, StateStoreConfig::S3(s3) if s3.bucket == "from-env"));
    }

    #[test]
    fn unknown_keys_are_sorted_and_listed_once() {
        let yaml = format!(
            "{}zeta: 1\nalpha:\n  nested: true\n",
            MINIMAL.replace("sink:", "delivery:\n  order_by_tables: orders\nsink:")
        );
        let keys = unknown_keys(
            &yaml,
            &[
                ("PGCDC__DELIVERY__ORDER_BY_TABLES", "orders"),
                ("PGCDC__ALPHA__OTHER", "x"),
            ],
        );
        assert_eq!(keys, ["alpha", "delivery.order_by_tables", "zeta"]);
    }

    #[test]
    fn unknown_keys_never_carry_their_values() {
        let yaml = format!("{MINIMAL}future_section: hunter2-file\n");
        let keys = unknown_keys(&yaml, &[("PGCDC__SINK__TOKEN", "hunter2-env")]);
        assert_eq!(keys, ["future_section", "sink.token"]);
        assert!(keys.iter().all(|key| !key.contains("hunter2")), "{keys:?}");
    }

    #[test]
    fn a_known_key_with_an_invalid_value_still_fails_beside_unknown_keys() {
        let yaml = format!(
            "{}future_section: 1\n",
            MINIMAL.replace("sink:", "delivery:\n  lanes: many\nsink:")
        );
        assert!(matches!(
            parse(&yaml, with_password()),
            Err(ConfigError::Invalid(_))
        ));
        let mut vars = with_password();
        vars.extend(env(&[
            ("PGCDC__DELIVERY__ORDER_BY_TABLES", "orders"),
            ("PGCDC__DELIVERY__LANES", "0"),
        ]));
        assert!(matches!(
            parse(MINIMAL, vars),
            Err(ConfigError::InvalidValue {
                field: "delivery.lanes",
                ..
            })
        ));
        let mut vars = with_password();
        vars.extend(env(&[
            ("PGCDC__FUTURE__ENABLED", "true"),
            ("PGCDC__SINK__TYPE", "kafka"),
        ]));
        assert!(matches!(parse(MINIMAL, vars), Err(ConfigError::Invalid(_))));
    }

    #[test]
    fn unknown_environment_paths_never_reach_the_tree() {
        for (vars, key) in [
            (
                vec![("PGCDC__SOURCE__PORT__FUTURE", "x")],
                "source.port.future",
            ),
            (
                vec![("PGCDC__SOURCE__TLS__MODE__DISABLE", "review-secret-canary")],
                "source.tls.mode.disable",
            ),
            (vec![("PGCDC__SINK__TYPE__SNS", "x")], "sink.type.sns"),
        ] {
            assert_eq!(unknown_keys(MINIMAL, &vars), [key], "{vars:?}");
        }
        let null_delivery = MINIMAL.replace("sink:", "delivery:\nsink:");
        assert!(matches!(
            parse(&null_delivery, with_password()),
            Err(ConfigError::Invalid(_))
        ));
        let mut vars = with_password();
        vars.extend(env(&[("PGCDC__DELIVERY__FUTURE", "x")]));
        assert!(matches!(
            parse(&null_delivery, vars),
            Err(ConfigError::Invalid(_))
        ));
    }

    #[test]
    fn errors_never_carry_values_from_unknown_environment_paths() {
        let canary = "review-secret-canary";
        let invalid_sink = MINIMAL.replace("type: stdout", "type: kafka");
        let null_delivery = MINIMAL.replace("sink:", "delivery:\nsink:");
        for yaml in [&invalid_sink, &null_delivery] {
            for name in [
                "PGCDC__SOURCE__PORT__FUTURE",
                "PGCDC__SOURCE__TLS__MODE__DISABLE",
                "PGCDC__DELIVERY__FUTURE",
                "PGCDC__DELIVERY__LANES__MAX",
                "PGCDC__PIPELINE__NAME",
                "PGCDC__SINK__TYPE__KAFKA",
                "PGCDC__FUTURE__ENABLED",
            ] {
                let mut vars = with_password();
                vars.push((name.to_owned(), canary.to_owned()));
                let error = parse(yaml, vars).unwrap_err().to_string();
                assert!(!error.contains(canary), "{name}: {error}");
            }
        }
    }

    #[test]
    fn known_environment_keys_under_omitted_sections_still_apply() {
        let mut vars = with_password();
        vars.extend(env(&[
            ("PGCDC__DELIVERY__PARK_AFTER", "60s"),
            ("PGCDC__SOURCE__TLS__MODE", "disable"),
            ("PGCDC__SOURCE__PORT", "5433"),
            ("PGCDC__PAYLOAD_STORE__TYPE", "s3"),
            ("PGCDC__PAYLOAD_STORE__BUCKET", "payloads"),
        ]));
        let config = parse(MINIMAL, vars).unwrap();
        assert_eq!(config.delivery.park_after, Duration::from_secs(60));
        assert_eq!(config.source.tls.mode, TlsMode::Disable);
        assert_eq!(config.source.port, 5433);
        assert!(config.payload_store.is_some());
        assert_eq!(config.unknown_keys, [] as [&str; 0]);
    }

    fn path(dotted: &str) -> Vec<String> {
        dotted.split('.').map(str::to_owned).collect()
    }

    #[test]
    fn discovery_maps_the_full_configuration() {
        let known = known_paths::<Config>().unwrap();
        for dotted in [
            "pipeline",
            "source.port",
            "source.tls.mode",
            "source.credentials.type",
            "source.credentials.password",
            "source.credentials.region",
            "sink.type",
            "sink.topic_arn",
            "sink.attribute_columns",
            "state_store.path",
            "state_store.bucket",
            "payload_store.bucket",
            "delivery.park_after",
            "leadership.lease_timeout",
            "admin.listen",
            "telemetry.sentry.dsn",
        ] {
            assert!(known.contains(&path(dotted)), "{dotted}");
        }
    }

    #[test]
    fn every_discovered_key_is_lowercase() {
        for keys in known_paths::<Config>().unwrap() {
            for key in &keys {
                assert_eq!(*key, key.to_ascii_lowercase(), "{keys:?}");
            }
        }
    }

    #[test]
    fn discovery_rejects_flattened_sections() {
        #[derive(Deserialize)]
        struct Shared {
            _name: String,
        }

        #[derive(Deserialize)]
        struct Section {
            #[serde(flatten)]
            _shared: Shared,
        }

        #[derive(Deserialize)]
        struct Outer {
            _pipeline: String,
            _section: Option<Section>,
        }

        let error = known_paths::<Outer>().unwrap_err();
        assert!(error.contains("_section"), "{error}");
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
                sentry: SentryConfig::default(),
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
                sentry: SentryConfig::default(),
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
    fn sentry_is_off_by_default_and_parses_from_yaml_and_environment() {
        let config = parse(MINIMAL, with_password()).unwrap();
        assert!(config.telemetry.sentry.dsn.is_none());
        assert_eq!(config.telemetry.sentry.environment, None);
        assert_eq!(config.telemetry.sentry.sample_rate, 1.0);
        let yaml = telemetry(
            "  sentry:\n    dsn: https://public@o1.ingest.sentry.io/2\n    environment: staging\n    sample_rate: 0.25",
        );
        let config = parse(&yaml, with_password()).unwrap();
        let sentry = &config.telemetry.sentry;
        assert_eq!(
            sentry.dsn.as_ref().map(ExposeSecret::expose_secret),
            Some("https://public@o1.ingest.sentry.io/2")
        );
        assert_eq!(sentry.environment.as_deref(), Some("staging"));
        assert_eq!(sentry.sample_rate, 0.25);
        let overridden = parse(
            MINIMAL,
            [
                with_password(),
                env(&[
                    (
                        "PGCDC__TELEMETRY__SENTRY__DSN",
                        "https://env@sentry.internal/3",
                    ),
                    ("PGCDC__TELEMETRY__SENTRY__SAMPLE_RATE", "0.5"),
                ]),
            ]
            .concat(),
        )
        .unwrap();
        assert_eq!(
            overridden
                .telemetry
                .sentry
                .dsn
                .as_ref()
                .map(ExposeSecret::expose_secret),
            Some("https://env@sentry.internal/3")
        );
        assert_eq!(overridden.telemetry.sentry.sample_rate, 0.5);
    }

    #[test]
    fn invalid_sentry_settings_are_rejected() {
        for (body, field) in [
            (
                "  sentry:\n    sample_rate: 1.5",
                "telemetry.sentry.sample_rate",
            ),
            ("  sentry:\n    dsn: not-a-url", "telemetry.sentry.dsn"),
        ] {
            assert_eq!(invalid_field(&telemetry(body)), field, "{body}");
        }
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
        let empty = parse(&telemetry("  otlp_endpoint: \"\""), with_password()).unwrap();
        assert_eq!(
            crate::telemetry::plan(&empty.telemetry, None),
            crate::telemetry::TelemetryPlan::Disabled
        );
    }
}
