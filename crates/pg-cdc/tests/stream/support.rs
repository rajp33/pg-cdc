use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use pg_cdc::config::{
    DEFAULT_DRAIN_TIMEOUT, DEFAULT_MAX_INFLIGHT_BYTES, DEFAULT_MAX_PARKED_EVENTS,
    DEFAULT_PARK_AFTER, LeadershipConfig, TlsMode,
};
use pg_cdc::connection::{ConnectParams, PasswordProvider, TlsConfig};
use pg_cdc::event::EventId;
use pg_cdc::health::Health;
use pg_cdc::lane::Stores;
use pg_cdc::order::{OrderBy, OrderKeyId};
use pg_cdc::session::{SessionConfig, SessionContext, SessionError, run_session};
use pg_cdc::sink::stdout::StdoutSink;
use pg_cdc::sink::{Outbound, Outcome, Sink, SinkCapabilities, SinkError};
use pg_cdc::state::filesystem::FilesystemStateStore;
use pg_cdc::state::memory::MemoryStateStore;
use pg_cdc::state::{
    LeaderRecord, ParkStatus, ParkedEvent, ParkedHead, StateStore, StoreError, SwapOutcome,
    Version, Versioned,
};
use pg_cdc_testkit::Postgres;
use secrecy::SecretString;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;

pub(crate) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub(crate) const SLOT: &str = "it_slot";
pub(crate) const PUBLICATION: &str = "it_pub";
pub(crate) const PIPELINE: &str = "it";
pub(crate) const WAIT: Duration = Duration::from_secs(30);

pub(crate) const TEST_LEADERSHIP: LeadershipConfig = LeadershipConfig {
    lease_timeout: Duration::from_secs(4),
    fence_delay: Duration::from_millis(4500),
    drain_timeout: DEFAULT_DRAIN_TIMEOUT,
};

pub(crate) async fn prepare(pg: &Postgres, statements: &[&str], tables: &[&str]) -> TestResult {
    for statement in statements {
        pg.execute(statement).await?;
    }
    pg.create_publication(PUBLICATION, tables).await?;
    pg.create_slot(SLOT).await?;
    Ok(())
}

pub(crate) fn params(pg: &Postgres) -> ConnectParams {
    ConnectParams {
        host: pg.host().to_owned(),
        port: pg.port(),
        database: pg.database().to_owned(),
        user: pg.user().to_owned(),
        password: Some(SecretString::from(pg.password().to_owned())),
        tls: TlsConfig {
            mode: TlsMode::Disable,
            ca_file: None,
        },
    }
}

pub(crate) fn params_via(proxy: &ControlProxy, pg: &Postgres) -> ConnectParams {
    ConnectParams {
        host: "127.0.0.1".to_owned(),
        port: proxy.port(),
        ..params(pg)
    }
}

pub(crate) fn config(status_interval: Duration, buffer: usize) -> SessionConfig {
    SessionConfig {
        pipeline: PIPELINE.to_owned(),
        slot: SLOT.to_owned(),
        publication: PUBLICATION.to_owned(),
        status_interval,
        buffer,
        order_by: OrderBy::Pipeline,
        lanes: 1,
        sink_concurrency: 4,
        attribute_columns: Vec::new(),
        max_inflight_bytes: DEFAULT_MAX_INFLIGHT_BYTES,
        park_after: DEFAULT_PARK_AFTER,
        max_parked_events: DEFAULT_MAX_PARKED_EVENTS,
        store_concurrency: 4,
        leadership: TEST_LEADERSHIP,
    }
}

pub(crate) fn context() -> SessionContext {
    context_for("test-holder")
}

pub(crate) fn context_for(holder: &str) -> SessionContext {
    let health = Health::new();
    health.set_sink_ready(true);
    health.set_store_ready(true);
    SessionContext {
        holder: holder.to_owned(),
        health,
    }
}

pub(crate) fn filesystem_stores(path: &std::path::Path) -> Stores {
    Stores {
        state: Arc::new(FilesystemStateStore::new(path, PIPELINE, 4)),
        payload: None,
    }
}

pub(crate) async fn parked(stores: &Stores, key: &[u8]) -> TestResult<Vec<ParkedEvent>> {
    Ok(stores
        .state
        .read_parked(&OrderKeyId::of(key), None, 100)
        .await?)
}

pub(crate) fn order_key(id: i64) -> Vec<u8> {
    format!(r#"["public","orders",{id}]"#).into_bytes()
}

pub(crate) const LIMITS: SinkCapabilities = SinkCapabilities {
    max_batch_messages: 10,
    max_batch_bytes: 1 << 20,
    max_message_bytes: 1 << 20,
};

pub(crate) fn memory_stores() -> Stores {
    Stores {
        state: Arc::new(MemoryStateStore::new()),
        payload: None,
    }
}

#[derive(Debug, Default)]
pub(crate) struct UnparkableStore(MemoryStateStore);

#[async_trait]
impl StateStore for UnparkableStore {
    async fn append_parked(&self, _events: &[ParkedEvent]) -> Result<(), StoreError> {
        Err(StoreError::Unavailable(
            "the test store refuses to park".to_owned(),
        ))
    }

    async fn load_parked_heads(&self, partition: u8) -> Result<Vec<ParkedHead>, StoreError> {
        self.0.load_parked_heads(partition).await
    }

    async fn list_parked_queues(
        &self,
        after: Option<&OrderKeyId>,
        limit: usize,
        length_cap: usize,
    ) -> Result<Vec<ParkedHead>, StoreError> {
        self.0.list_parked_queues(after, limit, length_cap).await
    }

    async fn read_parked(
        &self,
        key: &OrderKeyId,
        after: Option<&EventId>,
        limit: usize,
    ) -> Result<Vec<ParkedEvent>, StoreError> {
        self.0.read_parked(key, after, limit).await
    }

    async fn set_parked_status(
        &self,
        key: &OrderKeyId,
        id: &EventId,
        status: ParkStatus,
    ) -> Result<(), StoreError> {
        self.0.set_parked_status(key, id, status).await
    }

    async fn remove_parked(&self, events: &[(OrderKeyId, EventId)]) -> Result<(), StoreError> {
        self.0.remove_parked(events).await
    }

    async fn read_leader(&self) -> Result<Option<Versioned<LeaderRecord>>, StoreError> {
        self.0.read_leader().await
    }

    async fn swap_leader(
        &self,
        next: &LeaderRecord,
        expected: Option<&Version>,
    ) -> Result<SwapOutcome, StoreError> {
        self.0.swap_leader(next, expected).await
    }
}

pub(crate) fn unparkable_stores() -> Stores {
    Stores {
        state: Arc::new(UnparkableStore::default()),
        payload: None,
    }
}

#[derive(Debug)]
pub(crate) struct RecordingStore {
    inner: Arc<dyn StateStore>,
    calls: Mutex<Vec<&'static str>>,
    detach: Mutex<Option<Duration>>,
    landed: Arc<AtomicUsize>,
    stalled: AtomicBool,
}

impl RecordingStore {
    pub(crate) fn over(inner: Arc<dyn StateStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            calls: Mutex::default(),
            detach: Mutex::default(),
            landed: Arc::new(AtomicUsize::new(0)),
            stalled: AtomicBool::new(false),
        })
    }

    pub(crate) fn stall_leader_swaps(&self) {
        self.stalled.store(true, Ordering::SeqCst);
    }

    pub(crate) fn detach_appends(&self, delay: Duration) {
        *self.detach.lock().unwrap_or_else(PoisonError::into_inner) = Some(delay);
    }

    pub(crate) fn landed_appends(&self) -> usize {
        self.landed.load(Ordering::SeqCst)
    }

    pub(crate) async fn wait_for_call(&self, method: &'static str) -> TestResult {
        tokio::time::timeout(WAIT, async {
            while !self.calls().contains(&method) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await?;
        Ok(())
    }

    pub(crate) async fn wait_for_landed(&self, count: usize) -> TestResult {
        tokio::time::timeout(WAIT, async {
            while self.landed_appends() < count {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await?;
        Ok(())
    }

    pub(crate) fn calls(&self) -> Vec<&'static str> {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn stores(self: &Arc<Self>) -> Stores {
        let state: Arc<dyn StateStore> = Arc::clone(self) as Arc<dyn StateStore>;
        Stores {
            state,
            payload: None,
        }
    }

    fn record(&self, method: &'static str) {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(method);
    }
}

#[async_trait]
impl StateStore for RecordingStore {
    async fn append_parked(&self, events: &[ParkedEvent]) -> Result<(), StoreError> {
        self.record("append_parked");
        let delay = *self.detach.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(delay) = delay else {
            return self.inner.append_parked(events).await;
        };
        let inner = Arc::clone(&self.inner);
        let events = events.to_vec();
        let landed = Arc::clone(&self.landed);
        let write = tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let result = inner.append_parked(&events).await;
            landed.fetch_add(1, Ordering::SeqCst);
            result
        });
        write
            .await
            .map_err(|error| StoreError::Unavailable(error.to_string()))?
    }

    async fn load_parked_heads(&self, partition: u8) -> Result<Vec<ParkedHead>, StoreError> {
        self.record("load_parked_heads");
        self.inner.load_parked_heads(partition).await
    }

    async fn list_parked_queues(
        &self,
        after: Option<&OrderKeyId>,
        limit: usize,
        length_cap: usize,
    ) -> Result<Vec<ParkedHead>, StoreError> {
        self.record("list_parked_queues");
        self.inner
            .list_parked_queues(after, limit, length_cap)
            .await
    }

    async fn read_parked(
        &self,
        key: &OrderKeyId,
        after: Option<&EventId>,
        limit: usize,
    ) -> Result<Vec<ParkedEvent>, StoreError> {
        self.record("read_parked");
        self.inner.read_parked(key, after, limit).await
    }

    async fn set_parked_status(
        &self,
        key: &OrderKeyId,
        id: &EventId,
        status: ParkStatus,
    ) -> Result<(), StoreError> {
        self.record("set_parked_status");
        self.inner.set_parked_status(key, id, status).await
    }

    async fn remove_parked(&self, events: &[(OrderKeyId, EventId)]) -> Result<(), StoreError> {
        self.record("remove_parked");
        self.inner.remove_parked(events).await
    }

    async fn read_leader(&self) -> Result<Option<Versioned<LeaderRecord>>, StoreError> {
        self.record("read_leader");
        self.inner.read_leader().await
    }

    async fn swap_leader(
        &self,
        next: &LeaderRecord,
        expected: Option<&Version>,
    ) -> Result<SwapOutcome, StoreError> {
        self.record("swap_leader");
        if self.stalled.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        self.inner.swap_leader(next, expected).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Mode {
    Deliver,
    Hold,
    Fail,
    FailContaining(String),
    RejectContaining(String),
}

#[derive(Debug)]
pub(crate) struct TestSink {
    limits: SinkCapabilities,
    mode: watch::Receiver<Mode>,
    failures: watch::Sender<u64>,
    delivered: mpsc::UnboundedSender<Bytes>,
    calls: watch::Sender<u64>,
}

fn contains(body: &[u8], marker: &str) -> bool {
    body.windows(marker.len())
        .any(|window| window == marker.as_bytes())
}

#[async_trait]
impl Sink for TestSink {
    fn capabilities(&self) -> SinkCapabilities {
        self.limits
    }

    async fn preflight(&self) -> Result<(), SinkError> {
        Ok(())
    }

    async fn send(&self, batch: &[Outbound]) -> Vec<Outcome> {
        self.calls.send_modify(|count| *count += 1);
        let mut mode = self.mode.clone();
        let Ok(current) = mode
            .wait_for(|mode| *mode != Mode::Hold)
            .await
            .map(|mode| mode.clone())
        else {
            return vec![Outcome::Retryable(SinkError("test sink closed".to_owned())); batch.len()];
        };
        batch
            .iter()
            .map(|outbound| {
                let fail = match &current {
                    Mode::Fail => true,
                    Mode::FailContaining(marker) => contains(&outbound.body, marker),
                    Mode::Deliver | Mode::Hold | Mode::RejectContaining(_) => false,
                };
                let reject = matches!(&current, Mode::RejectContaining(marker) if contains(&outbound.body, marker));
                if reject {
                    self.failures.send_modify(|count| *count += 1);
                    Outcome::Terminal(SinkError("test sink rejects the event".to_owned()))
                } else if fail {
                    self.failures.send_modify(|count| *count += 1);
                    Outcome::Retryable(SinkError("test sink is failing".to_owned()))
                } else {
                    let _ = self.delivered.send(outbound.body.clone());
                    Outcome::Delivered
                }
            })
            .collect()
    }
}

pub(crate) struct Running {
    shutdown: CancellationToken,
    task: JoinHandle<Result<(), SessionError>>,
    events: mpsc::UnboundedReceiver<Bytes>,
    mode: watch::Sender<Mode>,
    failures: watch::Receiver<u64>,
    calls: watch::Receiver<u64>,
    health: Health,
}

pub(crate) fn spawn_session(
    params: ConnectParams,
    status_interval: Duration,
    buffer: usize,
) -> Running {
    spawn_with(params, config(status_interval, buffer))
}

pub(crate) fn spawn_with(params: ConnectParams, config: SessionConfig) -> Running {
    spawn_full(params, config, memory_stores(), LIMITS)
}

pub(crate) fn spawn_with_stores(
    params: ConnectParams,
    config: SessionConfig,
    stores: Stores,
) -> Running {
    spawn_full(params, config, stores, LIMITS)
}

pub(crate) fn spawn_full(
    params: ConnectParams,
    config: SessionConfig,
    stores: Stores,
    limits: SinkCapabilities,
) -> Running {
    spawn_in_context(params, config, stores, limits, context())
}

pub(crate) fn spawn_in_context(
    params: ConnectParams,
    config: SessionConfig,
    stores: Stores,
    limits: SinkCapabilities,
    context: SessionContext,
) -> Running {
    let (mode, mode_receiver) = watch::channel(Mode::Deliver);
    let (failures_sender, failures) = watch::channel(0);
    let (delivered, events) = mpsc::unbounded_channel();
    let (calls_sender, calls) = watch::channel(0);
    let sink: Arc<dyn Sink> = Arc::new(TestSink {
        limits,
        mode: mode_receiver,
        failures: failures_sender,
        delivered,
        calls: calls_sender,
    });
    let shutdown = CancellationToken::new();
    let health = context.health.clone();
    let task = spawn_run(params, config, sink, stores, context, shutdown.clone());
    Running {
        shutdown,
        task,
        events,
        mode,
        failures,
        calls,
        health,
    }
}

fn spawn_run(
    params: ConnectParams,
    config: SessionConfig,
    sink: Arc<dyn Sink>,
    stores: Stores,
    context: SessionContext,
    shutdown: CancellationToken,
) -> JoinHandle<Result<(), SessionError>> {
    tokio::spawn(async move {
        let provider = PasswordProvider::new(params);
        run_session(&config, &provider, &sink, &stores, &context, &shutdown).await
    })
}

impl Running {
    pub(crate) async fn next_event(&mut self) -> TestResult<Value> {
        let body = tokio::time::timeout(WAIT, self.events.recv())
            .await?
            .ok_or("session output closed")?;
        Ok(serde_json::from_slice(&body)?)
    }

    pub(crate) async fn next_events(&mut self, count: usize) -> TestResult<Vec<Value>> {
        let mut events = Vec::with_capacity(count);
        for _ in 0..count {
            events.push(self.next_event().await?);
        }
        Ok(events)
    }

    pub(crate) async fn skip_events(&mut self, count: usize) -> TestResult {
        for _ in 0..count {
            self.next_event().await?;
        }
        Ok(())
    }

    pub(crate) async fn no_event_within(&mut self, window: Duration) -> TestResult<bool> {
        Ok(tokio::time::timeout(window, self.events.recv())
            .await
            .is_err())
    }

    pub(crate) fn health(&self) -> Health {
        self.health.clone()
    }

    pub(crate) async fn finished(self, within: Duration) -> TestResult<Result<(), SessionError>> {
        Ok(tokio::time::timeout(within, self.task).await??)
    }

    pub(crate) async fn finished_with_output(
        mut self,
        within: Duration,
    ) -> TestResult<(Result<(), SessionError>, usize)> {
        let result = tokio::time::timeout(within, &mut self.task).await??;
        let mut delivered = 0;
        while self.events.try_recv().is_ok() {
            delivered += 1;
        }
        Ok((result, delivered))
    }

    pub(crate) fn set_mode(&self, mode: Mode) {
        self.mode.send_replace(mode);
    }

    pub(crate) async fn wait_for_failures(&mut self, count: u64) -> TestResult {
        tokio::time::timeout(WAIT, self.failures.wait_for(|failed| *failed >= count)).await??;
        Ok(())
    }

    pub(crate) async fn wait_for_sink_call(&mut self) -> TestResult {
        tokio::time::timeout(WAIT, self.calls.wait_for(|calls| *calls >= 1)).await??;
        Ok(())
    }

    pub(crate) fn request_stop(&self) {
        self.shutdown.cancel();
    }

    pub(crate) async fn stop(self) -> TestResult {
        self.shutdown.cancel();
        self.task.await??;
        Ok(())
    }

    pub(crate) async fn abort(self) -> TestResult {
        self.task.abort();
        match self.task.await {
            Err(error) if error.is_cancelled() => Ok(()),
            Err(error) => Err(error.into()),
            Ok(result) => Ok(result?),
        }
    }
}

pub(crate) struct StdoutRunning {
    shutdown: CancellationToken,
    task: JoinHandle<Result<(), SessionError>>,
    reader: DuplexStream,
}

pub(crate) fn spawn_stdout_session(
    params: ConnectParams,
    status_interval: Duration,
    pipe: usize,
) -> StdoutRunning {
    let (writer, reader) = tokio::io::duplex(pipe);
    let shutdown = CancellationToken::new();
    let sink: Arc<dyn Sink> = Arc::new(StdoutSink::new(writer));
    let task = spawn_run(
        params,
        config(status_interval, 64),
        sink,
        memory_stores(),
        context(),
        shutdown.clone(),
    );
    StdoutRunning {
        shutdown,
        task,
        reader,
    }
}

impl StdoutRunning {
    pub(crate) async fn stop_once_output_starts(mut self) -> TestResult<Vec<u8>> {
        let mut output = vec![0u8];
        tokio::time::timeout(WAIT, self.reader.read_exact(&mut output)).await??;
        self.shutdown.cancel();
        tokio::time::timeout(WAIT, self.reader.read_to_end(&mut output)).await??;
        self.task.await??;
        Ok(output)
    }
}

pub(crate) fn commit_lsn(event: &Value) -> TestResult<u64> {
    let text = event
        .pointer("/source/commit_lsn")
        .and_then(Value::as_str)
        .ok_or("missing commit_lsn")?;
    Ok(pg_cdc_testkit::parse_lsn(text)?)
}

type Registry = Arc<Mutex<Vec<Link>>>;

#[derive(Debug, Clone)]
struct Link {
    switch: Arc<watch::Sender<bool>>,
    severed: CancellationToken,
}

#[derive(Debug)]
pub(crate) struct ControlProxy {
    port: u16,
    connections: Registry,
    armed: Arc<AtomicBool>,
    _accepting: AbortOnDropHandle<()>,
}

impl ControlProxy {
    pub(crate) async fn start(pg: &Postgres) -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let upstream = (pg.host().to_owned(), pg.port());
        let connections = Registry::default();
        let registry = Arc::clone(&connections);
        let armed = Arc::new(AtomicBool::new(false));
        let arming = Arc::clone(&armed);
        let accepting = tokio::spawn(async move {
            let mut forwarders = JoinSet::new();
            while let Ok((client, _)) = listener.accept().await {
                let link = Link {
                    switch: Arc::new(watch::Sender::new(false)),
                    severed: CancellationToken::new(),
                };
                registry
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(link.clone());
                let at_ready = arming.swap(false, Ordering::SeqCst);
                forwarders.spawn(forward(client, upstream.clone(), link, at_ready));
            }
        });
        Ok(Self {
            port,
            connections,
            armed,
            _accepting: AbortOnDropHandle::new(accepting),
        })
    }

    pub(crate) fn port(&self) -> u16 {
        self.port
    }

    pub(crate) fn freeze(&self, connection: usize) -> TestResult {
        self.link(connection)?.switch.send_replace(true);
        Ok(())
    }

    pub(crate) fn sever(&self, connection: usize) -> TestResult {
        self.link(connection)?.severed.cancel();
        Ok(())
    }

    pub(crate) fn freeze_next_once_ready(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    pub(crate) async fn wait_until_frozen(&self, connection: usize) -> TestResult {
        let deadline = tokio::time::Instant::now() + WAIT;
        let link = loop {
            if let Ok(link) = self.link(connection) {
                break link;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("the proxy never accepted that connection".into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let mut frozen = link.switch.subscribe();
        tokio::time::timeout_at(deadline, frozen.wait_for(|frozen| *frozen)).await??;
        Ok(())
    }

    fn link(&self, connection: usize) -> TestResult<Link> {
        Ok(self
            .connections
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(connection)
            .ok_or("the proxy has not accepted that connection")?
            .clone())
    }
}

async fn forward(client: TcpStream, upstream: (String, u16), link: Link, at_ready: bool) {
    let Ok(server) = TcpStream::connect(upstream).await else {
        return;
    };
    let (client_read, client_write) = client.into_split();
    let (server_read, server_write) = server.into_split();
    let Link { switch, severed } = link;
    let freeze_at_ready = at_ready.then(|| Arc::clone(&switch));
    tokio::select! {
        () = severed.cancelled() => {}
        () = pump(client_read, server_write, switch.subscribe(), None) => {}
        () = pump(server_read, client_write, switch.subscribe(), freeze_at_ready) => {}
    }
}

async fn pump(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    mut frozen: watch::Receiver<bool>,
    mut freeze_at_ready: Option<Arc<watch::Sender<bool>>>,
) {
    let mut frames = Frames::default();
    let mut buffer = vec![0u8; 16 * 1024];
    loop {
        let Ok(read) = from.read(&mut buffer).await else {
            return;
        };
        if read == 0 {
            return;
        }
        if frozen.wait_for(|frozen| !*frozen).await.is_err() {
            return;
        }
        let Some(chunk) = buffer.get(..read) else {
            return;
        };
        if to.write_all(chunk).await.is_err() {
            return;
        }
        if freeze_at_ready.is_some()
            && frames.ends_ready_for_query(chunk)
            && let Some(switch) = freeze_at_ready.take()
        {
            switch.send_replace(true);
        }
    }
}

#[derive(Default)]
struct Frames {
    header: Vec<u8>,
    tag: u8,
    remaining: usize,
}

impl Frames {
    fn ends_ready_for_query(&mut self, mut chunk: &[u8]) -> bool {
        let mut ready = false;
        while !chunk.is_empty() {
            if self.remaining == 0 {
                let wanted = 5usize.saturating_sub(self.header.len()).min(chunk.len());
                let (head, rest) = chunk.split_at(wanted);
                self.header.extend_from_slice(head);
                chunk = rest;
                if let [tag, a, b, c, d] = *self.header.as_slice() {
                    self.tag = tag;
                    let length = u32::from_be_bytes([a, b, c, d]);
                    self.remaining = usize::try_from(length)
                        .unwrap_or(usize::MAX)
                        .saturating_sub(4);
                    self.header.clear();
                    ready |= self.remaining == 0 && self.tag == b'Z';
                }
            } else {
                let taken = self.remaining.min(chunk.len());
                chunk = chunk.get(taken..).unwrap_or_default();
                self.remaining -= taken;
                ready |= self.remaining == 0 && self.tag == b'Z';
            }
        }
        ready
    }
}
