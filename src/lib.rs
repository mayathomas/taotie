use std::{ops::Deref, thread};

use backend::DataFusionBackend;
pub use cli::ReplCommand;
use cli::{ConnectOpts, DescribeOpts, HeadOpts, ListOpts, SchemaOpts, SqlOpts};
use crossbeam_channel as mpsc;
use enum_dispatch::enum_dispatch;
use reedline_repl_rs::CallBackMap;
use tokio::runtime::Runtime;

mod backend;
mod cli;

#[enum_dispatch]
trait CmdExecutor {
    async fn execute<T: Backend>(self, backend: &mut T) -> anyhow::Result<String>;
}

trait Backend {
    type DataFrame: ReplDisplay;
    async fn connect(&mut self, opts: &ConnectOpts) -> anyhow::Result<()>;
    async fn list(&self) -> anyhow::Result<Self::DataFrame>;
    async fn schema(&self, name: &str) -> anyhow::Result<Self::DataFrame>;
    async fn describe(&self, name: &str) -> anyhow::Result<Self::DataFrame>;
    async fn head(&self, name: &str, n: usize) -> anyhow::Result<Self::DataFrame>;
    async fn sql(&self, sql: &str) -> anyhow::Result<Self::DataFrame>;
}

trait ReplDisplay {
    async fn display(self) -> anyhow::Result<String>;
}

pub struct ReplContext {
    pub tx: mpsc::Sender<ReplMsg>,
}

pub struct ReplMsg {
    cmd: ReplCommand,
    tx: oneshot::Sender<String>,
}

pub type ReplCallBacks = CallBackMap<ReplContext, reedline_repl_rs::Error>;

pub fn get_callbacks() -> ReplCallBacks {
    let mut callbacks = ReplCallBacks::new();
    callbacks.insert("connect".to_string(), cli::connect);
    callbacks.insert("list".to_string(), cli::list);
    callbacks.insert("schema".to_string(), cli::schema);
    callbacks.insert("describe".to_string(), cli::describe);
    callbacks.insert("head".to_string(), cli::head);
    callbacks.insert("sql".to_string(), cli::sql);
    callbacks
}

impl Default for ReplContext {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplContext {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded::<ReplMsg>();
        let rt = Runtime::new().expect("Failed to create runtime");

        let mut backend = DataFusionBackend::new();
        thread::Builder::new()
            .name("ReplBackend".to_string())
            .spawn(move || {
                while let Ok(msg) = rx.recv() {
                    if let Err(e) = rt.block_on(async {
                        let ret = msg.cmd.execute(&mut backend).await?;
                        msg.tx.send(ret)?;
                        Ok::<_, anyhow::Error>(())
                    }) {
                        eprintln!("Failed to process command: {}", e);
                    }
                }
            })
            .unwrap();
        Self { tx }
    }

    pub fn send(&self, cmd: ReplMsg, rx: oneshot::Receiver<String>) -> Option<String> {
        if let Err(e) = self.tx.send(cmd) {
            eprint!("Repl Send Error: {}", e);
            std::process::exit(1);
        }
        match rx.recv() {
            Ok(data) => Some(data),
            Err(e) => {
                eprint!("Repl Recv Error: {}", e);
                std::process::exit(1);
            }
        }
    }
}

impl Deref for ReplContext {
    type Target = mpsc::Sender<ReplMsg>;
    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl ReplMsg {
    pub fn new(cmd: impl Into<ReplCommand>) -> (Self, oneshot::Receiver<String>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                cmd: cmd.into(),
                tx,
            },
            rx,
        )
    }
}
