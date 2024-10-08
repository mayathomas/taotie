use std::{ops::Deref, thread};

pub use cli::ReplCommand;
use crossbeam_channel as mpsc;
use reedline_repl_rs::CallBackMap;

mod cli;

pub struct ReplContext {
    pub tx: mpsc::Sender<ReplCommand>,
}

pub type ReplCallBacks = CallBackMap<ReplContext, reedline_repl_rs::Error>;

pub fn get_callbacks() -> ReplCallBacks {
    let mut callbacks = ReplCallBacks::new();
    callbacks.insert("connect".to_string(), cli::connect);
    callbacks.insert("listg".to_string(), cli::list);
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
        let (tx, rx) = mpsc::unbounded();
        thread::Builder::new()
            .name("ReplBackend".to_string())
            .spawn(move || {
                while let Ok(cmd) = rx.recv() {
                    println!("{:?}", cmd);
                }
            })
            .unwrap();
        Self { tx }
    }

    pub fn send(&self, cmd: ReplCommand) {
        if let Err(e) = self.tx.send(cmd) {
            eprint!("Error: {}", e);
            std::process::exit(1);
        }
    }
}

impl Deref for ReplContext {
    type Target = mpsc::Sender<ReplCommand>;
    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}
