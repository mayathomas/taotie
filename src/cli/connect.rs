use clap::{ArgMatches, Parser};

use crate::{Backend, CmdExecutor, ReplContext, ReplMsg};

use super::ReplResult;

#[derive(Debug, Parser)]
pub struct ConnectOpts {
    #[arg(value_parser = verify_conn_str, help = "Connection String to the dataset, could be postgres or local file (support: csv, parquet, json)")]
    pub conn: DataSetConn,

    #[arg(short, long, help = "If database, the name of the table")]
    pub table: Option<String>,

    #[arg(short, long, help = "The name of the dataset")]
    pub name: String,
}

#[derive(Debug, Clone)]
pub enum DataSetConn {
    Postgres(String),
    Csv(String),
    Parquet(String),
    NdJson(String),
}

fn verify_conn_str(s: &str) -> Result<DataSetConn, String> {
    let conn_str = s.to_string();
    if conn_str.starts_with("postgres://") {
        Ok(DataSetConn::Postgres(conn_str))
    } else if conn_str.ends_with(".csv") {
        Ok(DataSetConn::Csv(conn_str))
    } else if conn_str.ends_with(".parquet") {
        Ok(DataSetConn::Parquet(conn_str))
    } else if conn_str.ends_with(".ndjson") {
        Ok(DataSetConn::NdJson(conn_str))
    } else {
        Err(format!("Invalid connection string: {}", s))
    }
}

pub fn connect(args: ArgMatches, ctx: &mut ReplContext) -> ReplResult {
    let conn = args
        .get_one::<DataSetConn>("conn")
        .expect("expect conn_str")
        .to_owned();
    let table = args.get_one::<String>("table").map(|s| s.to_string());
    let name = args
        .get_one::<String>("name")
        .expect("expect name")
        .to_string();

    let (msg, rx) = ReplMsg::new(ConnectOpts::new(conn, table, name));
    Ok(ctx.send(msg, rx))
}

impl ConnectOpts {
    pub fn new(conn: DataSetConn, table: Option<String>, name: String) -> Self {
        Self { conn, table, name }
    }
}

impl CmdExecutor for ConnectOpts {
    async fn execute<T: Backend>(self, backend: &mut T) -> anyhow::Result<String> {
        backend.connect(&self).await?;
        Ok(format!("Connected to dataset: {}", self.name))
    }
}
