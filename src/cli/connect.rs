use clap::{ArgMatches, Parser};

use crate::ReplContext;

use super::{ReplCommand, ReplResult};

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
    Json(String),
}

fn verify_conn_str(s: &str) -> Result<DataSetConn, String> {
    let conn_str = s.to_string();
    if conn_str.starts_with("postgres://") {
        Ok(DataSetConn::Postgres(conn_str))
    } else if conn_str.ends_with(".csv") {
        Ok(DataSetConn::Csv(conn_str))
    } else if conn_str.ends_with(".parquet") {
        Ok(DataSetConn::Parquet(conn_str))
    } else if conn_str.ends_with(".json") {
        Ok(DataSetConn::Json(conn_str))
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

    let cmd = ConnectOpts::new(conn, table, name).into();
    ctx.send(cmd);

    Ok(None)
}

impl From<ConnectOpts> for ReplCommand {
    fn from(value: ConnectOpts) -> Self {
        ReplCommand::Connect(value)
    }
}

impl ConnectOpts {
    pub fn new(conn: DataSetConn, table: Option<String>, name: String) -> Self {
        Self { conn, table, name }
    }
}
