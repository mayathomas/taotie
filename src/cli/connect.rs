use clap::{ArgMatches, Parser};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;

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
    Csv(FileOpts),
    Parquet(String),
    NdJson(FileOpts),
}

#[derive(Debug, Clone)]
pub struct FileOpts {
    pub filename: String,
    pub ext: String,
    pub compression: FileCompressionType,
}

fn verify_conn_str(s: &str) -> Result<DataSetConn, String> {
    let conn_str = s.to_string();
    if conn_str.starts_with("postgres://") {
        return Ok(DataSetConn::Postgres(conn_str));
    }

    let exts = conn_str.split('.').rev().collect::<Vec<_>>();
    let len = exts.len();
    let mut exts = exts.into_iter().take(len - 1);
    let ext1 = exts.next();
    let ext2 = exts.next();
    match (ext1, ext2) {
        (Some(ext1), Some(ext2)) => {
            let compression = match ext1 {
                "gz" => FileCompressionType::GZIP,
                "bz2" => FileCompressionType::BZIP2,
                "xz" => FileCompressionType::XZ,
                "zxtd" => FileCompressionType::ZSTD,
                v => return Err(format!("Invalid compression type: {}", v)),
            };
            let opts = FileOpts {
                filename: s.to_string(),
                ext: ext2.to_string(),
                compression,
            };
            match ext1 {
                "csv" => Ok(DataSetConn::Csv(opts)),
                "json" | "jsonl" | "ndjson" => Ok(DataSetConn::NdJson(opts)),
                v => Err(format!("Invalid file extension: {}", v)),
            }
        }
        (Some(ext1), None) => {
            let opts = FileOpts {
                filename: s.to_string(),
                ext: ext1.to_string(),
                compression: FileCompressionType::UNCOMPRESSED,
            };
            match ext1 {
                "csv" => Ok(DataSetConn::Csv(opts)),
                "json" | "jsonl" | "ndjson" => Ok(DataSetConn::NdJson(opts)),
                v => Err(format!("Invalid file extension: {}", v)),
            }
        }
        _ => Err(format!("Invalid connection string: {}", s)),
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
