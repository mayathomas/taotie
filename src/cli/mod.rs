mod connect;
mod describe;
mod head;
mod list;
mod schema;
mod sql;
pub use self::{
    connect::{ConnectOpts, DataSetConn},
    describe::DescribeOpts,
    head::HeadOpts,
    list::ListOpts,
    schema::SchemaOpts,
    sql::SqlOpts,
};
use anyhow::Result;
use clap::Parser;
use enum_dispatch::enum_dispatch;

pub use self::{
    connect::connect, describe::describe, head::head, list::list, schema::schema, sql::sql,
};

type ReplResult = Result<Option<String>, reedline_repl_rs::Error>;

#[derive(Debug, Parser)]
#[enum_dispatch(CmdExecutor)]
pub enum ReplCommand {
    //子命令，-- csv，中间有个空格
    #[command(
        name = "connect",
        about = "Connect to a dataset and register it to taotie"
    )]
    Connect(ConnectOpts),
    #[command(name = "list", about = "List registered datasets")]
    List(ListOpts),
    #[command(name = "schema", about = "Describe the schema of the dataset")]
    Schema(SchemaOpts),
    #[command(name = "describe", about = "Describe a dataset")]
    Describe(DescribeOpts),
    #[command(about = "Show first few rows of a dataset")]
    Head(HeadOpts),
    #[command(about = "Query a dataset using given SQL")]
    Sql(SqlOpts),
}
