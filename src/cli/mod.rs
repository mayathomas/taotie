mod connect;
mod describe;
mod head;
mod list;
mod sql;
use anyhow::Result;
use clap::Parser;
use connect::ConnectOpts;
use describe::DescribeOpts;
use head::HeadOpts;
use sql::SqlOpts;

pub use self::{connect::connect, describe::describe, head::head, list::list, sql::sql};

type ReplResult = Result<Option<String>, reedline_repl_rs::Error>;

#[derive(Debug, Parser)]
pub enum ReplCommand {
    //子命令，-- csv，中间有个空格
    #[command(
        name = "connect",
        about = "Connect to a dataset and register it to taotie"
    )]
    Connect(ConnectOpts),
    #[command(name = "list", about = "list registered datasets")]
    List,
    #[command(name = "describe", about = "describe a dataset")]
    Describe(DescribeOpts),
    #[command(about = "show first few rows of a dataset")]
    Head(HeadOpts),
    #[command(about = "Query a dataset using given SQL")]
    Sql(SqlOpts),
}
