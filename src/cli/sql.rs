use clap::{ArgMatches, Parser};

use crate::ReplContext;

use super::{ReplCommand, ReplResult};

#[derive(Debug, Parser)]
pub struct SqlOpts {
    #[arg(short, long, help = "The SQL query")]
    pub query: String,
}

pub fn sql(args: ArgMatches, _ctx: &mut ReplContext) -> ReplResult {
    let _query: String = args
        .get_one::<String>("query")
        .expect("expect query")
        .to_string();
    Ok(None)
}

impl From<SqlOpts> for ReplCommand {
    fn from(value: SqlOpts) -> Self {
        ReplCommand::Sql(value)
    }
}

impl SqlOpts {
    pub fn new(query: String) -> Self {
        Self { query }
    }
}
