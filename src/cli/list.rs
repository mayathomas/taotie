use clap::{ArgMatches, Parser};

use crate::{Backend, CmdExecutor, ReplContext, ReplDisplay, ReplMsg};

use super::{ReplCommand, ReplResult};

#[derive(Debug, Parser)]
pub struct ListOpts;

pub fn list(_args: ArgMatches, ctx: &mut ReplContext) -> ReplResult {
    let (msg, rx) = ReplMsg::new(ReplCommand::List(ListOpts));
    Ok(ctx.send(msg, rx))
}

impl CmdExecutor for ListOpts {
    async fn execute<T: Backend>(self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.list().await?;
        df.display().await
    }
}
