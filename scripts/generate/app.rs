use std::{
    env,
    path::{Path, PathBuf},
    process::Command,
};

use clap::{Args, Parser, Subcommand};
use handlebars::Handlebars;

macro_rules! try_out {
    ($out:expr) => {
        if !$out.status.success() {
            let err = String::from_utf8($out.stderr)?;
            eprintln!("{}", err);
            return Ok(());
        }
    };
}

pub(crate) use try_out;

#[derive(Parser)]
#[command(name = "generate")]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    Extension(ExtArgs),
}

#[derive(Args)]
pub struct ExtArgs {
    #[clap(short = 'N', long, help = "Name of extension to generate")]
    pub ext_name: String,
}

