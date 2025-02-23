use clap::{Args, Parser, Subcommand};
use serde::{Deserialize, Serialize};

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

#[derive(Args, Serialize, Deserialize)]
pub struct ExtArgs {
    #[serde(rename = "name")]
    #[clap(short = 'N', long = "name", help = "Name of extension to generate")]
    pub ext_name: String,

    #[clap(
        short = 'S',
        long = "skip",
        help = "Skip cargo new command",
        default_value_t = false
    )]
    pub skip_cargo: bool,

    #[clap(short, long, help = "Generate Scalar", default_value_t = false)]
    pub scalar: bool,
    #[clap(
        short,
        long = "agg",
        help = "Generate Aggregate",
        default_value_t = false
    )]
    pub aggregate: bool,
    #[clap(short, long, help = "Generate Vtable", default_value_t = false)]
    pub vtab: bool,
}
