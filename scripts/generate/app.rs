use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use handlebars::Handlebars;
use serde::{Deserialize, Serialize};

pub struct FileGen {
    pub filename: String,
    pub src: PathBuf,
    pub dest: PathBuf,
}

impl FileGen {
    pub fn new(filename: &str, src_dir: PathBuf, dest_dir: PathBuf) -> Self {
        Self {
            src: src_dir.join(format!("{}.hbs", &filename)),
            dest: dest_dir.join(&filename),
            filename: filename.to_owned(),
        }
    }

    pub fn register_template(&self, hbs: &mut Handlebars) -> anyhow::Result<()> {
        hbs.register_template_file(&self.filename, &self.src)?;
        Ok(())
    }
}

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

    #[clap(
        long = "skip-templ",
        help = "Skip template generation",
        default_value_t = false
    )]
    pub skip_templates: bool,

    #[clap(short, long, help = "Generate Scalar", default_value_t = false)]
    pub scalar: bool,

    #[serde(rename = "agg")]
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
