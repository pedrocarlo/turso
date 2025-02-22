mod app;
mod handlers;

use app::Commands;
use clap::Parser;
use handlers::extension::handle_extension;

fn main() -> anyhow::Result<()> {
    let cli = app::Cli::parse();
    match cli.command {
        Commands::Extension(args) => handle_extension(args),
    }
}
