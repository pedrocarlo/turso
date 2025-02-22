use std::process::Command;

fn main() -> anyhow::Result<()> {
    println!("Hello, world!");
    Command::new("cargo").args(["new", "--lib", "extensions/example"]);

    // Command
    Ok(())
}
