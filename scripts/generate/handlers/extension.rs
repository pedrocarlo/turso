use std::{
    env,
    path::{Path, PathBuf},
    process::Command,
};

use handlebars::Handlebars;

use crate::app::{try_out, ExtArgs};

pub fn handle_extension(args: ExtArgs) -> anyhow::Result<()> {
    let mut cargo = Command::new("cargo");
    let workspace_root = env::var("CARGO_WORKSPACE_DIR")?;
    let workspace_root = PathBuf::from(workspace_root); // This variable needs to be set in .cargo/config.toml

    // let cargo_new = cargo.args(["new", "--lib", "extensions/example"]).spawn()?;
    // let out = cargo_new.wait_with_output()?;

    // try_out!(out);

    let cargo_toml_key = "cargo_toml";
    let lib_key = "lib";
    let scalar_key = "scalar";
    let agg_key = "agg";
    let vtab_key = "vtab";

    let template_extension_dir =
        workspace_root.join(Path::new("scripts/generate/templates/extension"));

    let mut hbs = Handlebars::new();

    hbs.register_template_file(
        cargo_toml_key,
        template_extension_dir.join("Cargo.toml.hbs"),
    )?;
    hbs.register_template_file(lib_key, template_extension_dir.join("lib.rs.hbs"))?;
    hbs.register_template_file(scalar_key, template_extension_dir.join("scalar.rs.hbs"))?;
    hbs.register_template_file(agg_key, template_extension_dir.join("agg.rs.hbs"))?;
    hbs.register_template_file(vtab_key, template_extension_dir.join("vtab.rs.hbs"))?;

    let data = serde_json::json!({
        "name": "example",
        "scalar": true,
        "agg": true,
        "vtab": true,
    });
    let new_cargo_toml = hbs.render(cargo_toml_key, &data)?;

    let new_lib = hbs.render(lib_key, &data)?;
    println!("{new_lib}");

    let new_scalar = hbs.render(scalar_key, &data)?;

    let new_agg = hbs.render(agg_key, &data)?;

    let new_vtab = hbs.render(vtab_key, &data)?;
    Ok(())
}
