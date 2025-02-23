use std::{
    env,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
};

use serde_json::Value;

use convert_case::{Case, Casing};
use handlebars::Handlebars;

use crate::app::{try_out, ExtArgs};

macro_rules! write_to_file {
    ($text:expr, $path:expr) => {
        let mut f = File::create($path)?;
        f.write_all(&$text.as_bytes())?;
    };
}

pub fn handle_extension(mut args: ExtArgs) -> anyhow::Result<()> {
    // Convert "example text" -> "example_text" and "limbo_example" -> "example"
    args.ext_name = args.ext_name.replace("limbo", "");
    args.ext_name = args
        .ext_name
        .trim_matches(|ch: char| ch == '_' || ch == '-' || ch.is_whitespace())
        .to_string();

    args.ext_name = args
        .ext_name
        .from_case(Case::Snake)
        .from_case(Case::Lower)
        .to_case(Case::Snake);

    let workspace_root = env::var("CARGO_WORKSPACE_DIR")?;
    let workspace_root = PathBuf::from(workspace_root); // This variable needs to be set in .cargo/config.toml

    if !args.skip_cargo {
        let mut cargo = Command::new("cargo");
        let cargo_new = cargo
            .args([
                "new",
                "--lib",
                format!("extensions/{}", args.ext_name).as_str(),
            ])
            .spawn()?;
        let out = cargo_new.wait_with_output()?;

        try_out!(out);
    }

    let cargo_toml_key = "Cargo.toml";
    let lib_key = "lib.rs";
    let scalar_key = "scalar.rs";
    let agg_key = "agg.rs";
    let vtab_key = "vtab.rs";

    let template_extension_dir =
        workspace_root.join(Path::new("scripts/generate/templates/extension"));

    let extension_dir =
        workspace_root.join(Path::new(format!("extensions/{}", args.ext_name).as_str()));
    let extension_src_dir = extension_dir.join(Path::new("src"));

    let mut hbs = Handlebars::new();

    hbs.register_template_file(
        cargo_toml_key,
        template_extension_dir.join("Cargo.toml.hbs"),
    )?;
    hbs.register_template_file(lib_key, template_extension_dir.join("lib.rs.hbs"))?;
    hbs.register_template_file(scalar_key, template_extension_dir.join("scalar.rs.hbs"))?;
    hbs.register_template_file(agg_key, template_extension_dir.join("agg.rs.hbs"))?;
    hbs.register_template_file(vtab_key, template_extension_dir.join("vtab.rs.hbs"))?;

    let mut data = serde_json::json!({
        "struct_prefix":args.ext_name.to_case(Case::Pascal),
    });

    let args_data = serde_json::to_value(&args)?;

    merge(&mut data, &args_data);

    let new_cargo_toml = hbs.render(cargo_toml_key, &data)?;
    write_to_file!(new_cargo_toml, extension_dir.join(cargo_toml_key));

    let new_lib = hbs.render(lib_key, &data)?;
    write_to_file!(new_lib, extension_src_dir.join(lib_key));

    if args.scalar {
        let new_scalar = hbs.render(scalar_key, &data)?;
        write_to_file!(new_scalar, extension_src_dir.join(scalar_key));
    }

    if args.aggregate {
        let new_agg = hbs.render(agg_key, &data)?;
        write_to_file!(new_agg, extension_src_dir.join(agg_key));
    }

    if args.vtab {
        let new_vtab = hbs.render(vtab_key, &data)?;
        write_to_file!(new_vtab, extension_src_dir.join(vtab_key));
    }

    Ok(())
}

/// Merges b Value in a
fn merge(a: &mut Value, b: &Value) {
    match (a, b) {
        (Value::Object(a), Value::Object(b)) => {
            for (k, v) in b {
                merge(a.entry(k.clone()).or_insert(Value::Null), v);
            }
        }
        (a, b) => *a = b.clone(),
    }
}
