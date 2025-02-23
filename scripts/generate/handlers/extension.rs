use convert_case::{Case, Casing};
use handlebars::Handlebars;
use serde_json::Value;
use std::{
    env,
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
    process::Command,
};
use toml_edit::{value, Array, DocumentMut};

use crate::app::{ExtArgs, FileGen};

/// Handler for the `generate extension` command
pub fn handle_extension(mut args: ExtArgs) -> anyhow::Result<()> {
    // Convert "example text" -> "example_text" or "limbo_example" -> "example"
    // or "example_limbo" -> "example"
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

    if !args.skip_cargo {
        cargo_new(&args.ext_name)?;
    }

    // This env var needs to be set in .cargo/config.toml
    let workspace_root: PathBuf = env::var("CARGO_WORKSPACE_DIR")?.into();

    let template_extension_dir =
        workspace_root.join(Path::new("scripts/generate/templates/extension"));

    let extension_dir =
        workspace_root.join(Path::new(format!("extensions/{}", args.ext_name).as_str()));
    let extension_src_dir = extension_dir.join(Path::new("src"));

    // Super inneficient with cloning everywhere, could change to use references.
    // But as this is a script this is not a major concern
    let cargo_toml_file = FileGen::new(
        "Cargo.toml",
        template_extension_dir.to_owned(),
        extension_dir.to_owned(),
    );
    let lib_file = FileGen::new(
        "lib.rs",
        template_extension_dir.to_owned(),
        extension_src_dir.to_owned(),
    );
    let scalar_file = FileGen::new(
        "scalar.rs",
        template_extension_dir.to_owned(),
        extension_src_dir.to_owned(),
    );
    let agg_file = FileGen::new(
        "agg.rs",
        template_extension_dir.to_owned(),
        extension_src_dir.to_owned(),
    );
    let vtab_file = FileGen::new(
        "vtab.rs",
        template_extension_dir.to_owned(),
        extension_src_dir.to_owned(),
    );

    if !args.skip_templates {
        let mut hbs = Handlebars::new();

        register_templates(
            &mut hbs,
            vec![
                &cargo_toml_file,
                &lib_file,
                &scalar_file,
                &agg_file,
                &vtab_file,
            ],
        )?;

        let mut data = serde_json::json!({
            "struct_prefix":args.ext_name.to_case(Case::Pascal),
        });

        let args_data = serde_json::to_value(&args)?;

        merge(&mut data, &args_data);

        write_to_file(
            hbs.render(&cargo_toml_file.filename, &data)?,
            &cargo_toml_file.dest,
        )?;

        write_to_file(hbs.render(&lib_file.filename, &data)?, &lib_file.dest)?;

        if args.scalar {
            write_to_file(hbs.render(&scalar_file.filename, &data)?, &scalar_file.dest)?;
        }

        if args.aggregate {
            write_to_file(hbs.render(&agg_file.filename, &data)?, &agg_file.dest)?;
        }

        if args.vtab {
            write_to_file(hbs.render(&vtab_file.filename, &data)?, &vtab_file.dest)?;
        }
    }

    add_dependency(&args.ext_name, workspace_root)?;

    Ok(())
}

fn cargo_new(pkg_name: &str) -> anyhow::Result<()> {
    let mut cargo = Command::new("cargo");
    let cargo_new = cargo
        .args(["new", "--lib", format!("extensions/{}", pkg_name).as_str()])
        .spawn()?;
    let out = cargo_new.wait_with_output()?;

    if !out.status.success() {
        let err = String::from_utf8(out.stderr)?;
        return Err(anyhow::Error::msg(err));
    }

    Ok(())
}

fn register_templates(hbs: &mut Handlebars, files: Vec<&FileGen>) -> anyhow::Result<()> {
    for file in files {
        file.register_template(hbs)?;
    }
    Ok(())
}

/// Writes the `text` to the file `path`
fn write_to_file(text: String, path: &PathBuf) -> anyhow::Result<()> {
    let mut f = File::create(path)?;
    f.write_all(&text.as_bytes())?;
    Ok(())
}

fn add_dependency(ext_name: &str, root: PathBuf) -> anyhow::Result<()> {
    let workspace_dest = root.join("Cargo.toml");

    let cargo_toml_workspace = read_toml(&workspace_dest)?;

    add_dependency_workspace(ext_name, cargo_toml_workspace, &workspace_dest)?;

    let core_dest = root.join("core/Cargo.toml");
    let cargo_toml_core = read_toml(&core_dest)?;

    add_dependency_core(ext_name, cargo_toml_core, &core_dest)?;
    Ok(())
}

fn add_dependency_workspace(
    ext_name: &str,
    mut cargo_toml: DocumentMut,
    dest: &PathBuf,
) -> anyhow::Result<()> {
    let workspace_version = cargo_toml["workspace"]["package"]["version"].clone();

    cargo_toml["workspace"]["dependencies"][format!("limbo_{ext_name}")]["path"] =
        value(format!("extensions/{ext_name}"));

    cargo_toml["workspace"]["dependencies"][format!("limbo_{ext_name}")]["version"] =
        workspace_version;

    let mut f = File::options().write(true).truncate(true).open(dest)?;

    write!(f, "{}", cargo_toml.to_string())?;
    Ok(())
}

fn add_dependency_core(
    ext_name: &str,
    mut cargo_toml: DocumentMut,
    dest: &PathBuf,
) -> anyhow::Result<()> {
    let dependencies = &mut cargo_toml["dependencies"][format!("limbo_{ext_name}")];

    dependencies["workspace"] = value(true);
    dependencies["optional"] = value(true);

    let mut features = Array::new();
    features.push("static");
    dependencies["features"] = value(features);

    let mut ext_array_features = Array::new();
    ext_array_features.push(format!("limbo_{ext_name}/static"));
    cargo_toml["features"][ext_name] = value(ext_array_features);

    let mut f = File::options().write(true).truncate(true).open(dest)?;

    write!(f, "{}", cargo_toml.to_string())?;
    Ok(())
}

fn read_toml(dest: &PathBuf) -> anyhow::Result<DocumentMut> {
    let mut f = File::options().read(true).open(&dest)?;

    let mut contents = String::new();
    let _ = f.read_to_string(&mut contents)?;

    let doc = contents.parse::<DocumentMut>()?;
    Ok(doc)
}

/// Merges `b` Value in `a`
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
