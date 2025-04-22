use std::path::{Path, PathBuf};

#[allow(dead_code)]
pub mod generation;
#[allow(dead_code)]
pub mod model;
#[allow(dead_code)]
pub mod runner;
#[allow(dead_code)]
pub mod shrink;

pub struct Paths {
    pub base: PathBuf,
    pub db: PathBuf,
    pub plan: PathBuf,
    pub shrunk_plan: PathBuf,
    pub history: PathBuf,
    pub doublecheck_db: PathBuf,
    pub shrunk_db: PathBuf,
    pub diff_db: PathBuf,
}

impl Paths {
    pub fn new(output_dir: &Path) -> Self {
        Paths {
            base: output_dir.to_path_buf(),
            db: PathBuf::from(output_dir).join("test.db"),
            plan: PathBuf::from(output_dir).join("plan.sql"),
            shrunk_plan: PathBuf::from(output_dir).join("shrunk.sql"),
            history: PathBuf::from(output_dir).join("history.txt"),
            doublecheck_db: PathBuf::from(output_dir).join("double.db"),
            shrunk_db: PathBuf::from(output_dir).join("shrunk.db"),
            diff_db: PathBuf::from(output_dir).join("diff.db"),
        }
    }
}
