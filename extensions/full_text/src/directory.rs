use std::path::Path;
use std::sync::Arc;
use tantivy::directory::{
    error::{DeleteError, OpenReadError, OpenWriteError},
    Directory, FileHandle, WatchCallback, WatchHandle, WritePtr,
};

#[derive(Debug, Clone)]
struct LimboDirectory;

impl Directory for LimboDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        todo!()
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        todo!()
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        todo!()
    }

    fn open_write(&self, path: &std::path::Path) -> Result<WritePtr, OpenWriteError> {
        todo!()
    }

    fn atomic_read(&self, path: &std::path::Path) -> Result<Vec<u8>, OpenReadError> {
        todo!()
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> std::io::Result<()> {
        todo!()
    }

    fn sync_directory(&self) -> std::io::Result<()> {
        todo!()
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        todo!()
    }
}
