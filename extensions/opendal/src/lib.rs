use limbo_ext::{register_extension, ExtResult, ResultCode};
#[cfg(not(target_family = "wasm"))]
use limbo_ext::{VfsDerive, VfsExtension, VfsFile};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use tracing::debug;

register_extension! {
    vfs: { OpendalFS },
}

pub struct OpendalFile {
    file: File,
}

#[cfg(target_family = "wasm")]
pub struct TestFS;

#[cfg(not(target_family = "wasm"))]
#[derive(VfsDerive, Default)]
pub struct OpendalFS;

#[cfg(not(target_family = "wasm"))]
impl VfsExtension for OpendalFS {
    const NAME: &'static str = "opendal-vfs";
    type File = OpendalFile;
    fn open_file(&self, path: &str, flags: i32, _direct: bool) -> ExtResult<Self::File> {
        debug!("Opening file with Opendal VFS: {} flags: {}", path, flags);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(flags & 1 != 0)
            .open(path)
            .map_err(|_| ResultCode::Error)?;
        Ok(OpendalFile { file })
    }
}

#[cfg(not(target_family = "wasm"))]
impl VfsFile for OpendalFile {
    fn read(&mut self, buf: &mut [u8], count: usize, offset: i64) -> ExtResult<i32> {
        if self.file.seek(SeekFrom::Start(offset as u64)).is_err() {
            return Err(ResultCode::Error);
        }
        self.file
            .read(&mut buf[..count])
            .map_err(|_| ResultCode::Error)
            .map(|n| n as i32)
    }

    fn write(&mut self, buf: &[u8], count: usize, offset: i64) -> ExtResult<i32> {
        if self.file.seek(SeekFrom::Start(offset as u64)).is_err() {
            return Err(ResultCode::Error);
        }
        self.file
            .write(&buf[..count])
            .map_err(|_| ResultCode::Error)
            .map(|n| n as i32)
    }

    fn sync(&self) -> ExtResult<()> {
        self.file.sync_all().map_err(|_| ResultCode::Error)
    }

    fn size(&self) -> i64 {
        self.file.metadata().map(|m| m.len() as i64).unwrap_or(-1)
    }
}
