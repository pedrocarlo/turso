mod operator;

use limbo_ext::{register_extension, ExtResult, ResultCode};
#[cfg(not(target_family = "wasm"))]
use limbo_ext::{VfsDerive, VfsExtension, VfsFile};
use opendal::{
    layers::{BlockingLayer, TracingLayer},
    services, BlockingOperator, Operator, Result,
};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use tracing::debug;

macro_rules! try_result {
    ($expr:expr, $err:expr) => {
        match $expr {
            Ok(val) => val,
            Err(_) => return $err,
        }
    };
}

register_extension! {
    vfs: { OpendalFS },
}

pub struct OpendalFile {
    op: BlockingOperator,
    path: String,
}

#[cfg(target_family = "wasm")]
pub struct OpendalFS;

#[cfg(not(target_family = "wasm"))]
#[derive(VfsDerive, Default)]
pub struct OpendalFS;

#[cfg(not(target_family = "wasm"))]
impl VfsExtension for OpendalFS {
    const NAME: &'static str = "opendal-vfs";
    type File = OpendalFile;
    fn open_file(&self, path: &str, flags: i32, _direct: bool) -> ExtResult<Self::File> {
        debug!("Opening file with Opendal VFS: {} flags: {}", path, flags);

        let builder = services::S3::default().bucket("test");

        // Init an operator
        let op = Operator::new(builder)
            .map_err(|_| ResultCode::Error)?
            .layer(TracingLayer)
            .layer(BlockingLayer::create().map_err(|_| ResultCode::Error)?)
            .finish()
            .blocking();

        Ok(OpendalFile {
            op,
            path: path.to_string(),
        })
    }

    fn run_once(&self) -> ExtResult<()> {
        Ok(())
    }
}

#[cfg(not(target_family = "wasm"))]
impl VfsFile for OpendalFile {
    fn read(&mut self, buf: &mut [u8], count: usize, offset: i64) -> ExtResult<i32> {
        let reader = self.op.read_with(&self.path);
        let reader = reader.range(offset as u64..count as u64);
        let ret_buf = try_result!(reader.call(), Err(ResultCode::Error));
        buf[..count].clone_from_slice(&ret_buf.to_bytes());

        Ok(ret_buf.len() as i32)
    }

    fn write(&mut self, buf: &[u8], count: usize, offset: i64) -> ExtResult<i32> {
        let writer = self.op.write_with(&self.path, buf[..count].into());
        writer.
        // buf[..count].clone_from_slice(&ret_buf.to_bytes());

        Ok(1)
    }

    fn sync(&self) -> ExtResult<()> {
        self.file.sync_all().map_err(|_| ResultCode::Error)
    }

    fn size(&self) -> i64 {
        self.file.metadata().map(|m| m.len() as i64).unwrap_or(-1)
    }
}
