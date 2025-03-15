use std::{future::IntoFuture, sync::Arc};

use bytes::{Bytes, BytesMut};
use limbo_ext::{ExtResult as Result, ResultCode, VfsDerive, VfsExtension, VfsFile};
use tokio::{
    fs::OpenOptions,
    io::{AsyncReadExt, AsyncSeekExt, SeekFrom},
    runtime::Runtime,
    sync::RwLock,
};
use std::future;

macro_rules! try_result {
    ($expr:expr, $err:expr) => {
        match $expr {
            Ok(val) => val,
            Err(_) => return $err,
        }
    };
}

/// Your struct must also impl Default
#[derive(VfsDerive, Default)]
struct AsyncFS;

    struct AsyncFile {
        rt: Runtime,
        file: Arc<RwLock<tokio::fs::File>>,
    }

impl VfsExtension for AsyncFS {
    /// The name of your vfs module
    const NAME: &'static str = "async_file";

    type File = AsyncFile;

    fn open_file(&self, path: &str, flags: i32, _direct: bool) -> Result<Self::File> {
        let rt = try_result!(Runtime::new(), Err(ResultCode::Error));

        let file = try_result!(
            rt.block_on(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(flags & 1 != 0)
                    .open(path)
            ),
            Err(ResultCode::Error)
        );
        Ok(AsyncFile {
            rt,
            file: Arc::new(RwLock::new(file)),
        })
    }

    fn run_once(&self) -> Result<()> {
        // (optional) method to cycle/advance IO, if your extension is asynchronous
        Ok(())
    }

    fn close(&self, file: Self::File) -> Result<()> {
        // (optional) method to close or drop the file
        Ok(())
    }
}

impl VfsFile for AsyncFile {
    fn read(&mut self, buf: &mut [u8], count: usize, offset: i64) -> Result<i32> {
        let file = self.file.clone();
        let handle = self.rt.spawn(async move {
            let mut file_lock = file.write().await;

            if file_lock
                .seek(SeekFrom::Start(offset as u64))
                .await
                .is_err()
            {
                return Err(ResultCode::Error);
            }
            let mut temp_buf = BytesMut::with_capacity(count);
            file_lock.read_buf(&mut temp_buf);


            Ok(())
        });
        Ok(0)
    }

    fn write(&mut self, buf: &[u8], count: usize, offset: i64) -> Result<i32> {
        if self.file.seek(SeekFrom::Start(offset as u64)).is_err() {
            return Err(ResultCode::Error);
        }
        self.file
            .write(&buf[..count])
            .map_err(|_| ResultCode::Error)
            .map(|n| n as i32)
    }

    fn sync(&self) -> Result<()> {
        self.file.sync_all().map_err(|_| ResultCode::Error)
    }

    fn size(&self) -> i64 {
        self.file.metadata().map(|m| m.len() as i64).unwrap_or(-1)
    }
}
