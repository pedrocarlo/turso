use std::fmt;

use crate::{
    io::{File, FileSyncType},
    mvcc::persistent_storage::DurableStorage,
    storage::database::{DatabaseStorage, IOContext},
    sync::Arc,
    types::{IOCompletions, IOResult},
    Buffer, Completion, Result,
};

/// A pending I/O request that has not necessarily been submitted yet.
///
/// Execution code should construct these values and return them to the step
/// boundary. The boundary calls [`IoRequest::submit`] and then waits on the
/// resulting completions.
pub enum IoRequest {
    /// A cooperative yield that has no backend I/O to submit.
    Yield,
    /// Transitional bridge for operations that still submit I/O internally.
    Submitted(IOCompletions),
    /// Completion-based operation on a [`File`].
    File(FileOp),
    /// Completion-based operation on database page storage.
    Database(DatabaseOp),
    /// Completion-based operation on MVCC durable storage.
    DurableStorage(DurableStorageOp),
}

impl IoRequest {
    pub fn yield_now() -> Self {
        Self::Yield
    }

    pub fn submitted(completions: IOCompletions) -> Self {
        Self::Submitted(completions)
    }

    pub fn read(file: Arc<dyn File>, pos: u64, completion: Completion) -> Self {
        Self::File(FileOp::Read(FileRead {
            file,
            pos,
            completion,
        }))
    }

    pub fn write(
        file: Arc<dyn File>,
        pos: u64,
        buffer: Arc<Buffer>,
        completion: Completion,
    ) -> Self {
        Self::File(FileOp::Write(FileWrite {
            file,
            pos,
            buffer,
            completion,
        }))
    }

    pub fn write_vectored(
        file: Arc<dyn File>,
        pos: u64,
        buffers: Vec<Arc<Buffer>>,
        completion: Completion,
    ) -> Self {
        Self::File(FileOp::WriteVectored(FileWriteVectored {
            file,
            pos,
            buffers,
            completion,
        }))
    }

    pub fn sync(file: Arc<dyn File>, completion: Completion, sync_type: FileSyncType) -> Self {
        Self::File(FileOp::Sync(FileSync {
            file,
            completion,
            sync_type,
        }))
    }

    pub fn truncate(file: Arc<dyn File>, len: u64, completion: Completion) -> Self {
        Self::File(FileOp::Truncate(FileTruncate {
            file,
            len,
            completion,
        }))
    }

    pub fn database_write_page(
        storage: Arc<dyn DatabaseStorage>,
        page_idx: usize,
        buffer: Arc<Buffer>,
        io_ctx: IOContext,
        completion: Completion,
    ) -> Self {
        Self::Database(DatabaseOp::WritePage(DatabaseWritePage {
            storage,
            page_idx,
            buffer,
            io_ctx,
            completion,
        }))
    }

    pub fn database_sync(
        storage: Arc<dyn DatabaseStorage>,
        completion: Completion,
        sync_type: FileSyncType,
    ) -> Self {
        Self::Database(DatabaseOp::Sync(DatabaseSync {
            storage,
            completion,
            sync_type,
        }))
    }

    pub fn durable_sync(storage: Arc<dyn DurableStorage>, sync_type: FileSyncType) -> Self {
        Self::DurableStorage(DurableStorageOp::Sync(DurableStorageSync {
            storage,
            sync_type,
        }))
    }

    pub fn durable_truncate(storage: Arc<dyn DurableStorage>) -> Self {
        Self::DurableStorage(DurableStorageOp::Truncate(DurableStorageTruncate {
            storage,
        }))
    }

    pub fn submit(self) -> Result<IOCompletions> {
        match self {
            Self::Yield => Ok(IOCompletions::Single(Completion::new_yield())),
            Self::Submitted(completions) => Ok(completions),
            Self::File(op) => op.submit(),
            Self::Database(op) => op.submit(),
            Self::DurableStorage(op) => op.submit(),
        }
    }

    pub fn is_explicit_yield(&self) -> bool {
        match self {
            Self::Yield => true,
            Self::Submitted(completions) => completions.is_explicit_yield(),
            Self::File(_) | Self::Database(_) | Self::DurableStorage(_) => false,
        }
    }
}

impl<T> From<IOResult<T>> for IoResult<T> {
    fn from(value: IOResult<T>) -> Self {
        match value {
            IOResult::Done(value) => IoResult::Done(value),
            IOResult::IO(completions) => IoResult::IO(IoRequest::submitted(completions)),
        }
    }
}

impl From<IOCompletions> for IoRequest {
    fn from(value: IOCompletions) -> Self {
        Self::submitted(value)
    }
}

impl fmt::Debug for IoRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Yield => f.debug_tuple("Yield").finish(),
            Self::Submitted(completions) => f.debug_tuple("Submitted").field(completions).finish(),
            Self::File(op) => f.debug_tuple("File").field(op).finish(),
            Self::Database(op) => f.debug_tuple("Database").field(op).finish(),
            Self::DurableStorage(op) => f.debug_tuple("DurableStorage").field(op).finish(),
        }
    }
}

#[must_use]
pub enum IoResult<T> {
    Done(T),
    IO(IoRequest),
}

impl<T> IoResult<T> {
    pub fn map<U>(self, func: impl FnOnce(T) -> U) -> IoResult<U> {
        match self {
            IoResult::Done(value) => IoResult::Done(func(value)),
            IoResult::IO(request) => IoResult::IO(request),
        }
    }
}

pub enum FileOp {
    Read(FileRead),
    Write(FileWrite),
    WriteVectored(FileWriteVectored),
    Sync(FileSync),
    Truncate(FileTruncate),
}

impl FileOp {
    pub fn submit(self) -> Result<IOCompletions> {
        let completion = match self {
            Self::Read(op) => op.file.pread(op.pos, op.completion)?,
            Self::Write(op) => op.file.pwrite(op.pos, op.buffer, op.completion)?,
            Self::WriteVectored(op) => op.file.pwritev(op.pos, op.buffers, op.completion)?,
            Self::Sync(op) => op.file.sync(op.completion, op.sync_type)?,
            Self::Truncate(op) => op.file.truncate(op.len, op.completion)?,
        };
        Ok(IOCompletions::Single(completion))
    }
}

impl fmt::Debug for FileOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(op) => op.fmt(f),
            Self::Write(op) => op.fmt(f),
            Self::WriteVectored(op) => op.fmt(f),
            Self::Sync(op) => op.fmt(f),
            Self::Truncate(op) => op.fmt(f),
        }
    }
}

pub struct FileRead {
    pub file: Arc<dyn File>,
    pub pos: u64,
    pub completion: Completion,
}

impl fmt::Debug for FileRead {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileRead")
            .field("pos", &self.pos)
            .finish_non_exhaustive()
    }
}

pub struct FileWrite {
    pub file: Arc<dyn File>,
    pub pos: u64,
    pub buffer: Arc<Buffer>,
    pub completion: Completion,
}

impl fmt::Debug for FileWrite {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileWrite")
            .field("pos", &self.pos)
            .field("buffer_len", &self.buffer.len())
            .finish_non_exhaustive()
    }
}

pub struct FileWriteVectored {
    pub file: Arc<dyn File>,
    pub pos: u64,
    pub buffers: Vec<Arc<Buffer>>,
    pub completion: Completion,
}

impl fmt::Debug for FileWriteVectored {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileWriteVectored")
            .field("pos", &self.pos)
            .field("buffer_count", &self.buffers.len())
            .finish_non_exhaustive()
    }
}

pub struct FileSync {
    pub file: Arc<dyn File>,
    pub completion: Completion,
    pub sync_type: FileSyncType,
}

impl fmt::Debug for FileSync {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileSync")
            .field("sync_type", &self.sync_type)
            .finish_non_exhaustive()
    }
}

pub struct FileTruncate {
    pub file: Arc<dyn File>,
    pub len: u64,
    pub completion: Completion,
}

impl fmt::Debug for FileTruncate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileTruncate")
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

pub enum DatabaseOp {
    WritePage(DatabaseWritePage),
    Sync(DatabaseSync),
}

impl DatabaseOp {
    pub fn submit(self) -> Result<IOCompletions> {
        let completion = match self {
            Self::WritePage(op) => {
                op.storage
                    .write_page(op.page_idx, op.buffer, &op.io_ctx, op.completion)?
            }
            Self::Sync(op) => op.storage.sync(op.completion, op.sync_type)?,
        };
        Ok(IOCompletions::Single(completion))
    }
}

impl fmt::Debug for DatabaseOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WritePage(op) => op.fmt(f),
            Self::Sync(op) => op.fmt(f),
        }
    }
}

pub struct DatabaseWritePage {
    pub storage: Arc<dyn DatabaseStorage>,
    pub page_idx: usize,
    pub buffer: Arc<Buffer>,
    pub io_ctx: IOContext,
    pub completion: Completion,
}

impl fmt::Debug for DatabaseWritePage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DatabaseWritePage")
            .field("page_idx", &self.page_idx)
            .field("buffer_len", &self.buffer.len())
            .field("io_ctx", &self.io_ctx)
            .finish_non_exhaustive()
    }
}

pub struct DatabaseSync {
    pub storage: Arc<dyn DatabaseStorage>,
    pub completion: Completion,
    pub sync_type: FileSyncType,
}

impl fmt::Debug for DatabaseSync {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DatabaseSync")
            .field("sync_type", &self.sync_type)
            .finish_non_exhaustive()
    }
}

pub enum DurableStorageOp {
    Sync(DurableStorageSync),
    Truncate(DurableStorageTruncate),
}

impl DurableStorageOp {
    pub fn submit(self) -> Result<IOCompletions> {
        let completion = match self {
            Self::Sync(op) => op.storage.sync(op.sync_type)?,
            Self::Truncate(op) => op.storage.truncate()?,
        };
        Ok(IOCompletions::Single(completion))
    }
}

impl fmt::Debug for DurableStorageOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sync(op) => op.fmt(f),
            Self::Truncate(op) => op.fmt(f),
        }
    }
}

pub struct DurableStorageSync {
    pub storage: Arc<dyn DurableStorage>,
    pub sync_type: FileSyncType,
}

impl fmt::Debug for DurableStorageSync {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableStorageSync")
            .field("sync_type", &self.sync_type)
            .finish_non_exhaustive()
    }
}

pub struct DurableStorageTruncate {
    pub storage: Arc<dyn DurableStorage>,
}

impl fmt::Debug for DurableStorageTruncate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableStorageTruncate")
            .finish_non_exhaustive()
    }
}
