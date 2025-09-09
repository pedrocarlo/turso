use libc::{
    MAP_SHARED, O_CREAT, O_RDWR, PROT_READ, PROT_WRITE, close, ftruncate, mmap, munmap, open,
};
use rand::RngCore;
use rand_chacha::ChaCha8Rng;
use std::collections::HashSet;
use std::ptr;
use std::sync::{Arc, Mutex};
use turso_core::{Clock, Completion, File, IO, Instant, OpenFlags, Result};

pub struct SimulatorIO {
    files: Mutex<HashSet<String>>,
    keep_files: bool,
    rng: Mutex<ChaCha8Rng>,
}

impl SimulatorIO {
    pub fn new(keep_files: bool, rng: ChaCha8Rng) -> Self {
        Self {
            files: Mutex::new(HashSet::new()),
            keep_files,
            rng: Mutex::new(rng),
        }
    }
}

impl Drop for SimulatorIO {
    fn drop(&mut self) {
        let files = self.files.lock().unwrap();
        if !self.keep_files {
            for path in files.iter() {
                unsafe {
                    let c_path = std::ffi::CString::new(path.clone()).unwrap();
                    libc::unlink(c_path.as_ptr());
                }
            }
        } else {
            for path in files.iter() {
                println!("Keeping file: {}", path);
            }
        }
    }
}

impl Clock for SimulatorIO {
    fn now(&self) -> Instant {
        Instant { secs: 0, micros: 0 } // Simple implementation for now
    }
}

impl IO for SimulatorIO {
    fn open_file(&self, path: &str, _flags: OpenFlags, _create_new: bool) -> Result<Arc<dyn File>> {
        let file = Arc::new(SimulatorFile::new(path));
        self.files.lock().unwrap().insert(path.to_string());
        Ok(file)
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        let mut files = self.files.lock().unwrap();
        if files.remove(path) {
            // File descriptor and mmap will be cleaned up when the Arc<SimulatorFile> is dropped
            if !self.keep_files {
                unsafe {
                    let c_path = std::ffi::CString::new(path).unwrap();
                    libc::unlink(c_path.as_ptr());
                }
            }
        }
        Ok(())
    }

    fn step(&self) -> Result<()> {
        // No-op for now
        Ok(())
    }

    fn wait_for_completion(&self, _completion: Completion) -> Result<()> {
        // No-op - completions are already completed immediately in the file operations
        Ok(())
    }

    fn generate_random_number(&self) -> i64 {
        let mut rng = self.rng.lock().unwrap();
        rng.next_u64() as i64
    }
}

const FILE_SIZE: usize = 1 << 30; // 1 GiB

struct SimulatorFile {
    data: *mut u8,
    size: usize,
    fd: i32,
}

impl SimulatorFile {
    fn new(file_path: &str) -> Self {
        unsafe {
            let c_path = std::ffi::CString::new(file_path).unwrap();

            let fd = open(c_path.as_ptr(), O_CREAT | O_RDWR, 0o644);
            if fd == -1 {
                let errno = std::io::Error::last_os_error();
                panic!("Failed to create file {}: {}", file_path, errno);
            }

            if ftruncate(fd, FILE_SIZE as i64) == -1 {
                let errno = std::io::Error::last_os_error();
                panic!("Failed to truncate file {}: {}", file_path, errno);
            }

            let data = mmap(
                ptr::null_mut(),
                FILE_SIZE,
                PROT_READ | PROT_WRITE,
                MAP_SHARED,
                fd,
                0,
            ) as *mut u8;

            if data == libc::MAP_FAILED as *mut u8 {
                let errno = std::io::Error::last_os_error();
                panic!("mmap failed for file {}: {}", file_path, errno);
            }

            Self { data, size: 0, fd }
        }
    }
}

impl Drop for SimulatorFile {
    fn drop(&mut self) {
        unsafe {
            munmap(self.data as *mut libc::c_void, FILE_SIZE);
            close(self.fd);
        }
    }
}

unsafe impl Send for SimulatorFile {}
unsafe impl Sync for SimulatorFile {}

impl File for SimulatorFile {
    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        let pos = pos as usize;
        let read_completion = c.as_read();
        let buffer = read_completion.buf_arc();
        let len = buffer.len();

        unsafe {
            if pos + len <= FILE_SIZE {
                ptr::copy_nonoverlapping(self.data.add(pos), buffer.as_mut_ptr(), len);
                c.complete(len as i32);
            } else {
                c.complete(0);
            }
        }
        Ok(c)
    }

    fn pwrite(
        &self,
        pos: u64,
        buffer: Arc<turso_core::Buffer>,
        c: Completion,
    ) -> Result<Completion> {
        let pos = pos as usize;
        let len = buffer.len();

        unsafe {
            if pos + len <= FILE_SIZE {
                ptr::copy_nonoverlapping(buffer.as_ptr(), self.data.add(pos), len);
                c.complete(len as i32);
            } else {
                c.complete(0);
            }
        }
        Ok(c)
    }

    fn pwritev(
        &self,
        pos: u64,
        buffers: Vec<Arc<turso_core::Buffer>>,
        c: Completion,
    ) -> Result<Completion> {
        let mut offset = pos as usize;
        let mut total_written = 0;

        unsafe {
            for buffer in buffers {
                let len = buffer.len();
                if offset + len <= FILE_SIZE {
                    ptr::copy_nonoverlapping(buffer.as_ptr(), self.data.add(offset), len);
                    offset += len;
                    total_written += len;
                } else {
                    break;
                }
            }
        }

        c.complete(total_written as i32);
        Ok(c)
    }

    fn sync(&self, c: Completion) -> Result<Completion> {
        // No-op for memory files
        c.complete(0);
        Ok(c)
    }

    fn truncate(&self, _len: u64, c: Completion) -> Result<Completion> {
        // Simple truncate implementation
        c.complete(0);
        Ok(c)
    }

    fn lock_file(&self, _exclusive: bool) -> Result<()> {
        // No-op for memory files
        Ok(())
    }

    fn unlock_file(&self) -> Result<()> {
        // No-op for memory files
        Ok(())
    }

    fn size(&self) -> Result<u64> {
        Ok(self.size as u64)
    }
}
