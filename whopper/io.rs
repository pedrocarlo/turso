use libc::{
    MAP_SHARED, O_CREAT, O_RDWR, PROT_READ, PROT_WRITE, close, ftruncate, mmap, munmap, open,
};
use rand::{Rng, RngCore};
use rand_chacha::ChaCha8Rng;
use std::collections::HashSet;
use std::ptr;
use std::sync::{Arc, Mutex, Weak};
use tracing::debug;
use turso_core::{Clock, Completion, File, IO, Instant, OpenFlags, Result};

#[derive(Debug, Clone)]
pub struct IOFaultConfig {
    /// Probability of a cosmic ray bit flip on write (0.0-1.0)
    pub cosmic_ray_probability: f64,
}

impl Default for IOFaultConfig {
    fn default() -> Self {
        Self {
            cosmic_ray_probability: 0.0,
        }
    }
}

pub struct SimulatorIO {
    files: Mutex<Vec<(String, Weak<SimulatorFile>)>>,
    keep_files: bool,
    rng: Mutex<ChaCha8Rng>,
    fault_config: IOFaultConfig,
}

impl SimulatorIO {
    pub fn new(keep_files: bool, rng: ChaCha8Rng, fault_config: IOFaultConfig) -> Self {
        debug!("SimulatorIO fault config: {:?}", fault_config);
        Self {
            files: Mutex::new(Vec::new()),
            keep_files,
            rng: Mutex::new(rng),
            fault_config,
        }
    }
}

impl Drop for SimulatorIO {
    fn drop(&mut self) {
        let files = self.files.lock().unwrap();
        let paths: HashSet<String> = files.iter().map(|(path, _)| path.clone()).collect();
        if !self.keep_files {
            for path in paths.iter() {
                unsafe {
                    let c_path = std::ffi::CString::new(path.clone()).unwrap();
                    libc::unlink(c_path.as_ptr());
                }
            }
        } else {
            for path in paths.iter() {
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

        // Store weak reference to avoid keeping files open forever
        let mut files = self.files.lock().unwrap();
        files.push((path.to_string(), Arc::downgrade(&file)));

        Ok(file as Arc<dyn File>)
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        let mut files = self.files.lock().unwrap();
        files.retain(|(p, _)| p != path);

        // File descriptor and mmap will be cleaned up when the Arc<SimulatorFile> is dropped
        if !self.keep_files {
            unsafe {
                let c_path = std::ffi::CString::new(path).unwrap();
                libc::unlink(c_path.as_ptr());
            }
        }
        Ok(())
    }

    fn step(&self) -> Result<()> {
        // Inject cosmic ray faults with configured probability
        if self.fault_config.cosmic_ray_probability > 0.0 {
            let mut rng = self.rng.lock().unwrap();
            if rng.random::<f64>() < self.fault_config.cosmic_ray_probability {
                // Clean up dead weak references and collect live files
                let mut files = self.files.lock().unwrap();
                files.retain(|(_, weak)| weak.strong_count() > 0);

                // Collect files that are still alive
                let open_files: Vec<_> = files
                    .iter()
                    .filter_map(|(path, weak)| weak.upgrade().map(|file| (path.clone(), file)))
                    .collect();

                if !open_files.is_empty() {
                    let file_idx = rng.random_range(0..open_files.len());
                    let (path, file) = &open_files[file_idx];

                    // Get the actual file size (not the mmap size)
                    let file_size = *file.size.lock().unwrap();
                    if file_size > 0 {
                        // Pick a random offset within the actual file size
                        let byte_offset = rng.random_range(0..file_size);
                        let bit_idx = rng.random_range(0..8);

                        unsafe {
                            let old_byte = *file.data.add(byte_offset);
                            *file.data.add(byte_offset) ^= 1 << bit_idx;
                            println!(
                                "Cosmic ray! File: {} - Flipped bit {} at offset {} (0x{:02x} -> 0x{:02x})",
                                path,
                                bit_idx,
                                byte_offset,
                                old_byte,
                                *file.data.add(byte_offset)
                            );
                        }
                    }
                }
            }
        }
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
    size: Mutex<usize>,
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

            Self {
                data,
                size: Mutex::new(0),
                fd,
            }
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
                // Update the file size if we wrote beyond the current size
                let mut size = self.size.lock().unwrap();
                if pos + len > *size {
                    *size = pos + len;
                }
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

        // Update the file size if we wrote beyond the current size
        if total_written > 0 {
            let mut size = self.size.lock().unwrap();
            let end_pos = (pos as usize) + total_written;
            if end_pos > *size {
                *size = end_pos;
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

    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        let mut size = self.size.lock().unwrap();
        *size = len as usize;
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
        Ok(*self.size.lock().unwrap() as u64)
    }
}
