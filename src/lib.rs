use std::{
    path::Path,
    sync::atomic::{AtomicU32, AtomicU64, Ordering},
};

use mmapcell::MmapCell;

type LenType = u32;
type IdxType = u32;

const MAX_MESSAGES_PER_PAGE: usize = 2_usize.pow(16) - 1;
const EXPECTED_MESSAGE_SIZE_BYTES: usize = 4096;
const MAX_BYTES_PER_PAGE: usize = MAX_MESSAGES_PER_PAGE * EXPECTED_MESSAGE_SIZE_BYTES;

const COUNT_INCREMENT: u64 = u32::MAX as u64 + 1;
const WRITE_IDX_MASK: u64 = u32::MAX as u64;

pub struct DataPage {
    write_idx_map: [AtomicU32; MAX_MESSAGES_PER_PAGE],
    count_write_idx: AtomicU64,
    buf: [u8; MAX_BYTES_PER_PAGE],
}

impl DataPage {
    pub fn new<P: AsRef<Path>>(path: P) -> MmapCell<DataPage> {
        unsafe { MmapCell::new_named(path) }.unwrap()
    }

    pub fn push<T: AsRef<[u8]>>(&mut self, data: T) {
        let count_write_idx = self.count_write_idx.fetch_add(
            COUNT_INCREMENT + data.as_ref().len() as u64 + size_of::<u32>() as u64,
            Ordering::Relaxed,
        );

        let count = ((count_write_idx & !WRITE_IDX_MASK) >> 32) as usize;
        let write_idx = (count_write_idx & WRITE_IDX_MASK) as usize;

        if count >= MAX_MESSAGES_PER_PAGE || write_idx + data.as_ref().len() >= MAX_BYTES_PER_PAGE {
            // PAGE FULL
            return;
        }

        self.buf[write_idx..write_idx + size_of::<LenType>()]
            .copy_from_slice(&(data.as_ref().len() as LenType).to_le_bytes());
        self.buf[write_idx + size_of::<LenType>()
            ..write_idx + size_of::<LenType>() + data.as_ref().len()]
            .copy_from_slice(data.as_ref());

        self.write_idx_map[count].store(write_idx as IdxType + 1, Ordering::Release);
        atomic_wait::wake_all(&self.write_idx_map[count]);
    }

    pub fn pop(&self, count: usize) -> &[u8] {
        let idx = loop {
            atomic_wait::wait(&self.write_idx_map[count], 0);
            match self.write_idx_map[count].load(Ordering::Acquire) {
                0 => continue,
                idx => break idx as usize - 1,
            }
        };

        let len = LenType::from_le_bytes(
            self.buf[idx..idx + size_of::<LenType>()]
                .try_into()
                .expect("u32 is 4 bytes"),
        ) as usize;

        &self.buf[idx + size_of::<LenType>()..idx + size_of::<LenType>() + len]
    }
}

#[cfg(test)]
mod test {
    use std::{sync::Arc, thread};

    use super::*;

    #[test]
    fn simple_test() {
        const TEST_PAGE_PATH: &str = "/tmp/test-data-page.bin";
        const TEST_MESSAGE: &str = "test123asdf asdf asdf";

        let p = Arc::new(DataPage::new(TEST_PAGE_PATH));
        let p_clone = p.clone();

        let t = thread::spawn(move || {
            let msg = p_clone.get().pop(0);
            assert!(String::from_utf8_lossy(msg).eq(TEST_MESSAGE));
        });

        thread::sleep(std::time::Duration::from_millis(100));

        p.get_mut().push(TEST_MESSAGE);
        let _ = t.join();
        let _ = std::fs::remove_file(TEST_PAGE_PATH);
    }
}
