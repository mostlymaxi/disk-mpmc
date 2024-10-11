use std::{
    cell::RefCell,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering},
        Arc, RwLock,
    },
};

use mmapcell::MmapCell;

type LenType = u32;
type IdxType = u32;

const MAX_PAGES: usize = 3;
const MAX_RECEIVER_GROUPS: usize = 64;
const MAX_MESSAGES_PER_PAGE: usize = 2_usize.pow(16) - 1;
const EXPECTED_MESSAGE_SIZE_BYTES: usize = 4096;
const MAX_BYTES_PER_PAGE: usize = MAX_MESSAGES_PER_PAGE * EXPECTED_MESSAGE_SIZE_BYTES;

const COUNT_INCREMENT: u64 = u32::MAX as u64 + 1;
const WRITE_IDX_MASK: u64 = u32::MAX as u64;

#[repr(C)]
pub struct DataPage {
    count_write_idx: AtomicU64,
    receiver_group_count: [AtomicU32; MAX_RECEIVER_GROUPS],
    write_idx_map: [AtomicU32; MAX_MESSAGES_PER_PAGE],
    buf: [u8; MAX_BYTES_PER_PAGE],
}

#[derive(Clone)]
pub struct DataPagesManager {
    path: PathBuf,
    datapage_count: Arc<AtomicUsize>,
    datapage_ring: Arc<RwLock<[Arc<MmapCell<DataPage>>; MAX_PAGES]>>,
}

impl DataPagesManager {
    pub fn get_or_create_datapage(
        &self,
        num: usize,
    ) -> Result<Arc<MmapCell<DataPage>>, std::io::Error> {
        let datapages = self.datapage_ring.read().expect("read lock");

        if num > self.datapage_count.load(Ordering::Relaxed) {
            drop(datapages);
            let mut datapages = self.datapage_ring.write().expect("write lock");

            if num > self.datapage_count.load(Ordering::Relaxed) {
                let last_datapage = self
                    .datapage_count
                    .fetch_add(1, Ordering::Relaxed)
                    .wrapping_add(1);

                datapages[last_datapage % MAX_PAGES] = Arc::new(DataPage::new(&self.path)?);

                std::fs::remove_file(self.path.join(format!("{}", last_datapage - MAX_PAGES)))?;
            }

            let dp_count = self.datapage_count.load(Ordering::Relaxed);
            return Ok(datapages[dp_count % MAX_PAGES].clone());
        }

        Ok(datapages[num % MAX_PAGES].clone())
    }

    pub fn get_latest_datapage(&self) -> Arc<MmapCell<DataPage>> {
        let datapages = self.datapage_ring.read().expect("read lock");
        let last_datapage = self.datapage_count.load(Ordering::Relaxed);
        datapages[last_datapage % MAX_PAGES].clone()
    }
}

#[derive(Clone)]
pub struct Receiver {
    group: usize,
    datapage_count: Arc<AtomicUsize>,
    manager: DataPagesManager,
    inner: RefCell<DataPageReceiver>,
}

impl Receiver {
    pub fn pop(&self) -> Result<&[u8], std::io::Error> {
        self.inner.borrow().pop();

        let datapage = self.manager.get_or_create_datapage(
            self.datapage_count
                .fetch_add(1, Ordering::Relaxed)
                .wrapping_add(1),
        )?;

        self.inner.replace(DataPageReceiver {
            group: self.group,
            datapage,
        });

        Ok(&[b'a'])
    }
}

#[derive(Clone)]
pub struct DataPageReceiver {
    group: usize,
    datapage: Arc<MmapCell<DataPage>>,
}

impl DataPageReceiver {
    pub fn pop(&self) -> &[u8] {
        let count =
            self.datapage.get().receiver_group_count[self.group].fetch_add(1, Ordering::Relaxed);

        self.datapage.get().get(count)
    }
}

#[derive(Debug)]
struct DataPageFull;
#[derive(Debug)]
struct EndOfDataPage;

impl DataPage {
    const SIZE_OF_LEN: usize = size_of::<LenType>();

    pub fn new<P: AsRef<Path>>(path: P) -> Result<MmapCell<DataPage>, std::io::Error> {
        unsafe { MmapCell::new_named(path) }
    }

    fn wake_all_frfr(&self, last_count: usize) {}

    pub fn push<T: AsRef<[u8]>>(&mut self, data: T) -> Result<(), DataPageFull> {
        let count_write_idx = self.count_write_idx.fetch_add(
            COUNT_INCREMENT + data.as_ref().len() as u64 + Self::SIZE_OF_LEN as u64,
            Ordering::Relaxed,
        );

        let count = ((count_write_idx & !WRITE_IDX_MASK) >> 32) as usize;
        let write_idx = (count_write_idx & WRITE_IDX_MASK) as usize;
        let full_msg_len = data.as_ref().len() + Self::SIZE_OF_LEN;

        if count >= MAX_MESSAGES_PER_PAGE {
            return Err(DataPageFull);
        }

        if write_idx + full_msg_len >= MAX_BYTES_PER_PAGE {
            let magic_num = MAX_MESSAGES_PER_PAGE << 32 + write_idx
            self.count_write_idx.load(MAX_MESSAGES_PER_PAGE << 32, order)
            return Err(DataPageFull);
        }

        self.buf[write_idx..write_idx + Self::SIZE_OF_LEN]
            .copy_from_slice(&(data.as_ref().len() as LenType).to_le_bytes());

        self.buf
            [write_idx + Self::SIZE_OF_LEN..write_idx + Self::SIZE_OF_LEN + data.as_ref().len()]
            .copy_from_slice(data.as_ref());

        self.write_idx_map[count].store(write_idx as IdxType + 1, Ordering::Release);
        atomic_wait::wake_all(&self.write_idx_map[count]);

        Ok(())
    }

    pub fn get(&self, count: u32) -> Result<&[u8], EndOfDataPage> {
        let count = count as usize;

        if count >= MAX_MESSAGES_PER_PAGE {
            return Err(EndOfDataPage);
        }

        let idx = loop {
            atomic_wait::wait(&self.write_idx_map[count], 0);
            match self.write_idx_map[count].load(Ordering::Acquire) {
                0 => continue,
                idx => break idx - 1,
            }
        } as usize;

        let len = LenType::from_le_bytes(
            self.buf[idx..idx + Self::SIZE_OF_LEN]
                .try_into()
                .expect("u32 is 4 bytes"),
        ) as usize;

        Ok(&self.buf[idx + Self::SIZE_OF_LEN..idx + Self::SIZE_OF_LEN + len])
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
            let msg = p_clone.get().get(0);
            assert!(String::from_utf8_lossy(msg).eq(TEST_MESSAGE));
        });

        thread::sleep(std::time::Duration::from_millis(100));

        p.get_mut().push(TEST_MESSAGE);
        let _ = t.join();
        let _ = std::fs::remove_file(TEST_PAGE_PATH);
    }
}
