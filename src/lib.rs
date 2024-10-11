use std::{
    cell::RefCell,
    marker::PhantomData,
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
const IDX_SALT: u32 = 1;
const MAX_RECEIVER_GROUPS: usize = 64;
const MAX_MESSAGES_PER_PAGE: u32 = 2_u32.pow(16) - 1;
const EXPECTED_MESSAGE_SIZE_BYTES: u32 = 4096;
const MAX_BYTES_PER_PAGE: u32 = MAX_MESSAGES_PER_PAGE * EXPECTED_MESSAGE_SIZE_BYTES;

const COUNT_INCREMENT: u64 = u32::MAX as u64 + 1;
const WRITE_IDX_MASK: u64 = !(u32::MAX as u64);

union CountWriteIdx {
    write_idx: std::mem::ManuallyDrop<AtomicU64>,
    count: std::mem::ManuallyDrop<AtomicU32>,
}

impl CountWriteIdx {
    pub fn fetch_add(&self, val: u32) -> (u32, u32) {
        let val = val as u64;
        let write_idx_count =
            unsafe { self.write_idx.fetch_add((val << 32) + 1, Ordering::Release) };
        atomic_wait::wake_all(unsafe { &*self.count });

        let write_idx = ((write_idx_count & WRITE_IDX_MASK) >> 32) as u32;
        let count = (write_idx_count & WRITE_IDX_MASK) as u32;

        (write_idx, count)
    }
}

#[repr(C)]
pub struct DataPage {
    count_write_idx: CountWriteIdx,
    receiver_group_count: [AtomicU32; MAX_RECEIVER_GROUPS],
    idx_map_with_salt: [AtomicU32; MAX_MESSAGES_PER_PAGE as usize],
    buf: [u8; MAX_BYTES_PER_PAGE as usize],
}

#[derive(Clone)]
pub struct DataPagesManager {
    path: PathBuf,
    datapage_count: Arc<AtomicUsize>,
    datapage_ring: Arc<RwLock<[Arc<MmapCell<DataPage>>; MAX_PAGES]>>,
}

impl DataPagesManager {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> {
        // TODO: actually get the page count
        let total_page_count = MAX_PAGES;

        let mut init_pages = Vec::new();
        for i in total_page_count - MAX_PAGES..total_page_count {
            init_pages.push(Arc::new(DataPage::new(path.as_ref().join(i.to_string()))?));
        }

        let init_pages = match init_pages.try_into() {
            Ok(init_pages) => init_pages,
            Err(_) => panic!("init_pages had incorrect size, this should never fail"),
        };

        Ok(DataPagesManager {
            path: path.as_ref().into(),
            datapage_count: Arc::new(AtomicUsize::new(total_page_count)),
            datapage_ring: Arc::new(RwLock::new(init_pages)),
        })
    }

    pub fn get_or_create_datapage(
        &self,
        num: usize,
    ) -> Result<(usize, Arc<MmapCell<DataPage>>), std::io::Error> {
        let datapages = self.datapage_ring.read().expect("read lock");
        let dp_count = self.datapage_count.load(Ordering::Relaxed);

        if num >= dp_count {
            drop(datapages);
            let mut datapages = self.datapage_ring.write().expect("write lock");

            let dp_count = match self.datapage_count.load(Ordering::Relaxed) {
                dp_count if num >= dp_count => {
                    let dp_count = self.datapage_count.fetch_add(1, Ordering::Relaxed);

                    datapages[dp_count % MAX_PAGES] =
                        Arc::new(DataPage::new(self.path.join(dp_count.to_string()))?);

                    std::fs::remove_file(
                        self.path
                            .join(format!("{}", dp_count.saturating_sub(MAX_PAGES))),
                    )?;
                    dp_count
                }
                dp_count if num < dp_count => dp_count,
                _ => unreachable!("num is neither >= or < somehow..."),
            };

            return Ok((dp_count, datapages[dp_count % MAX_PAGES].clone()));
        }

        let num = match num < dp_count.saturating_sub(MAX_PAGES) {
            true => dp_count.saturating_sub(MAX_PAGES),
            false => num,
        };

        Ok((num, datapages[num % MAX_PAGES].clone()))
    }

    //pub fn get_latest_datapage(&self) -> Arc<MmapCell<DataPage>> {
    //    let datapages = self.datapage_ring.read().expect("read lock");
    //    let last_datapage = self.datapage_count.load(Ordering::Relaxed);
    //    datapages[last_datapage % MAX_PAGES].clone()
    //}
}

pub struct Grouped;
pub struct Anonymous;

#[derive(Clone)]
pub struct Receiver<T> {
    group: usize,
    anon_count: RefCell<u32>,
    manager: DataPagesManager,
    datapage_count: RefCell<usize>,
    datapage: RefCell<Arc<MmapCell<DataPage>>>,
    _type: std::marker::PhantomData<T>,
}

impl Receiver<Grouped> {
    pub fn new(group: usize, manager: DataPagesManager) -> Result<Self, std::io::Error> {
        let (datapage_count, datapage) = manager.get_or_create_datapage(0)?;
        let datapage_count = RefCell::new(datapage_count);
        let datapage = RefCell::new(datapage);

        Ok(Receiver {
            group,
            anon_count: RefCell::new(0),
            manager,
            datapage_count,
            datapage,
            _type: PhantomData,
        })
    }

    pub fn pop(&self) -> Result<&[u8], std::io::Error> {
        loop {
            let datapage = self.datapage.borrow();
            let count =
                datapage.get().receiver_group_count[self.group].fetch_add(1, Ordering::Relaxed);

            match datapage.get().get(count) {
                Ok(data) => return Ok(data),
                // WARN: if you add more errors in the future make sure to match on them!!!
                Err(_e) => {}
            };

            let (dp_count, datapage) = self
                .manager
                .get_or_create_datapage(self.datapage_count.borrow().wrapping_add(1))?;

            *self.datapage_count.borrow_mut() = dp_count;
            self.datapage.replace(datapage);
        }
    }
}

impl Receiver<Anonymous> {
    pub fn pop(&self) -> Result<&[u8], std::io::Error> {
        loop {
            let datapage = self.datapage.borrow();
            let count = self.anon_count.borrow();
            *self.anon_count.borrow_mut() += 1;

            match datapage.get().get(*count) {
                Ok(data) => return Ok(data),
                // WARN: if you add more errors in the future make sure to match on them!!!
                Err(_e) => {}
            };

            *self.anon_count.borrow_mut() = 0;

            let (dp_count, datapage) = self
                .manager
                .get_or_create_datapage(self.datapage_count.borrow().wrapping_add(1))?;

            *self.datapage_count.borrow_mut() = dp_count;
            self.datapage.replace(datapage);
        }
    }
}

#[derive(Debug)]
pub struct DataPageFull;
#[derive(Debug)]
pub struct EndOfDataPage;

impl DataPage {
    const SIZE_OF_LEN: usize = size_of::<LenType>();

    pub fn new<P: AsRef<Path>>(path: P) -> Result<MmapCell<DataPage>, std::io::Error> {
        unsafe { MmapCell::new_named(path) }
    }

    pub fn push<T: AsRef<[u8]>>(&mut self, data: T) -> Result<(), DataPageFull> {
        let (count, write_idx) = self
            .count_write_idx
            .fetch_add(data.as_ref().len() as u32 + Self::SIZE_OF_LEN as u32);

        let full_msg_len = (data.as_ref().len() + Self::SIZE_OF_LEN) as u32;

        if count >= MAX_MESSAGES_PER_PAGE {
            return Err(DataPageFull);
        }

        // INFO:
        // if we hit MAX_BYTES_PER_PAGE before we max out the count
        // we need to ensure that no readers are waiting. We do this
        // by setting the current count to map to u32::MAX and then
        // let readers recursively wake the next reader until no more
        // readers are waiting. (there might still be a race condition here
        // but i'm kinda over it)
        if write_idx + full_msg_len >= MAX_BYTES_PER_PAGE {
            self.idx_map_with_salt[count as usize].store(u32::MAX, Ordering::Release);
            atomic_wait::wake_all(&self.idx_map_with_salt[count as usize]);

            return Err(DataPageFull);
        }

        self.buf[write_idx as usize..write_idx as usize + Self::SIZE_OF_LEN]
            .copy_from_slice(&(data.as_ref().len() as LenType).to_le_bytes());

        self.buf[write_idx as usize + Self::SIZE_OF_LEN
            ..write_idx as usize + Self::SIZE_OF_LEN + data.as_ref().len()]
            .copy_from_slice(data.as_ref());

        self.idx_map_with_salt[count as usize]
            .store(write_idx as IdxType + IDX_SALT, Ordering::Release);
        atomic_wait::wake_all(&self.idx_map_with_salt[count as usize]);

        Ok(())
    }

    pub fn get(&self, count: u32) -> Result<&[u8], EndOfDataPage> {
        if count >= MAX_MESSAGES_PER_PAGE {
            return Err(EndOfDataPage);
        }

        let idx_with_salt = loop {
            atomic_wait::wait(&self.idx_map_with_salt[count as usize], 0);
            match self.idx_map_with_salt[count as usize].load(Ordering::Acquire) {
                0 => continue,
                i => break i,
            }
        };

        if idx_with_salt >= MAX_BYTES_PER_PAGE {
            let next_count = count.saturating_add(1);

            self.idx_map_with_salt[next_count as usize].store(u32::MAX, Ordering::Release);
            atomic_wait::wake_all(&self.idx_map_with_salt[next_count as usize]);
            return Err(EndOfDataPage);
        }

        let idx = idx_with_salt.saturating_sub(IDX_SALT);

        let len = LenType::from_le_bytes(
            self.buf[idx as usize..idx as usize + Self::SIZE_OF_LEN]
                .try_into()
                .expect("u32 is 4 bytes"),
        );

        Ok(&self.buf
            [idx as usize + Self::SIZE_OF_LEN..idx as usize + Self::SIZE_OF_LEN + len as usize])
    }
}

#[cfg(test)]
mod test {
    use std::{sync::Arc, thread};

    use rand::random;

    use super::*;

    fn mkdir_random() -> PathBuf {
        const TEST_DIR: &str = "/tmp/";
        let num: u64 = random();
        let rand_file_name = format!("disk-mpmc-test-{:X}", num);

        let dir = Path::new(TEST_DIR).join(rand_file_name);

        std::fs::create_dir_all(&dir).unwrap();

        dir
    }

    #[test]
    fn simple_test() {
        const TEST_MESSAGE: &str = "test123asdf asdf asdf";
        let path = mkdir_random();

        let p = Arc::new(DataPage::new(path.join("0")).unwrap());
        let p_clone = p.clone();

        let t = thread::spawn(move || {
            let msg = p_clone.get().get(0).unwrap();
            assert!(String::from_utf8_lossy(msg).eq(TEST_MESSAGE));
        });

        thread::sleep(std::time::Duration::from_millis(100));

        p.get_mut().push(TEST_MESSAGE).unwrap();

        let e = t.join();
        std::fs::remove_dir_all(path).unwrap();

        e.unwrap();
    }

    #[test]
    fn receiver_test() {
        const TEST_MESSAGE: &str = "test123asdf asdf asdf";
        let path = mkdir_random();
        let manager = DataPagesManager::new(&path).unwrap();
        let rx = Receiver::new(0, manager.clone()).unwrap();

        let t = thread::spawn(move || {
            let msg = rx.pop().unwrap();
            assert!(String::from_utf8_lossy(msg).eq(TEST_MESSAGE));
        });

        thread::sleep(std::time::Duration::from_millis(100));

        let (count, p) = manager.get_or_create_datapage(0).unwrap();
        assert_eq!(count, 0);

        p.get_mut().push(TEST_MESSAGE).unwrap();

        let e = t.join();
        std::fs::remove_dir_all(path).unwrap();

        e.unwrap();
    }
}
