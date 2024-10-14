use std::{cell::RefCell, marker::PhantomData, sync::Arc};

use mmapcell::MmapCell;

mod datapage;
mod manager;

use datapage::DataPage;
use manager::DataPagesManager;

#[derive(Clone)]
pub struct Grouped;

#[derive(Clone)]
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
            let count = datapage.get().increment_group_count(self.group, 1);

            match datapage.get().get(count) {
                Ok(data) => return Ok(data),
                // WARN: if you add more errors in the future make sure to match on them!!!
                Err(_e) => {}
            };

            drop(datapage);

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

#[derive(Clone)]
pub struct Sender {
    manager: DataPagesManager,
    datapage_count: RefCell<usize>,
    datapage: RefCell<Arc<MmapCell<DataPage>>>,
}

impl Sender {
    pub fn new(manager: DataPagesManager) -> Result<Self, std::io::Error> {
        let (datapage_count, datapage) = manager.get_or_create_datapage(0)?;
        let datapage_count = RefCell::new(datapage_count);
        let datapage = RefCell::new(datapage);

        Ok(Sender {
            manager,
            datapage_count,
            datapage,
        })
    }

    pub fn push<T: AsRef<[u8]>>(&self, data: T) -> Result<(), std::io::Error> {
        loop {
            match self.datapage.borrow().get_mut().push(&data) {
                Ok(()) => return Ok(()),
                Err(_e) => {}
            }

            let (dp_count, datapage) = self
                .manager
                .get_or_create_datapage(self.datapage_count.borrow().wrapping_add(1))?;

            *self.datapage_count.borrow_mut() = dp_count;
            self.datapage.replace(datapage);
        }
    }
}

#[cfg(test)]
mod test {
    use std::{
        path::{Path, PathBuf},
        sync::{
            atomic::{AtomicUsize, Ordering},
            mpsc, Barrier,
        },
        thread,
        time::Instant,
    };

    use rand::random;
    use tracing::info;

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
    fn send_receive_test() {
        const TEST_MESSAGE: &str = "test123asdf asdf asdf";
        let path = mkdir_random();
        let manager = DataPagesManager::new(&path).unwrap();
        let rx = Receiver::new(0, manager.clone()).unwrap();

        let t = thread::spawn(move || {
            let msg = rx.pop().unwrap();
            assert!(String::from_utf8_lossy(msg).eq(TEST_MESSAGE));
        });

        thread::sleep(std::time::Duration::from_millis(100));

        let tx = Sender::new(manager).unwrap();
        tx.push(TEST_MESSAGE).unwrap();

        let e = t.join();
        std::fs::remove_dir_all(path).unwrap();

        e.unwrap();
    }

    #[test]
    fn big() {
        const TOTAL_MESSAGES: usize = 5_000_000;
        const NUM_THREADS: usize = 8;
        const TEST_MESSAGE: &str = "test123asdf asdf asdf";
        tracing_subscriber::fmt::init();

        let path = mkdir_random();
        let manager = DataPagesManager::new(&path).unwrap();
        let rx = Receiver::new(0, manager.clone()).unwrap();
        let (tx_end, rx_end) = mpsc::sync_channel(1);

        let mut handles = Vec::new();
        let msg_count = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(NUM_THREADS * 2 + 1));

        for _ in 0..NUM_THREADS {
            let tx_end_clone = tx_end.clone();
            let rx_clone = rx.clone();
            let msgs_count_clone = msg_count.clone();
            let barrier_clone = barrier.clone();

            handles.push(thread::spawn(move || {
                barrier_clone.wait();

                loop {
                    let m = msgs_count_clone.load(Ordering::Relaxed);

                    if m == TOTAL_MESSAGES {
                        break;
                    }

                    let msg = rx_clone.pop().unwrap(); // blocking
                    assert!(String::from_utf8_lossy(msg).eq(TEST_MESSAGE));
                    msgs_count_clone.fetch_add(1, Ordering::Relaxed);
                }

                let _ = tx_end_clone.send(());
            }));
        }

        let tx = Sender::new(manager).unwrap();

        for _ in 0..NUM_THREADS {
            let tx_clone = tx.clone();
            let barrier_clone = barrier.clone();

            handles.push(thread::spawn(move || {
                barrier_clone.wait();

                for _ in 0..TOTAL_MESSAGES / NUM_THREADS {
                    tx_clone.push(TEST_MESSAGE).unwrap();
                }
            }));
        }

        barrier.wait();
        let now = Instant::now();
        let _ = rx_end.recv();
        let elapsed = now.elapsed().as_millis();
        info!("wrote {} in {} milliseconds", TOTAL_MESSAGES, elapsed);

        // std::fs::emove_dir_all(path).unwrap();
    }
}
