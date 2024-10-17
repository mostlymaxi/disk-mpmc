use std::{marker::PhantomData, sync::Arc, time::Duration};

use mmapcell::MmapCell;

mod datapage;
pub mod manager;

use datapage::DataPage;
use manager::DataPagesManager;

#[derive(Clone)]
pub struct Grouped;

#[derive(Clone)]
pub struct Anonymous;

#[derive(Clone)]
pub struct Receiver<T> {
    group: usize,
    anon_count: u32,
    manager: DataPagesManager,
    datapage_count: usize,
    datapage: Arc<MmapCell<DataPage>>,
    _type: std::marker::PhantomData<T>,
}

impl Receiver<Grouped> {
    pub fn new(group: usize, manager: DataPagesManager) -> Result<Self, std::io::Error> {
        let (datapage_count, datapage) = manager.get_or_create_datapage(0)?;
        //let datapage_count = RefCell::new(datapage_count);
        //let datapage = RefCell::new(datapage);

        Ok(Receiver {
            group,
            anon_count: 0,
            manager,
            datapage_count,
            datapage,
            _type: PhantomData,
        })
    }

    pub fn pop_with_timeout(&mut self, timeout: Duration) -> Result<Option<&[u8]>, std::io::Error> {
        loop {
            let count = self.datapage.get().increment_group_count(self.group, 1);

            match self.datapage.get().get_with_timeout(count, timeout) {
                Ok(data) => return Ok(data),
                // WARN: if you add more errors in the future make sure to match on them!!!
                Err(_e) => {}
            };

            let (dp_count, datapage) = self
                .manager
                .get_or_create_datapage(self.datapage_count.wrapping_add(1))?;

            self.datapage_count = dp_count;
            self.datapage = datapage;
        }
    }

    pub fn pop(&mut self) -> Result<&[u8], std::io::Error> {
        loop {
            let count = self.datapage.get().increment_group_count(self.group, 1);

            match self.datapage.get().get(count) {
                Ok(data) => return Ok(data),
                // WARN: if you add more errors in the future make sure to match on them!!!
                Err(_e) => {}
            };

            let (dp_count, datapage) = self
                .manager
                .get_or_create_datapage(self.datapage_count.wrapping_add(1))?;

            self.datapage_count = dp_count;
            self.datapage = datapage;
        }
    }
}

impl Receiver<Anonymous> {
    pub fn pop(&mut self) -> Result<&[u8], std::io::Error> {
        loop {
            let count = self.anon_count;
            self.anon_count += 1;

            match self.datapage.get().get(count) {
                Ok(data) => return Ok(data),
                // WARN: if you add more errors in the future make sure to match on them!!!
                Err(_e) => {}
            };

            self.anon_count = 0;

            let (dp_count, datapage) = self
                .manager
                .get_or_create_datapage(self.datapage_count.wrapping_add(1))?;

            self.datapage_count = dp_count;
            self.datapage = datapage;
        }
    }
}

#[derive(Clone)]
pub struct Sender {
    manager: DataPagesManager,
    datapage_count: usize,
    datapage: Arc<MmapCell<DataPage>>,
}

impl Sender {
    pub fn new(manager: DataPagesManager) -> Result<Self, std::io::Error> {
        let (datapage_count, datapage) = manager.get_or_create_datapage(0)?;
        //let datapage_count = RefCell::new(datapage_count);
        //let datapage = RefCell::new(datapage);

        Ok(Sender {
            manager,
            datapage_count,
            datapage,
        })
    }

    pub fn push<T: AsRef<[u8]>>(&mut self, data: T) -> Result<(), std::io::Error> {
        loop {
            match self.datapage.get_mut().push(&data) {
                Ok(()) => return Ok(()),
                Err(_e) => {}
            }

            let (dp_count, datapage) = self
                .manager
                .get_or_create_datapage(self.datapage_count.wrapping_add(1))?;

            self.datapage_count = dp_count;
            self.datapage = datapage;
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
    fn sequential_test() {
        const TEST_MESSAGE: &str = const_str::repeat!("a", 100);

        tracing_subscriber::fmt::init();

        let path = mkdir_random();
        let manager = DataPagesManager::new(&path).unwrap();

        let mut tx = Sender::new(manager.clone()).unwrap();
        let now = Instant::now();

        for _ in 0..50_000_000 {
            tx.push(TEST_MESSAGE).unwrap();
        }
        let elapsed = now.elapsed();

        let test_msg_bytes = TEST_MESSAGE.as_bytes().len() * 50_000_000;
        let test_msg_mb = test_msg_bytes as f64 * 0.000001;
        info!(
            "pushed 50,000,000 messages ({:.2} MB) in {} ms [{:.2}MB/s]",
            test_msg_mb,
            elapsed.as_millis(),
            test_msg_bytes as f64 / elapsed.as_micros() as f64
        );

        let mut rx = Receiver::new(0, manager).unwrap();
        let now = Instant::now();
        for _ in 0..50_000_000 {
            rx.pop().unwrap();
        }
        let elapsed = now.elapsed();

        let test_msg_bytes = TEST_MESSAGE.as_bytes().len() * 50_000_000;
        let test_msg_mb = test_msg_bytes as f64 * 0.000001;
        info!(
            "popped 50,000,000 messages ({:.2} MB) in {} ms [{:.2}MB/s]",
            test_msg_mb,
            elapsed.as_millis(),
            test_msg_bytes as f64 / elapsed.as_micros() as f64
        );

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn spsc_test() {
        const TOTAL_MESSAGES: usize = 50_000_000;
        const NUM_THREADS: usize = 1;
        const TEST_MESSAGE: &str = const_str::repeat!("a", 100);

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
            let mut rx_clone = rx.clone();
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
            let mut tx_clone = tx.clone();
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

        let elapsed = now.elapsed();
        let test_msg_bytes = TEST_MESSAGE.as_bytes().len() * 50_000_000;
        let test_msg_mb = test_msg_bytes as f64 * 0.000001;
        info!(
            "pushed & popped 50,000,000 messages ({:.2} MB) in {} ms [{:.2}MB/s]",
            test_msg_mb,
            elapsed.as_millis(),
            test_msg_bytes as f64 / elapsed.as_micros() as f64
        );

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn mpmc_test() {
        const TOTAL_MESSAGES: usize = 50_000_000;
        const NUM_THREADS: usize = 8;
        const TEST_MESSAGE: &str = const_str::repeat!("a", 100);

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
            let mut rx_clone = rx.clone();
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
            let mut tx_clone = tx.clone();
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

        let elapsed = now.elapsed();
        let test_msg_bytes = TEST_MESSAGE.as_bytes().len() * 50_000_000;
        let test_msg_mb = test_msg_bytes as f64 * 0.000001;
        info!(
            "pushed & popped 50,000,000 messages ({:.2} MB) in {} ms [{:.2}MB/s]",
            test_msg_mb,
            elapsed.as_millis(),
            test_msg_bytes as f64 / elapsed.as_micros() as f64
        );

        std::fs::remove_dir_all(path).unwrap();
    }
}
