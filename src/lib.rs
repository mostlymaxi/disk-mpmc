use std::{cell::RefCell, marker::PhantomData, sync::Arc};

use mmapcell::MmapCell;

mod datapage;
mod manager;

use datapage::DataPage;
use manager::DataPagesManager;

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
            let count = datapage.get().increment_group_count(self.group, 1);

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

#[cfg(test)]
mod test {
    use std::{
        path::{Path, PathBuf},
        thread,
    };

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
