use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock,
    },
};

use mmapcell::MmapCell;

use crate::datapage::DataPage;

const MAX_PAGES: usize = 3;

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
