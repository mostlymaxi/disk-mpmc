use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use mmapcell::MmapCell;
use parking_lot::RwLock;

use crate::datapage::DataPage;

#[derive(Clone)]
pub struct DataPagesManager {
    path: PathBuf,
    max_datapages: Arc<AtomicUsize>,
    datapage_count: Arc<AtomicUsize>,
    datapage_ring: Arc<RwLock<VecDeque<Arc<MmapCell<DataPage>>>>>,
}

impl DataPagesManager {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> {
        // TODO: actually get the page count
        let total_page_count = 0;

        // let mut init_pages = VecDeque::with_capacity(MAX_PAGES + 1);
        let mut init_pages = VecDeque::new();
        init_pages.push_back(unsafe {
            Arc::new(MmapCell::new_named(
                path.as_ref().join(total_page_count.to_string()),
            )?)
        });

        Ok(DataPagesManager {
            path: path.as_ref().into(),
            max_datapages: Arc::new(AtomicUsize::new(usize::MAX)),
            datapage_count: Arc::new(AtomicUsize::new(total_page_count)),
            datapage_ring: Arc::new(RwLock::new(init_pages)),
        })
    }

    fn load_total_page_count<P: AsRef<Path>>(path: P) {}

    pub fn set_max_datapages(&mut self, val: usize) {
        let _dp = self.datapage_ring.write();
        self.max_datapages.store(val, Ordering::Relaxed);
    }

    pub fn get_max_datapages(&self) -> usize {
        self.max_datapages.load(Ordering::Relaxed)
    }

    pub fn get_last_datapage(&self) -> Result<(usize, Arc<MmapCell<DataPage>>), std::io::Error> {
        let datapages = self.datapage_ring.read();
        let last_datapage = datapages
            .back()
            .ok_or(std::io::Error::other("DataPage not found"))?;

        let dp_count = self.datapage_count.load(Ordering::Relaxed);
        Ok((dp_count, last_datapage.clone()))
    }

    pub fn get_or_create_datapage(
        &self,
        num: usize,
    ) -> Result<(usize, Arc<MmapCell<DataPage>>), std::io::Error> {
        let mut datapages = self.datapage_ring.upgradable_read();
        let dp_count = self.datapage_count.load(Ordering::Relaxed);
        let max_dps = self.max_datapages.load(Ordering::Relaxed);

        if num > dp_count {
            return datapages.with_upgraded(|datapages| {
                let dp_count = self.datapage_count.fetch_add(1, Ordering::Relaxed) + 1;
                let max_dps = self.max_datapages.load(Ordering::Relaxed);

                if dp_count >= max_dps {
                    std::fs::remove_file(self.path.join(format!("{}", dp_count - max_dps)))?;

                    let _ = datapages.pop_front();
                }

                datapages.push_back(Arc::new(DataPage::new(
                    self.path.join(dp_count.to_string()),
                )?));

                Ok::<(usize, Arc<MmapCell<DataPage>>), std::io::Error>((
                    dp_count,
                    datapages[dp_count % max_dps].clone(),
                ))
            });
        }

        let dp_count = num.max(dp_count.saturating_sub(max_dps));

        Ok((dp_count, datapages[dp_count % max_dps].clone()))
    }
}
