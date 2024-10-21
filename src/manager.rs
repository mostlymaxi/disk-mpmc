use std::{
    collections::VecDeque,
    ffi::OsStr,
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

const DATAPAGE_FILE_STEM: &str = ".dp.data.maxi";

impl DataPagesManager {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> {
        // TODO: actually get the page count
        let total_page_count = Self::load_total_page_count(path.as_ref())?;
        let max_page_count = Self::load_max_page(path.as_ref())?;

        let mut init_pages = VecDeque::new();
        for i in max_page_count.saturating_sub(total_page_count)..max_page_count + 1 {
            init_pages.push_back(unsafe {
                Arc::new(MmapCell::new_named(
                    path.as_ref()
                        .join(DATAPAGE_FILE_STEM)
                        .with_extension(i.to_string()),
                )?)
            });
        }

        Ok(DataPagesManager {
            path: path.as_ref().into(),
            max_datapages: Arc::new(AtomicUsize::new(usize::MAX)),
            datapage_count: Arc::new(AtomicUsize::new(total_page_count)),
            datapage_ring: Arc::new(RwLock::new(init_pages)),
        })
    }

    fn load_total_page_count<P: AsRef<Path>>(path: P) -> Result<usize, std::io::Error> {
        Ok(std::fs::read_dir(path)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().file_stem() == Some(OsStr::new(DATAPAGE_FILE_STEM)))
            .count())
    }

    fn load_max_page<P: AsRef<Path>>(path: P) -> Result<usize, std::io::Error> {
        Ok(std::fs::read_dir(path)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().file_stem() == Some(OsStr::new(DATAPAGE_FILE_STEM)))
            .filter_map(|e| {
                e.path()
                    .extension()
                    .and_then(|s| s.to_str().and_then(|s| s.parse().ok()))
            })
            .max()
            .unwrap_or(0))
    }

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
                    std::fs::remove_file(
                        self.path
                            .join(DATAPAGE_FILE_STEM)
                            .with_extension((dp_count - max_dps).to_string()),
                    )?;

                    let _ = datapages.pop_front();
                }

                datapages.push_back(Arc::new(DataPage::new(
                    self.path
                        .join(DATAPAGE_FILE_STEM)
                        .with_extension(dp_count.to_string()),
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
