use std::{
    path::Path,
    sync::atomic::{AtomicU32, AtomicU64, Ordering},
    time::Duration,
};

use linux_futex::{Futex, Private};
use mmapcell::MmapCell;

type LenType = u32;
type IdxType = u32;

const IDX_SALT: u32 = 1;
const MAX_RECEIVER_GROUPS: usize = 64;
const MAX_MESSAGES_PER_PAGE: u32 = 2_u32.pow(16) - 1;
const DP_BUILD_EMSG_SIZE: &str = match option_env!("FRANZ_BUILD_EMSG_SIZE") {
    Some(m) => m,
    None => "2048",
};
const EXPECTED_MESSAGE_SIZE_BYTES: u32 = const_str::parse!(DP_BUILD_EMSG_SIZE, u32);
const MAX_BYTES_PER_PAGE: u32 = MAX_MESSAGES_PER_PAGE * EXPECTED_MESSAGE_SIZE_BYTES;

const WRITE_IDX_MASK: u64 = !(u32::MAX as u64);
const COUNT_MASK: u64 = !WRITE_IDX_MASK;

// u64 where the first 32 bits are used for the write_idx
// and the last 32 bits are used for count
union CountWriteIdx {
    write_idx: std::mem::ManuallyDrop<AtomicU64>,
    _count: std::mem::ManuallyDrop<AtomicU32>,
}

impl CountWriteIdx {
    pub fn fetch_add(&self, val: u32) -> (u32, u32) {
        let val = val as u64;
        let write_idx_count =
            unsafe { self.write_idx.fetch_add((val << 32) + 1, Ordering::Release) };
        // atomic_wait::wake_one(unsafe { &*self.count });

        let write_idx = ((write_idx_count & WRITE_IDX_MASK) >> 32) as u32;
        let count = (write_idx_count & COUNT_MASK) as u32;

        (write_idx, count)
    }
}

#[derive(Debug)]
pub struct DataPageFull;

#[derive(Debug)]
pub struct EndOfDataPage;

#[repr(C)]
pub struct DataPage {
    count_write_idx: CountWriteIdx,
    receiver_group_count: [AtomicU32; MAX_RECEIVER_GROUPS],
    idx_map_with_salt: [Futex<Private>; MAX_MESSAGES_PER_PAGE as usize],
    buf: [u8; MAX_BYTES_PER_PAGE as usize],
}

impl DataPage {
    const SIZE_OF_LEN: usize = size_of::<LenType>();

    pub fn increment_group_count(&self, group: usize, val: u32) -> u32 {
        // maybe try waiting on the group count or something idk
        // let count = self.receiver_group_count[group].load(Ordering::Relaxed);
        // self.count_write_idx.count.wa
        self.receiver_group_count[group].fetch_add(val, Ordering::Release)
    }

    pub fn new<P: AsRef<Path>>(path: P) -> Result<MmapCell<DataPage>, std::io::Error> {
        unsafe { MmapCell::new_named(path) }
    }

    pub fn push<T: AsRef<[u8]>>(&mut self, data: T) -> Result<(), DataPageFull> {
        let (write_idx, count) = self
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
            self.idx_map_with_salt[count as usize]
                .value
                .store(u32::MAX, Ordering::Release);

            self.idx_map_with_salt[count as usize].wake(i32::MAX);

            return Err(DataPageFull);
        }

        self.buf[write_idx as usize..write_idx as usize + Self::SIZE_OF_LEN]
            .copy_from_slice(&(data.as_ref().len() as LenType).to_le_bytes());

        self.buf[write_idx as usize + Self::SIZE_OF_LEN
            ..write_idx as usize + Self::SIZE_OF_LEN + data.as_ref().len()]
            .copy_from_slice(data.as_ref());

        self.idx_map_with_salt[count as usize]
            .value
            .store(write_idx as IdxType + IDX_SALT, Ordering::Release);

        self.idx_map_with_salt[count as usize].wake(i32::MAX);

        Ok(())
    }

    pub fn try_get(&self, count: u32) -> Result<Option<&[u8]>, EndOfDataPage> {
        if count >= MAX_MESSAGES_PER_PAGE {
            return Err(EndOfDataPage);
        }

        let idx_with_salt = match self.idx_map_with_salt[count as usize]
            .value
            .load(Ordering::Acquire)
        {
            0 => return Ok(None),
            i => i,
        };

        if idx_with_salt >= MAX_BYTES_PER_PAGE {
            let next_count = count.saturating_add(1);

            self.idx_map_with_salt[next_count as usize]
                .value
                .store(u32::MAX, Ordering::Release);

            self.idx_map_with_salt[next_count as usize].wake(i32::MAX);

            return Err(EndOfDataPage);
        }

        let idx = idx_with_salt.saturating_sub(IDX_SALT);

        let len = LenType::from_le_bytes(
            self.buf[idx as usize..idx as usize + Self::SIZE_OF_LEN]
                .try_into()
                .expect("u32 is 4 bytes"),
        );

        Ok(Some(
            &self.buf
                [idx as usize + Self::SIZE_OF_LEN..idx as usize + Self::SIZE_OF_LEN + len as usize],
        ))
    }

    pub fn get_with_timeout(
        &self,
        count: u32,
        timeout: Duration,
    ) -> Result<Option<&[u8]>, EndOfDataPage> {
        if count >= MAX_MESSAGES_PER_PAGE {
            return Err(EndOfDataPage);
        }

        // why does this look like this??
        // stfu it used to be a loop
        let idx_with_salt = 'out: {
            // futex man pages seem to imply
            // we should check the value prior
            // to doing the syscall idrk
            match self.idx_map_with_salt[count as usize]
                .value
                .load(Ordering::Acquire)
            {
                0 => {}
                i => break 'out i,
            }

            let _ = self.idx_map_with_salt[count as usize].wait_for(0, timeout);
            match self.idx_map_with_salt[count as usize]
                .value
                .load(Ordering::Acquire)
            {
                0 => return Ok(None),
                i => break 'out i,
            }
        };

        if idx_with_salt >= MAX_BYTES_PER_PAGE {
            let next_count = count.saturating_add(1);

            self.idx_map_with_salt[next_count as usize]
                .value
                .store(u32::MAX, Ordering::Release);

            self.idx_map_with_salt[next_count as usize].wake(i32::MAX);
            return Err(EndOfDataPage);
        }

        let idx = idx_with_salt.saturating_sub(IDX_SALT);

        let len = LenType::from_le_bytes(
            self.buf[idx as usize..idx as usize + Self::SIZE_OF_LEN]
                .try_into()
                .expect("u32 is 4 bytes"),
        );

        Ok(Some(
            &self.buf
                [idx as usize + Self::SIZE_OF_LEN..idx as usize + Self::SIZE_OF_LEN + len as usize],
        ))
    }

    pub fn get(&self, count: u32) -> Result<&[u8], EndOfDataPage> {
        if count >= MAX_MESSAGES_PER_PAGE {
            return Err(EndOfDataPage);
        }

        let idx_with_salt = loop {
            // futex man pages seem to imply
            // we should check the value prior
            // to doing the syscall idrk
            match self.idx_map_with_salt[count as usize]
                .value
                .load(Ordering::Acquire)
            {
                0 => {}
                i => break i,
            }

            let _ = self.idx_map_with_salt[count as usize].wait(0);
            match self.idx_map_with_salt[count as usize]
                .value
                .load(Ordering::Acquire)
            {
                0 => continue,
                i => break i,
            }
        };

        if idx_with_salt >= MAX_BYTES_PER_PAGE {
            let next_count = count.saturating_add(1);

            self.idx_map_with_salt[next_count as usize]
                .value
                .store(u32::MAX, Ordering::Release);

            self.idx_map_with_salt[next_count as usize].wake(i32::MAX);
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
    use std::{
        path::{Path, PathBuf},
        sync::Arc,
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
}
