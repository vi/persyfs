//#![allow(unused)]
use fuser::MountOption;


mod api;

/*
mod persy_storage {
    use std::time::{Duration, SystemTime};

    use bincode::Options;
    use persy::{ByteVec, IndexIter, Persy};

    use crate::api::{Bytes, FileAttr, FileType, Ino};

    mod names {
        pub const ATTRS: &str = "attrs";
        pub const NEW_INODE: &str = "new_inode";
        pub const SYMLINKS: &str = "symlinks";
        pub const FILE_BLOCKS: &str = "file_blocks";
    }

    struct PersyLayer {
        persy: Persy,
        block_size: u32,
    }

    fn bco() -> impl bincode::Options {
        bincode::DefaultOptions::new()
            .with_limit(1024)
            .with_big_endian()
            .reject_trailing_bytes()
            .with_fixint_encoding()
    }

    pub fn open(
        file: std::fs::File,
        block_size: u32,
    ) -> anyhow::Result<impl crate::api::PersistanceLayer> {
        let mut config = persy::Config::new();
        config.change_cache_age_limit(Duration::from_secs(60));
        config.change_transaction_lock_timeout(Duration::from_secs(10));
        config.change_tx_strategy(persy::TxStrategy::LastWin);
        let mut recover_count = std::sync::atomic::AtomicU32::new(0);
        let persy = Persy::open_from_file_with_recover(file, config, |_| {
            recover_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            true
        })?;
        let recover_count = recover_count.load(std::sync::atomic::Ordering::Relaxed);
        if recover_count > 0 {
            log::warn!("Recovered {} transactions", recover_count);
        }

        Ok(PersyLayer { persy, block_size })
    }

    impl crate::api::PersistanceLayer for PersyLayer {
        fn new_inode(&self) -> anyhow::Result<u64> {
            let mut t = self.persy.begin()?;
            let new_inode: Vec<Ino> = t.get(names::NEW_INODE, &0)?.collect();
            anyhow::ensure!(new_inode.len() == 1);
            let mut new_inode = new_inode[0];
            loop {
                if t.get::<_, ByteVec>(names::ATTRS, &new_inode)?.len() == 0 {
                    break;
                }
                log::warn!(
                    "{} index seems inconsistent with the real inodes table",
                    names::NEW_INODE
                );
                new_inode += 1000;
            }
            let fa = FileAttr {
                ino: new_inode,
                size: 0,
                blocks: 0,
                atime: SystemTime::now(),
                mtime: SystemTime::now(),
                ctime: SystemTime::now(),
                crtime: SystemTime::now(),
                kind: FileType::CharDevice,
                perm: 0,
                nlink: 0,
                uid: 0,
                gid: 0,
                rdev: 0,
                blksize: 0,
                flags: 0,
            };
            let fa_bin = bco().serialize(&fa)?;
            t.put(names::ATTRS, new_inode, ByteVec::new(fa_bin))?;
            t.commit()?;
            Ok(new_inode)
        }

        fn remove_inode(&self, ino: Ino) -> anyhow::Result<()> {
            todo!()
        }

        fn read_attr(&self, ino: Ino) -> anyhow::Result<fuser::FileAttr> {
            let attrs: Vec<ByteVec> = self.persy.get(names::ATTRS, &ino)?.collect();
            anyhow::ensure!(attrs.len() == 1);
            let attr = bco().deserialize(&attrs[0])?;
            Ok(attr)
        }

        fn write_attr(&self, ino: Ino, data: fuser::FileAttr) -> anyhow::Result<()> {
            let mut t = self.persy.begin()?;
            t.put(names::ATTRS, ino, ByteVec::new(bco().serialize(&data)?))?;
            t.commit()?;
            Ok(())
        }

        fn readdir(
            &self,
            ino: Ino,
        ) -> anyhow::Result<Vec<(crate::api::Filename, crate::api::DirEnt)>> {
            let index_name = format!("{}", ino);
            let iter: IndexIter<ByteVec, ByteVec> = self.persy.range(&index_name, ..)?;
            let mut ret = Vec::with_capacity(4);
            for (fnam, values) in iter {
                let ft: Vec<ByteVec> = values.collect();
                anyhow::ensure!(ft.len() == 1);
                ret.push((fnam.to_vec(), bco().deserialize(&ft[0])?))
            }
            Ok(ret)
        }

        fn block_size(&self) -> usize {
            self.block_size as usize
        }

        fn write_block(
            &self,
            ino: Ino,
            block_n: u64,
            data: crate::api::Bytes,
        ) -> anyhow::Result<()> {
            todo!()
        }

        fn read_block(&self, ino: Ino, block_n: u64) -> anyhow::Result<Vec<u8>> {
            todo!()
        }

        fn modify_block(
            &self,
            ino: Ino,
            block_n: u64,
            offset_within_block: usize,
            new_data: &[u8],
        ) -> anyhow::Result<()> {
            todo!()
        }

        fn shrink_file(&self, ino: Ino, new_number_of_blocks: u64) -> anyhow::Result<()> {
            todo!()
        }

        fn read_symlink(&self, ino: Ino) -> anyhow::Result<crate::api::Bytes> {
            todo!()
        }

        fn write_symlink(&self, ino: Ino, content: crate::api::Bytes) -> anyhow::Result<()> {
            todo!()
        }

        fn lookup(
            &self,
            root: Ino,
            path: Vec<crate::api::Filename>,
        ) -> anyhow::Result<Option<Ino>> {
            todo!()
        }

        fn unlink(&self, dir: Ino, name: crate::api::Filename) -> anyhow::Result<()> {
            todo!()
        }

        fn rename(
            &self,
            olddir: Ino,
            oldname: crate::api::Filename,
            newdir: Ino,
            newname: crate::api::Filename,
        ) -> anyhow::Result<()> {
            todo!()
        }
    }
}*/

mod filesystem;
mod memory_storage;

fn main() -> anyhow::Result<()> {
    /*
    Persy::create("./data.persy")?;
    let persy = Persy::open("./data.persy", Config::new())?;
    let mut tx = persy.begin()?;
    let segid = tx.create_segment("seg")?;
    let data = vec![1; 20];
    tx.insert(segid, &data)?;
    let data2 = vec![2; 20];
    tx.insert(segid, &data2)?;
    let prepared = tx.prepare()?;
    prepared.commit()?;
    for (_id, content) in persy.scan("seg")? {
        dbg!(content);
        //assert_eq!(content[0], 1);
        //....
    }
     */

    env_logger::init();
    let mys = memory_storage::create(4096, fuser::FileType::Directory);
    let myfs = filesystem::new(mys);
    fuser::mount2(
        myfs,
        "m",
        &[
            MountOption::AutoUnmount,
            MountOption::AllowOther,
            MountOption::DefaultPermissions,
            MountOption::FSName("persyfs".to_owned()),
        ],
    )?;
    Ok(())
}
