use std::{
    borrow::BorrowMut,
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::api::{dummy_fileattr, DirentLocation, Error, Result};
use fuser::{FileAttr, FileType};
use log::debug;

use crate::api::{Blocknumber, Bytes, DirentContent, Filename, Ino, PersistanceLayer};

#[derive(Default)]
struct MemoryStorage {
    new_entries_block_size: u32,
    inode_counter: u64,
    attrs: HashMap<Ino, FileAttr>,
    dirents: HashMap<Ino, HashMap<Filename, DirentContent>>,
    files_content: HashMap<Ino, HashMap<Blocknumber, Bytes>>,
    symlinks: HashMap<Ino, Bytes>,
}

pub fn create(block_size: u32, root_type: FileType) -> impl crate::api::PersistanceLayer {
    let mut ret = MemoryStorage::default();
    ret.inode_counter = 1;
    ret.new_entries_block_size = block_size;
    let ret = Arc::new(Mutex::new(ret));
    let rootino = ret.new_inode().unwrap();
    assert_eq!(rootino, 1);
    let mut l = ret.lock().unwrap();
    let rootnode = l.attrs.get_mut(&1).unwrap();
    rootnode.ino = 1;
    rootnode.blksize = block_size;
    rootnode.kind = root_type;
    rootnode.nlink = 1;
    rootnode.perm = 0o777;
    drop(l);

    ret
}

impl PersistanceLayer for Arc<Mutex<MemoryStorage>> {
    fn new_inode(&self) -> Result<u64> {
        let mut l = self.lock().unwrap();
        let ret = l.inode_counter;
        l.inode_counter += 1;
        let mut attr = dummy_fileattr();
        attr.ino = ret;
        attr.blksize = l.new_entries_block_size;
        l.attrs.insert(ret, attr);
        Ok(ret)
    }

    fn maybe_remove_inode(&self, ino: crate::api::Ino) -> Result<bool> {
        let mut l = self.lock().unwrap();
        if let Some(a) = l.attrs.get(&ino) {
            log::debug!("Maybe remove inode {} with nlinks {}", ino, a.nlink);
            if a.nlink != 0 {
                return Ok(false);
            }
        } else {
            return Err(Error::InodeNotFound(ino));
        }
        l.attrs.remove(&ino);
        l.dirents.remove(&ino);
        l.files_content.remove(&ino);
        l.symlinks.remove(&ino);
        Ok(true)
    }

    fn read_attr(&self, ino: crate::api::Ino) -> Result<fuser::FileAttr> {
        if let Some(attr) = self.lock().unwrap().attrs.get(&ino) {
            Ok(attr.clone())
        } else {
            Err(Error::InodeNotFound(ino))
        }
    }

    fn read_block_size(&self, ino: Ino) -> Result<u32> {
        if let Some(attr) = self.lock().unwrap().attrs.get(&ino) {
            Ok(attr.blksize)
        } else {
            Err(Error::InodeNotFound(ino))
        }
    }

    fn write_attr(&self, attr: fuser::FileAttr) -> Result<()> {
        let mut l = self.lock().unwrap();
        let l = &mut (*l);
        if let Some(attr_) = l.attrs.get_mut(&attr.ino) {
            let nlinks = attr_.nlink;
            let oldfilesize = attr_.size;
            *attr_ = attr;
            attr_.nlink = nlinks;

            if attr_.size < oldfilesize {
                let new_number_of_blocks =
                    (attr_.size + (attr_.blksize as u64) - 1) / attr_.blksize as u64;

                if let Some(c) = l.files_content.get_mut(&attr.ino) {
                    c.retain(|block_n, _| block_n < &new_number_of_blocks);
                }
            }

            Ok(())
        } else {
            Err(Error::InodeNotFound(attr.ino))
        }
    }

    fn readdir(
        &self,
        ino: crate::api::Ino,
    ) -> Result<Vec<(crate::api::Filename, crate::api::DirentContent)>> {
        let mut l = self.lock().unwrap();
        Ok(l.dirents
            .entry(ino)
            .or_default()
            .iter()
            .map(|(a, b)| (a.clone(), *b))
            .collect())
    }

    fn read_block_and_filelen(
        &self,
        ino: crate::api::Ino,
        block_n: u64,
    ) -> Result<(Option<Bytes>, u64)> {
        let mut l = self.lock().unwrap();
        let l = (*l).borrow_mut();
        let content = l.files_content.entry(ino).or_default();
        let attr = l.attrs.get(&ino).ok_or(Error::InodeNotFound(ino))?;
        let maybe_block = content.get(&block_n).cloned();
        Ok((maybe_block, attr.size))
    }

    fn write_or_modify_block_and_maybe_filelen(
        &self,
        ino: crate::api::Ino,
        block_n: u64,
        offset_within_block: usize,
        new_data: &[u8],
        file_length_candidate: u64,
    ) -> Result<()> {
        let mut l = self.lock().unwrap();
        let l = (*l).borrow_mut();
        let attr = l.attrs.get_mut(&ino).ok_or(Error::InodeNotFound(ino))?;
        let len = new_data.len();
        let required_block_len = len + offset_within_block;
        assert!(offset_within_block <= attr.blksize as usize);
        assert!(len <= attr.blksize as usize);
        assert!(required_block_len <= attr.blksize as usize);
        let c = l
            .files_content
            .entry(ino)
            .or_insert_with(|| Default::default());
        let b = c
            .entry(block_n)
            .or_insert_with(|| vec![0u8; offset_within_block + new_data.len()]);
        if b.len() < required_block_len {
            b.resize(required_block_len, 0);
        }
        b[offset_within_block..(offset_within_block + new_data.len())].copy_from_slice(new_data);
        let file_size_cand = block_n * attr.blksize as u64 + len as u64 + offset_within_block as u64;
        assert_eq!(file_size_cand, file_length_candidate);
        attr.size = attr
            .size
            .max(file_length_candidate);
        attr.blocks = attr.blocks.max(block_n + 1);
        Ok(())
    }

    fn read_symlink(&self, ino: crate::api::Ino) -> Result<Bytes> {
        let l = self.lock().unwrap();
        Ok(l.symlinks.get(&ino).ok_or(Error::InodeNotFound(ino))?).cloned()
    }

    fn write_symlink(&self, ino: crate::api::Ino, content: Bytes) -> Result<()> {
        let mut l = self.lock().unwrap();
        l.symlinks.insert(ino, content);
        Ok(())
    }

    fn link_unlink(
        &self,
        old: either::Either<crate::api::DirentLocation, DirentContent>,
        new: Option<crate::api::DirentLocation>,
        allow_replace: bool,
    ) -> Result<Option<Ino>> {
        let mut l = self.lock().unwrap();

        let mut ret = None;

        let pending_unlink: Option<DirentLocation>;
        let mut decrement_nlinks = false;
        let mut increment_nlinks = false;
        let entry: DirentContent = match old {
            either::Either::Left(old_location) => {
                if Some(&old_location) == new.as_ref() {
                    // no-op
                    return Ok(None);
                }

                let a = l
                    .dirents
                    .get(&old_location.dir_ino)
                    .ok_or(Error::InodeNotFound(old_location.dir_ino))?
                    .get(&old_location.filename)
                    .ok_or_else(|| Error::DiretryNotFound(old_location.clone()))?
                    .clone();
                pending_unlink = Some(old_location);
                a
            }
            either::Either::Right(c) => {
                if new.is_none() {
                    // no-op
                    return Ok(None);
                }
                pending_unlink = None;
                increment_nlinks = true;
                c
            }
        };

        let evicted_entry_to_be_unlinked: Option<DirentContent> = if let Some(new_location) = new {
            let d = l.dirents.entry(new_location.dir_ino).or_default();
            if allow_replace {
                d.insert(new_location.filename, entry)
            } else {
                if d.contains_key(&new_location.filename) {
                    return Err(Error::AlreadyExists(new_location));
                } else {
                    d.insert(new_location.filename, entry);
                }
                None
            }
        } else {
            decrement_nlinks = true;
            None
        };

        debug!(
            "evict? {}, increment? {}, decrement? {}",
            evicted_entry_to_be_unlinked.is_some(),
            increment_nlinks,
            decrement_nlinks
        );
        assert!(evicted_entry_to_be_unlinked.is_none() || !decrement_nlinks);

        if let Some(oe) = evicted_entry_to_be_unlinked {
            if let Some(oe) = l.attrs.get_mut(&oe.ino) {
                oe.nlink = oe.nlink.saturating_sub(1);
                if oe.nlink == 0 {
                    ret = Some(oe.ino);
                }
            }
        }

        if let Some(pu) = pending_unlink {
            if let Some(de) = l.dirents.get_mut(&pu.dir_ino) {
                assert!(de.remove(&pu.filename).is_some());
            }
            if decrement_nlinks {
                if let Some(e) = l.attrs.get_mut(&entry.ino) {
                    e.nlink = e.nlink.saturating_sub(1);
                    if e.nlink == 0 {
                        assert!(ret.is_none());
                        ret = Some(e.ino);
                    }
                }
            }
        }

        if increment_nlinks {
            if let Some(e) = l.attrs.get_mut(&entry.ino) {
                e.nlink += 1;
            }
        }

        Ok(ret)
    }

    fn lookup(
        &self,
        root: crate::api::Ino,
        filename: crate::api::Filename,
    ) -> Result<Option<crate::api::Ino>> {
        let l = self.lock().unwrap();

        let mut cursor = root;

        cursor = if let Some(y) = l
            .dirents
            .get(&cursor)
            .ok_or_else(|| {
                Error::DiretryNotFound(DirentLocation {
                    dir_ino: root,
                    filename: b"/".to_vec(),
                })
            })?
            .get(&filename)
        {
            y.ino
        } else {
            return Ok(None);
        };

        Ok(Some(cursor))
    }
}
