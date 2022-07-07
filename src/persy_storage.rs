#![allow(unused)]
use std::time::{Duration, SystemTime};

use bincode::Options;
use log::{debug, error, info};
use persy::{ByteVec, IndexIter, Persy, PersyId, IndexOpsError};

use crate::api::{Bytes, FileAttr, FileType, Ino, Error, Result, dummy_fileattr, DirentContent, DirentLocation};

mod names {
    /// File attributes except of `nlinks` and `size` (and `blocks`)
    pub const ATTRS: &str = "attrs";
    /// Number of the next inode to be allocated. Only ever gets incremented.
    pub const INODE_COUNTER: &str = "inode_counter";
    /// Contents of symlinks
    pub const SYMLINKS: &str = "symlinks";
    /// Directory entries that refer this file, for counting `nlinks`.
    pub const BACKLINKS: &str = "_backlinks";
    /// Segment postfix for files.
    pub const DATA_SEGMENT: &str = "_data";
    /// Index from block number to PersyId of the data segment
    pub const BLOCKS_INDEX: &str = "_blocks";
    /// Directory content
    pub const DIR_INDEX: &str = "_dir";
    /// Index from Ino to u64.
    pub const FILESIZE: &str = "filesize";
}

struct PersyLayer {
    persy: Persy,
    new_entries_block_size: u32,
}

fn bco() -> impl bincode::Options {
    bincode::DefaultOptions::new()
        .with_limit(1024)
        .with_big_endian()
        .reject_trailing_bytes()
        .with_fixint_encoding()
}

type Dummy = u8;
const DUMMY_VALUE : Dummy = 0;

pub fn open(
    path: &std::path::Path,
    block_size: u32,
    root_node_kind: FileType,
) -> anyhow::Result<impl crate::api::PersistanceLayer> {
    let mut config = persy::Config::new();
    config.change_cache_age_limit(Duration::from_secs(60));
    config.change_transaction_lock_timeout(Duration::from_secs(10));
    config.change_tx_strategy(persy::TxStrategy::LastWin);
    let recover_count = std::sync::atomic::AtomicU32::new(0);
    if !path.exists() {
        info!("Creating new Persy database");
        Persy::create(path)?;
    }
    debug!("Opening database");
    let persy = Persy::open_with_recover(path, config, |_| {
        recover_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        true
    })?;
    let recover_count = recover_count.load(std::sync::atomic::Ordering::Relaxed);
    if recover_count > 0 {
        log::warn!("Recovered {} transactions", recover_count);
    }

    if !persy.exists_index(names::ATTRS)? {
        let mut t = persy.begin()?;
        t.create_index::<u64, ByteVec>(names::ATTRS, persy::ValueMode::Replace)?;
        t.commit()?;
    }
    if !persy.exists_index(names::FILESIZE)? {
        let mut t = persy.begin()?;
        t.create_index::<u64, u64>(names::FILESIZE, persy::ValueMode::Replace)?;
        t.commit()?;
    }
    if !persy.exists_index(names::SYMLINKS)? {
        let mut t = persy.begin()?;
        t.create_index::<u64, ByteVec>(names::SYMLINKS, persy::ValueMode::Replace)?;
        t.commit()?;
    }
    if !persy.exists_index(names::INODE_COUNTER)? {
        let mut t = persy.begin()?;
        t.create_index::<Dummy, u64>(names::INODE_COUNTER, persy::ValueMode::Replace)?;
        t.put::<Dummy,u64>(names::INODE_COUNTER, DUMMY_VALUE, 2)?;
        t.commit()?;
    }
    if persy.get::<Dummy,u64>(names::INODE_COUNTER, &DUMMY_VALUE)?.next().is_none() {
        let mut t = persy.begin()?;
        // it would automatically skip over allocated nodes
        t.put::<Dummy,u64>(names::INODE_COUNTER, DUMMY_VALUE, 2)?;
        t.commit()?;
    }
    let mut root_node_iter = persy.get::<u64,ByteVec>(names::ATTRS, &1)?;
    if root_node_iter.next().is_none() {
        drop(root_node_iter);
        info!("Creating root inode");
        let mut t = persy.begin()?;
        let mut attr = dummy_fileattr();
        attr.ino = 1;
        attr.nlink = 1;
        attr.kind = root_node_kind;
        t.put::<u64, ByteVec>(names::ATTRS, 1, ByteVec::from(bco().serialize(&attr)?))?;
        t.commit()?;
    }

    info!("Opened database");
    Ok(PersyLayer { persy, new_entries_block_size: block_size })
}

impl<T: Into<persy::PersyError>> From<persy::PE<T>> for crate::api::Error {
    fn from(e: persy::PE<T>) -> Self {
        anyhow::Error::from(e.persy_error()).into()
    }
}

impl From<bincode::Error> for crate::api::Error {
    fn from(e: bincode::Error) -> Self {
        anyhow::Error::from(e).into()
    }
}

trait IterExt<T> {
    fn get_the_only_element(self) -> crate::api::Result<T>;
    fn get_at_most_one_element(self) -> crate::api::Result<Option<T>>;
}
impl<I, T> IterExt<T> for I where I : Iterator<Item=T> {
    fn get_the_only_element(mut self) -> crate::api::Result<T> {
        match self.next() {
            Some(x) => match self.next() {
                Some(_) => Err(Error::Whatever(anyhow::anyhow!("Got inconsistent element from Persy index"))),
                None => Ok(x),
            }
            None => Err(Error::Whatever(anyhow::anyhow!("Failed to find expected element in a Persy index"))),
        }
    }
    fn get_at_most_one_element(mut self) -> crate::api::Result<Option<T>> {
        match self.next() {
            Some(x) => match self.next() {
                Some(_) => Err(Error::Whatever(anyhow::anyhow!("Got inconsistent element from Persy index"))),
                None => Ok(Some(x)),
            }
            None => Ok(None),
        }
    }
}

impl crate::api::PersistanceLayer for PersyLayer {
    fn new_inode(&self) -> Result<u64> {
        debug!("new_inode");
        let mut t = self.persy.begin()?;
        let new_inode: Option<Ino> = t.get::<Dummy,u64>(names::INODE_COUNTER, &DUMMY_VALUE)?.max();
        if new_inode.is_none() {
            return Err(anyhow::anyhow!("Cannot get inode counter").into());
        }
        let mut new_inode = new_inode.unwrap();
        loop {
            if t.get::<u64, ByteVec>(names::ATTRS, &new_inode)?.len() == 0 {
                break;
            }
            log::warn!(
                "{} index seems inconsistent with the real inodes table",
                names::INODE_COUNTER
            );
            new_inode += 1000;
        }
        let mut fa = dummy_fileattr();
        fa.ino = new_inode;
        fa.blksize = self.new_entries_block_size;
        let fa_bin = bco().serialize(&fa)?;
        t.put::<u64,ByteVec>(names::ATTRS, new_inode, ByteVec::new(fa_bin))?;
        t.put::<Dummy,u64>(names::INODE_COUNTER, DUMMY_VALUE, new_inode+1)?;
        debug!("new_inode commit");
        t.commit()?;
        Ok(new_inode) 
    }

    fn maybe_remove_inode(&self, ino: Ino) -> Result<bool> {
        let str_backlinks = format!("{}{}", ino, names::BACKLINKS);
        let mut t = self.persy.begin()?;
        let mut backlinks = t.range::<ByteVec, Dummy, _>(&str_backlinks, ..)?;
        if backlinks.next().is_some() {
            debug!("Not removing ino {} yet", ino);
            return Ok(false)
        }
        drop(backlinks);
        debug!("Removing ino {}", ino);

        t.remove::<u64, ByteVec>(names::ATTRS, ino, None)?;
        // remove directory content
        let _ = t.drop_index(&format!("{}{}", ino, names::DIR_INDEX));
        // remove backlinks
        let _ = t.drop_index(&str_backlinks);
        // remove symlink
        let _ = t.remove::<u64, ByteVec>(names::SYMLINKS, ino, None);
        // remove regular file content
        let _ = t.drop_segment(&format!("{}{}", ino, names::DATA_SEGMENT));
        // remove regular file block index
        let _ = t.drop_index(&format!("{}{}", ino, names::BLOCKS_INDEX));
        // remove filesize
        let _ = t.remove::<u64, u64>(names::FILESIZE, ino, None);

        debug!("remove commit");
        t.commit()?;
        Ok(true)
    }

    fn read_attr(&self, ino: Ino) -> Result<FileAttr> {
        debug!("read_addr ino={}", ino);
        let backlinks_name =format!("{}{}", ino, names::BACKLINKS);
        let block_index_name = format!("{}{}", ino, names::BLOCKS_INDEX);
        let s = self.persy.snapshot()?;

        let attr: ByteVec = s.get::<u64, ByteVec>(names::ATTRS, &ino)?.get_the_only_element()?;
        let mut attr : FileAttr = bco().deserialize(&attr)?;

        attr.nlink = 0;
        if self.persy.exists_index(&backlinks_name)? {
            let mut backlinks = s.range::<ByteVec, Dummy, _>(&backlinks_name, ..)?;
            attr.nlink = backlinks.count() as u32;
        }

        if attr.kind == FileType::RegularFile  {
            if let Some(filesize) = s.get::<u64, u64>(names::FILESIZE, &ino)?.get_at_most_one_element()? {
                attr.size = filesize;
            }
        }
       
        attr.blocks = 0;

        if self.persy.exists_index(&block_index_name)? {
            let data_segments = s.range::<u64, PersyId, _>(&block_index_name, ..)?;
            attr.blocks = data_segments.count() as u64;
        }

        Ok(attr)
    }

    fn read_block_size(&self, ino: Ino) -> Result<u32> {
        let attr = self.persy.get::<u64, ByteVec>(names::ATTRS, &ino)?.get_the_only_element()?;
        let mut attr : FileAttr = bco().deserialize(&attr)?;
        Ok(attr.blksize)
    }

    fn write_attr(&self, attr: FileAttr) -> Result<()> {
        debug!("write_attr ino={}", attr.ino);
        let ino = attr.ino;
        let mut t = self.persy.begin()?;

        let mut attr_bin : Vec<u8> = bco().serialize(&attr)?;

        t.put::<u64, ByteVec>(names::ATTRS, ino, ByteVec::from(attr_bin))?;
        if attr.kind == FileType::RegularFile  {
            debug!("Regular file mode");
            let old_size = t.get::<u64,u64>(names::FILESIZE, &ino)?.get_at_most_one_element()?.unwrap_or(0);
            t.put::<u64, u64>(names::FILESIZE, ino, attr.size)?;
            debug!("Old size {}, new size {}", old_size, attr.size);

            let new_number_of_blocks =
            (attr.size + (attr.blksize as u64) - 1) / attr.blksize as u64;

            let segment = format!("{}{}", ino, names::DATA_SEGMENT);
            let block_index = format!("{}{}", ino, names::BLOCKS_INDEX);
            if attr.size < old_size && t.exists_index(&block_index)? && t.exists_segment(&segment)? {
                let block_ids_to_remove : Vec<_> = t.range::<u64, PersyId, _>(&block_index, new_number_of_blocks..)?.collect();

                for bid in block_ids_to_remove {
                    debug!("Removing block");
                    let _ = t.delete(&segment, &bid.1.get_the_only_element()?);
                    let _ = t.remove::<u64,PersyId>(&block_index, bid.0, None);
                }
            }
        }

        debug!("write_attr commit");
        t.commit()?;
        Ok(())
    }

    fn readdir(&self, ino: Ino) -> Result<Vec<(crate::api::Filename, crate::api::DirentContent)>> {
        debug!("readdir ino={}", ino);
        let s = self.persy.snapshot()?;

        let dirs = match s.range::<ByteVec, ByteVec, _>(&format!("{}{}", ino, names::DIR_INDEX), ..) {
            Ok(x) => x,
            Err(e) => match e.error() {
                IndexOpsError::IndexNotFound  => return Ok(vec![]),
                ee => return Err(Into::<persy::PE<IndexOpsError>>::into(ee).into()),
            }
        };
        let mut ret = Vec::with_capacity(4);

        for dir in dirs {
            let dc : DirentContent = bco().deserialize(&dir.1.get_the_only_element()?)?;
            ret.push((dir.0.to_vec(), dc));
        }

        Ok(ret)
    }

    fn read_block_and_filelen(&self, ino: Ino, block_n: crate::api::Blocknumber) -> Result<(Option<Bytes>, u64)> {
        let s = self.persy.snapshot()?;

        let fsz = s.get::<u64, u64>(names::FILESIZE, &ino)?.get_the_only_element()?;

        let data_segment_name = format!("{}{}", ino, names::DATA_SEGMENT);
        let blocks_index_name = format!("{}{}", ino, names::BLOCKS_INDEX);

        let bi = s.get::<u64,PersyId>(&blocks_index_name, &block_n);
        let bi = match bi {
            Ok(x) => Ok(x),
            Err(e) => Err(match e {
                persy::PE::PE(IndexOpsError::IndexNotFound) => return Ok((None, fsz)),
                x => x,
            })
        };
        let mut bi = bi?;
        match bi.next() {
            None => {
                Ok((None, fsz))
            },
            Some(x) => {
                match bi.next() {
                    None => {
                        Ok((s.read(&data_segment_name, &x)?, fsz))
                    }
                    Some(x) => return Err(anyhow::anyhow!("Inconsistent block").into()),
                }
            }
        }
    }

    fn write_or_modify_block_and_maybe_filelen(
        &self,
        ino: Ino,
        block_n: u64,
        offset_within_block: usize,
        new_data: &[u8],
        len_cand: u64,
    ) -> Result<()> {
        let mut t = self.persy.begin()?;

        let data_segment_name = format!("{}{}", ino, names::DATA_SEGMENT);
        let blocks_index_name = format!("{}{}", ino, names::BLOCKS_INDEX);

        let _ = t.create_index::<u64,PersyId>(&blocks_index_name, persy::ValueMode::Replace);
        let _ = t.create_segment(&data_segment_name);
        
        let fsz = t.get::<u64, u64>(names::FILESIZE, &ino)?.get_the_only_element()?;
        if fsz < len_cand {
            t.put::<u64, u64>(names::FILESIZE, ino, len_cand)?;
        }

        let mut bi = t.get::<u64,PersyId>(&blocks_index_name, &block_n)?;
        let block_id_to_patch = match bi.next() {
            None => {
                let bid = if offset_within_block == 0 {
                    t.insert(&data_segment_name, new_data)?
                } else {
                    let mut buf = vec![0u8; new_data.len() + offset_within_block];
                    buf[offset_within_block..].copy_from_slice(new_data);
                    t.insert(&data_segment_name, &buf)?
                };
                t.put::<u64,PersyId>(&blocks_index_name, block_n, bid);
                None
            },
            Some(x) => {
                match bi.next() {
                    None => {
                        Some(x)
                    }
                    Some(x) => return Err(anyhow::anyhow!("Inconsistent block").into()),
                }
            }
        };

        if let Some(block_id) = block_id_to_patch {
            let mut block = match t.read(&data_segment_name, &block_id)? {
                Some(x) => x,
                None => {
                    error!("Zeroing out missing block {} in file {}", block_n, ino);
                    vec![]
                }                    
            };

            let required_block_len = new_data.len() + offset_within_block;

            if block.len() < required_block_len {
                block.resize(required_block_len, 0);
            }
            block[offset_within_block..(offset_within_block + new_data.len())].copy_from_slice(new_data);

            t.update(&data_segment_name, &block_id, &block)?;
        }

        t.commit()?;
        Ok(())
    }

    fn read_symlink(&self, ino: Ino) -> Result<Bytes> {
        debug!("read_symlink ino={:?}", ino);
        Ok(self.persy.get::<u64, ByteVec>(names::SYMLINKS, &ino)?.get_the_only_element()?.into())
    }

    fn write_symlink(&self, ino: Ino, content: Bytes) -> Result<()> {
        debug!("write_symlink ino={:?}", ino);
        let mut t = self.persy.begin()?;
        t.put::<u64, ByteVec>(names::SYMLINKS, ino, ByteVec::from(content))?;
        t.commit()?;
        debug!("write_symlink commit");
        Ok(())
    }

    fn link_unlink(
        &self,
        old: either::Either<crate::api::DirentLocation, crate::api::DirentContent>,
        new: Option<crate::api::DirentLocation>,
        allow_replace: bool,
    ) -> Result<Option<Ino>> {
        debug!("link_unlink old={:?} new={:?}", old, new);
        let mut t = self.persy.begin()?;

        let mut evicted_ino = None;
 
        let entry : DirentContent = match old {
            either::Either::Left(del) => {
                let del_bytes = ByteVec::from(bco().serialize(&del)?);
                let dir_index_name = format!("{}{}", del.dir_ino, names::DIR_INDEX);
                let filename = ByteVec::from(del.filename);

                let mut check = t.get::<ByteVec,ByteVec>(&dir_index_name, &filename)?;
                match check.next() {
                    Some(entry_buf) => {
                        let entry : DirentContent = bco().deserialize(&entry_buf)?;
                        
                        let backlinks_name = format!("{}{}", entry.ino, names::BACKLINKS);
                        t.remove::<ByteVec, Dummy>(&backlinks_name, del_bytes, None)?;
                        t.remove::<ByteVec,ByteVec>(&dir_index_name, filename, None)?;

                        if new.is_none() {
                            // unlink mode
                            evicted_ino = Some(entry.ino);
                        }

                        entry
                    }
                    None => return Err(Error::DiretryNotFound(DirentLocation{dir_ino:del.dir_ino, filename:filename.to_vec()})),
                }
            }
            either::Either::Right(x) => x,
        };

        if let Some(new_name) = new {
            let dir_index_name = format!("{}{}", new_name.dir_ino, names::DIR_INDEX);
            let _ = t.create_index::<ByteVec,ByteVec>(&dir_index_name, persy::ValueMode::Replace);
            let new_name_bytes = bco().serialize(&new_name)?;
            let filename = ByteVec::from(new_name.filename);

            let mut check = t.get::<ByteVec,ByteVec>(&dir_index_name, &filename)?;
            if let Some(unfortunate_entry) = check.next() {
                if !allow_replace {
                    return Err(Error::AlreadyExists(DirentLocation{dir_ino:new_name.dir_ino, filename:filename.to_vec()}));
                }
                t.remove::<ByteVec,ByteVec>(&dir_index_name, filename.clone(), None)?;
                let unfortunate_entry : DirentContent = bco().deserialize(&unfortunate_entry)?;
                let backlinks_name = format!("{}{}", unfortunate_entry.ino, names::BACKLINKS);
                t.remove::<ByteVec, Dummy>(&backlinks_name, ByteVec::from(new_name_bytes.clone()), None)?;
                assert!(evicted_ino.is_none());
                evicted_ino = Some(unfortunate_entry.ino);
            }

            t.put::<ByteVec,ByteVec>(&dir_index_name, filename, ByteVec::from(bco().serialize(&entry)?))?;
            let backlinks_name = format!("{}{}", entry.ino, names::BACKLINKS);
            if ! t.exists_index(&backlinks_name)? {
                t.create_index::<ByteVec, Dummy>(&backlinks_name, persy::ValueMode::Replace)?;
            }
            t.put::<ByteVec, Dummy>(&backlinks_name, ByteVec::from(new_name_bytes), DUMMY_VALUE)?;
        }

        debug!("link_unlink commit");
        t.commit()?;
        Ok(evicted_ino)
    }

    fn lookup(&self, root: Ino, filename: crate::api::Filename) -> Result<Option<Ino>> {
        debug!("lookup root={:?} filename=`{}`", root, String::from_utf8_lossy(&filename));
        let dir_index_name = format!("{}{}", root, names::DIR_INDEX);
        let ret = self.persy.get::<ByteVec,ByteVec>(&dir_index_name, &ByteVec::from(filename));
        let mut iter = match ret {
            Ok(x) => Ok(x),
            Err(e) => {
                Err(match e {
                    persy::PE::PE(IndexOpsError::IndexNotFound) => return Ok(None),
                    x => x,
                })
            }
        }?;
        match iter.next() {
            Some(x) => {
                if iter.next().is_some() {
                    return Err(anyhow::anyhow!("Inconsistent dir entry").into());
                }
                let de : DirentContent = bco().deserialize(&x)?;
                Ok(Some(de.ino))
            }
            None => Ok(None),
        }
    }
}
