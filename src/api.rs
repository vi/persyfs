use std::time::SystemTime;

use serde::{Deserialize, Serialize};

pub extern crate either;
pub type Filename = Vec<u8>;
pub type Bytes = Vec<u8>;
pub type Ino = u64;
pub type Blocknumber = u64;
pub use fuser::{FileAttr, FileType};

#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
pub struct DirentContent {
    pub ino: u64,
    pub kind: FileType,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DirentLocation {
    pub dir_ino: Ino,
    pub filename: Filename,
}

impl std::fmt::Display for DirentLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<{}>/`{}`", self.dir_ino, String::from_utf8_lossy(&self.filename))
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("error: {0}")]
    Whatever(#[from] anyhow::Error),
    #[error("inode {0} not found")]
    InodeNotFound(Ino),
    #[error("directory entry `{0}` not found")]
    DiretryNotFound(DirentLocation),
    #[error("directory entry `{0}` already exists")]
    AlreadyExists(DirentLocation),
}
pub type Result<T> = std::result::Result<T, Error>;

pub trait PersistanceLayer {
    /// inode numbers are not reused. Removed files's inode mumber is lost forever.
    /// Should allocate a `FileAttr` for this inode number.
    fn new_inode(&self) -> Result<u64>;
    /// Remove inode if its `FileAttr::nlink` is zero (or not incoming references) and return `true`, return `false` otherwise.
    fn maybe_remove_inode(&self, ino: Ino) -> Result<bool>;
    /// `FileAttr::nlink` may be calculated, not actually stored.
    fn read_attr(&self, ino: Ino) -> Result<FileAttr>;
    /// `FileAttr::nlink` should be ignored and managed by implementor. 
    /// `FileAttr::size` should not be ignored, it should be recorded. Orphaned data blocks should be removed automatically by implementor.
    fn write_attr(&self, ino: Ino, data: FileAttr) -> Result<()>;
    fn readdir(&self, ino: Ino) -> Result<Vec<(Filename, DirentContent)>>;
    /// Write block at this block slot.
    /// Implementor should also update file size in FileAttr
    fn write_block(&self, ino: Ino, block_n: Blocknumber, data: Bytes) -> Result<()>;
    /// Read specified block from file. Less than `FileAttr::blksize` bytes may be returned.
    /// They are assumed zero if it is not the end of file. `None` means a block full of zeros (sparse region)
    /// Additionally it should return total file size at the moment of reading.
    fn read_block_and_filelen(&self, ino: Ino, block_n: Blocknumber) -> Result<(Option<Bytes>, u64)>;
    fn modify_block(
        &self,
        ino: Ino,
        block_n: u64,
        offset_within_block: usize,
        new_data: &[u8],
    ) -> Result<()>;
    fn read_symlink(&self, ino: Ino) -> Result<Bytes>;
    fn write_symlink(&self, ino: Ino, content: Bytes) -> Result<()>;
   
    /// Link, unlink or rename a file.
    /// 
    /// | old      | new      | op    |
    /// | ---      | ---      | ---   |
    /// | content  | location | link  |
    /// | location | location | rename|
    /// | location | none     | unlink|
    /// | content  | none     | noop  |
    /// 
    /// Should update `FileAttr::nlink` accordingly, also report the inode number if its `nlink` has reached zero.
    /// Should not automatically remove the inode even if number if links get zero.
    /// Care should be taken by implementer with regard to simultaneaus calls to `link_unlink`s from multiple threads.
    /// 
    /// Remember to handle the case where new_location == old_location.
    fn link_unlink(
        &self,
        old: either::Either<DirentLocation, DirentContent>,
        new: Option<DirentLocation>,
        allow_replace: bool,
    ) -> Result<Option<Ino>>;

    fn lookup(&self, root: Ino, filename: Filename) -> Result<Option<Ino>>;
}

pub fn dummy_fileattr() -> FileAttr {
    FileAttr {
        ino: 0,
        size: 0,
        blocks: 0,
        atime: SystemTime::now(),
        mtime: SystemTime::now(),
        ctime: SystemTime::now(),
        crtime: SystemTime::now(),
        kind: FileType::CharDevice,
        perm: 0777,
        nlink: 0,
        uid: 0,
        gid: 0,
        rdev: 0,
        blksize: 0,
        flags: 0,
    }
}
