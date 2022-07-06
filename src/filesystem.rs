use std::{
    ffi::OsStr,
    os::unix::prelude::OsStrExt,
    sync::atomic::AtomicU32,
    time::{Duration, SystemTime},
};

use crate::api::{DirentContent, DirentLocation, Ino, PersistanceLayer};
use either::Either;
use fuser::{FileAttr, FileType};
use libc::{
    c_int, EEXIST, EINVAL, EIO, ENOENT, ENOSYS, ENOTDIR, ENOTEMPTY, EPERM, RENAME_NOREPLACE,
    S_IFLNK, S_IFMT, S_IFSOCK,
};
use log::{debug, error, warn};

const TTL: Duration = Duration::from_secs(60);

struct Filesystem<L: PersistanceLayer> {
    l: L,
    opened_files: dashmap::DashMap<Ino, std::sync::atomic::AtomicU32>,
}

pub fn new<L: PersistanceLayer>(l: L) -> impl fuser::Filesystem {
    Filesystem {
        opened_files: Default::default(),
        l,
    }
}

fn ee(e: crate::api::Error) -> c_int {
    match e {
        crate::api::Error::Whatever(ee) => {
            log::error!("Error: {}", ee);
            EIO
        }
        crate::api::Error::InodeNotFound(ino) => {
            log::warn!("Inode {} not found", ino);
            ENOENT
        }
        crate::api::Error::DiretryNotFound(ee) => {
            log::debug!("Dirent {} not found ", ee);
            ENOENT
        }
        crate::api::Error::AlreadyExists(ee) => {
            log::debug!("Direct {} already exists", ee);
            EEXIST
        }
    }
}

impl<L: PersistanceLayer> fuser::Filesystem for Filesystem<L> {
    fn lookup(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEntry,
    ) {
        match self.l.lookup(parent, name.as_bytes().to_vec()) {
            Ok(Some(x)) => match self.l.read_attr(x) {
                Ok(attr) => reply.entry(&TTL, &attr, 0),
                Err(e) => reply.error(ee(e)),
            },
            Ok(None) => {
                reply.error(ENOENT);
            }
            Err(e) => {
                reply.error(ee(e));
            }
        }
    }

    fn forget(&mut self, _req: &fuser::Request<'_>, _ino: u64, _nlookup: u64) {
        // inodes are not reused in this implementation, so no need to keep track of them
    }

    fn getattr(&mut self, _req: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyAttr) {
        match self.l.read_attr(ino) {
            Ok(attr) => reply.attr(&TTL, &attr),
            Err(e) => reply.error(ee(e)),
        }
    }

    fn setattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        ctime: Option<std::time::SystemTime>,
        _fh: Option<u64>,
        crtime: Option<std::time::SystemTime>,
        _chgtime: Option<std::time::SystemTime>,
        bkuptime: Option<std::time::SystemTime>,
        flags: Option<u32>,
        reply: fuser::ReplyAttr,
    ) {
        match self.l.read_attr(ino) {
            Ok(mut attr) => {
                if let Some(mode) = mode {
                    attr.perm = mode as u16;
                }
                if let Some(uid) = uid {
                    attr.uid = uid;
                }
                if let Some(gid) = gid {
                    attr.gid = gid;
                }
                if let Some(size) = size {
                    attr.size = size;
                }
                match _atime {
                    Some(fuser::TimeOrNow::Now) => attr.atime = SystemTime::now(),
                    Some(fuser::TimeOrNow::SpecificTime(t)) => attr.atime = t,
                    None => (),
                }
                match _mtime {
                    Some(fuser::TimeOrNow::Now) => attr.mtime = SystemTime::now(),
                    Some(fuser::TimeOrNow::SpecificTime(t)) => attr.mtime = t,
                    None => (),
                }
                if let Some(ctime) = ctime {
                    attr.ctime = ctime;
                }
                if let Some(crtime) = crtime {
                    attr.crtime = crtime;
                }
                if let Some(bkuptime) = bkuptime {
                    attr.crtime = bkuptime;
                }
                if let Some(flags) = flags {
                    attr.flags = flags;
                }
                match self.l.write_attr(ino, attr) {
                    Ok(()) => reply.attr(&TTL, &attr),
                    Err(e) => reply.error(ee(e)),
                }
            }
            Err(e) => reply.error(ee(e)),
        }
    }

    fn readlink(&mut self, _req: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyData) {
        match self.l.read_symlink(ino) {
            Ok(x) => reply.data(&x),
            Err(e) => reply.error(ee(e)),
        }
    }

    fn mknod(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        mode: u32,
        _umask: u32,
        rdev: u32,
        reply: fuser::ReplyEntry,
    ) {
        let code = move || -> crate::api::Result<_> {
            let mut attr: FileAttr;
            let n = self.l.new_inode()?;
            attr = self.l.read_attr(n)?;
            attr.kind = match (mode & S_IFMT) {
                libc::S_IFSOCK => FileType::Socket,
                libc::S_IFLNK => FileType::Symlink,
                libc::S_IFREG => FileType::RegularFile,
                libc::S_IFBLK => FileType::BlockDevice,
                libc::S_IFDIR => FileType::Directory,
                libc::S_IFCHR => FileType::CharDevice,
                libc::S_IFIFO => FileType::NamedPipe,
                _ => {
                    return Err(crate::api::Error::Whatever(anyhow::anyhow!(
                        "Strange kind of file in mknod"
                    )));
                }
            };
            attr.perm = mode as u16;
            // what to do with `_umask`?
            attr.rdev = rdev;
            attr.uid = req.uid();
            attr.gid = req.gid();
            self.l.write_attr(n, attr)?;

            match self.l.link_unlink(
                Either::Right(DirentContent {
                    ino: n,
                    kind: attr.kind,
                }),
                Some(DirentLocation {
                    dir_ino: parent,
                    filename: name.as_bytes().to_vec(),
                }),
                false,
            ) {
                Ok(Some(_)) => unreachable!(),
                Ok(None) => Ok(attr),
                Err(e) => {
                    self.l.maybe_remove_inode(n);
                    Err(e)
                }
            }
        };
        match code() {
            Ok(attr) => reply.entry(&TTL, &attr, 0),
            Err(e) => reply.error(ee(e)),
        }
    }

    fn mkdir(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        mode: u32,
        umask: u32,
        reply: fuser::ReplyEntry,
    ) {
        let code = move || {
            let mut attr: FileAttr;
            let n = self.l.new_inode()?;
            attr = self.l.read_attr(n)?;
            attr.kind = FileType::Directory;
            attr.perm = mode as u16;
            attr.uid = req.uid();
            attr.gid = req.gid();
            // what to do with `_umask`?
            self.l.write_attr(n, attr)?;

            match self.l.link_unlink(
                Either::Right(DirentContent {
                    ino: n,
                    kind: attr.kind,
                }),
                Some(DirentLocation {
                    dir_ino: parent,
                    filename: name.as_bytes().to_vec(),
                }),
                false,
            ) {
                Ok(Some(_)) => unreachable!(),
                Ok(None) => Ok(attr),
                Err(e) => {
                    self.l.maybe_remove_inode(n);
                    Err(e)
                }
            }
        };
        match code() {
            Ok(attr) => reply.entry(&TTL, &attr, 0),
            Err(e) => reply.error(ee(e)),
        }
    }

    fn unlink(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        match self.l.link_unlink(
            Either::Left(DirentLocation {
                dir_ino: parent,
                filename: name.as_bytes().to_vec(),
            }),
            None,
            false,
        ) {
            Ok(Some(x)) => {
                let _ = self.l.maybe_remove_inode(x);
                reply.ok()
            }
            Ok(None) => reply.ok(),
            Err(e) => reply.error(ee(e)),
        }
    }

    fn rmdir(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        let n = match self.l.lookup(parent, name.as_bytes().to_vec()) {
            Ok(Some(a)) => a,
            Ok(None) => return reply.error(ENOENT),
            Err(e) => return reply.error(ee(e)),
        };
        match self.l.readdir(n) {
            Ok(des) => {
                if !des.is_empty() {
                    return reply.error(ENOTEMPTY);
                }
            }
            Err(e) => return reply.error(ENOTDIR),
        }
        match self.l.link_unlink(
            Either::Left(DirentLocation {
                dir_ino: parent,
                filename: name.as_bytes().to_vec(),
            }),
            None,
            false,
        ) {
            Ok(Some(x)) => {
                assert_eq!(x, n);
                let _ = self.l.maybe_remove_inode(x);
                reply.ok()
            }
            Ok(None) => reply.ok(),
            Err(e) => reply.error(ee(e)),
        }
    }

    fn symlink(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        link: &std::path::Path,
        reply: fuser::ReplyEntry,
    ) {
        let code = move || {
            let mut attr: FileAttr;
            let n = self.l.new_inode()?;
            attr = self.l.read_attr(n)?;
            attr.kind = FileType::Symlink;
            attr.perm = 0o777;
            self.l.write_attr(n, attr)?;

            self.l
                .write_symlink(n, link.as_os_str().as_bytes().to_vec())?;

            match self.l.link_unlink(
                Either::Right(DirentContent {
                    ino: n,
                    kind: attr.kind,
                }),
                Some(DirentLocation {
                    dir_ino: parent,
                    filename: name.as_bytes().to_vec(),
                }),
                false,
            ) {
                Ok(Some(_)) => unreachable!(),
                Ok(None) => Ok(attr),
                Err(e) => {
                    self.l.maybe_remove_inode(n);
                    Err(e)
                }
            }
        };
        match code() {
            Ok(attr) => reply.entry(&TTL, &attr, 0),
            Err(e) => reply.error(ee(e)),
        }
    }

    fn rename(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        newparent: u64,
        newname: &std::ffi::OsStr,
        flags: u32,
        reply: fuser::ReplyEmpty,
    ) {
        let allow_replace = match flags {
            0 => true,
            RENAME_NOREPLACE => false,
            x => {
                log::warn!("Rename flags {} not implemented", x);
                return reply.error(ENOSYS);
            }
        };
        match self.l.link_unlink(
            either::Left(DirentLocation {
                dir_ino: parent,
                filename: name.as_bytes().to_vec(),
            }),
            Some(DirentLocation {
                dir_ino: newparent,
                filename: newname.as_bytes().to_vec(),
            }),
            allow_replace,
        ) {
            Ok(None) => reply.ok(),
            Ok(Some(x)) => {
                if self
                    .opened_files
                    .entry(x)
                    .or_default()
                    .load(std::sync::atomic::Ordering::SeqCst)
                    == 0
                {
                    let _ = self.l.maybe_remove_inode(x);
                }
                reply.ok();
            }
            Err(e) => reply.error(ee(e)),
        }
    }

    fn link(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        newparent: u64,
        newname: &std::ffi::OsStr,
        reply: fuser::ReplyEntry,
    ) {
        let code = move || {
            let mut attr: FileAttr;
            attr = self.l.read_attr(ino)?;

            let allow_replace = false;
            match self.l.link_unlink(
                either::Right(DirentContent {
                    ino,
                    kind: attr.kind,
                }),
                Some(DirentLocation {
                    dir_ino: newparent,
                    filename: newname.as_bytes().to_vec(),
                }),
                allow_replace,
            )? {
                None => (),
                Some(_) => unreachable!(),
            }

            Ok(attr)
        };
        match code() {
            Ok(attr) => reply.entry(&TTL, &attr, 0),
            Err(e) => reply.error(ee(e)),
        }
    }

    fn open(&mut self, _req: &fuser::Request<'_>, ino: u64, _flags: i32, reply: fuser::ReplyOpen) {
        let attr = match self.l.read_attr(ino) {
            Err(e) => return reply.error(ee(e)),
            Ok(x) => x,
        };
        self.opened_files
            .entry(ino)
            .or_insert(AtomicU32::new(0))
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        reply.opened(attr.blksize as u64, 0);
    }

    fn read(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        let blksize = fh;

        if offset < 0 {
            error!("Negative offset requested?");
            return reply.error(EINVAL);
        }
        let original_offset = offset;
        let mut offset = offset as u64;

        let mut buffer = vec![0u8; size as usize];
        let mut data = &mut buffer[..];

        let mut fsz = 0;
        loop {
            let (block_n, offset_within_block) = (offset / blksize, offset % blksize);
            match self.l.read_block_and_filelen(ino, block_n) {
                Err(e) => return reply.error(ee(e)),
                Ok((None, fsz_)) => {
                    fsz = fsz_;
                    // Leave that block zeroed
                }
                Ok((Some(block), fsz_)) => {
                    assert!(block.len() as u64 <= blksize);
                    fsz = fsz_;
                    if block.len() > offset_within_block as usize {
                        let copy_length = data.len().min(block.len() - offset_within_block as usize);
                        data[..copy_length]
                            .copy_from_slice(&block[(offset_within_block as usize)..(offset_within_block as usize + copy_length)]);
                    }
                }
            }
            if (data.len().saturating_sub(offset_within_block as usize)) <= blksize as usize {
                break;
            }
            offset += (blksize - offset_within_block);
            data = &mut data[((blksize - offset_within_block) as usize)..];
        }
        if buffer.len() as u64 + original_offset as u64 > fsz {
            if original_offset as u64 <= fsz {
                buffer.resize((fsz - original_offset as u64) as usize, 0);
            } else {
                buffer.clear();
            }
        }

        reply.data(&buffer)
    }

    fn write(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        buffer: &[u8],
        write_flags: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        let blksize = fh;

        if offset < 0 {
            error!("Negative offset requested?");
            return reply.error(EINVAL);
        }
        let mut offset = offset as u64;

        let mut data = &buffer[..];

        loop {
            let (block_n, offset_within_block) = (offset / blksize, offset % blksize);
            let write_limit = data
                .len()
                .min(blksize as usize - offset_within_block as usize);
            debug!(
                "Writing block {} from offset {} to inode {}. Remaining {} bytes of data to write. write_limit={}",
                block_n,
                offset_within_block,
                ino,
                data.len(),
                write_limit,
            );
            let ret = if offset_within_block == 0 && data.len() >= blksize as usize {
                // simple full block write
                self.l
                    .write_block(ino, block_n, data[..write_limit].to_vec())
            } else {
                self.l.modify_block(
                    ino,
                    block_n,
                    offset_within_block as usize,
                    &data[..write_limit],
                )
                // partial block write
            };
            if let Err(e) = ret {
                return reply.error(ee(e));
            }

            if (data.len().saturating_sub(offset_within_block as usize)) <= blksize as usize {
                break;
            }
            offset += (blksize - offset_within_block);
            data = &data[((blksize - offset_within_block) as usize)..];
        }
        reply.written(buffer.len() as u32);
    }

    fn release(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        if let Some(c) = self.opened_files.get(&ino) {
            if c.fetch_sub(1, std::sync::atomic::Ordering::SeqCst) == 1 {
                self.l.maybe_remove_inode(ino);
            }
        }
        self.opened_files.remove_if(&ino, |_, x| {
            x.load(std::sync::atomic::Ordering::SeqCst) == 0
        });
        reply.ok();
    }

    fn opendir(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _flags: i32,
        reply: fuser::ReplyOpen,
    ) {
        self.opened_files
            .entry(ino)
            .or_insert(AtomicU32::new(0))
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        reply.opened(0, 0);
    }

    fn readdir(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: fuser::ReplyDirectory,
    ) {
        let dir = match self.l.readdir(ino) {
            Ok(d) => d,
            Err(e) => return reply.error(ee(e)),
        };
        debug!("Received {} entries", dir.len());
        if offset as usize > dir.len() {
            return reply.ok();
        }
        for (offset_offset, (name, dec)) in dir[offset as usize..].iter().enumerate() {
            debug!("Directory entry.");
            let mut offset_to_report = offset + offset_offset as i64 + 1;
            /*if offset_to_report as usize >= dir.len() {
                offset_to_report = 0;
            }*/
            if reply.add(
                ino,
                offset_to_report,
                dec.kind,
                OsStr::from_bytes(name),
            ) {
                debug!("Full directory buffer.");
                break;
            }
        }
        reply.ok();
    }

    fn releasedir(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        _flags: i32,
        reply: fuser::ReplyEmpty,
    ) {
        self.release(_req, ino, _fh, _flags, None, false, reply)
    }

    fn statfs(&mut self, _req: &fuser::Request<'_>, _ino: u64, reply: fuser::ReplyStatfs) {
        reply.statfs(0, 0, 0, 0, 0, 512, 255, 0);
    }

    fn access(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        _mask: i32,
        reply: fuser::ReplyEmpty,
    ) {
        reply.ok()
    }
}
