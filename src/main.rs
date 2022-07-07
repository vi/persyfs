use std::path::PathBuf;

//#![allow(unused)]
use fuser::{MountOption, FileType};


mod api;
mod filesystem;
mod memory_storage;
mod persy_storage;

use gumdrop::Options;

#[derive(Debug, Options)]
struct PersyFsOpts {
    help: bool,

    /// File path to a Persy database file to open or create or special string `mem:` to use in-memory implementation
    #[options(free, required)]
    path: PathBuf,

    /// Target directory or file to mount FUSE filesystem to
    #[options(free, required)]
    mountpoint: String,

    /// Use FUSE option `default_permissions`.
    default_persmissions: bool,

    /// Auto unmount FUSE filesyste,.
    auto_unmount: bool,

    /// Use FUSE option `allow_other`.
    allow_other: bool,

    /// Use FUSE option `allow_root`.
    allow_root: bool,

    /// Create root inode as a regular file instead of directory
    root_regular_file: bool,

    /// Mount FUSE as readonly. Currently database is still opened readwrite though.
    readonly: bool,

    /// Check for permissions in FUSE filesystem
    default_permissions: bool,

    /// Pass some other option to fusermount
    custom_fuse_option: Vec<String>,

    /// Block size for newly created files
    #[options(default="4096")]
    block_size: u32,
}

fn main() -> anyhow::Result<()> {
    env_logger::init();
    let opts = PersyFsOpts::parse_args_default_or_exit();

    let mys : Box<dyn api::PersistanceLayer + Send>;

    let root_node_kind = if opts.root_regular_file {
        FileType::RegularFile
    } else {
        FileType::Directory
    };

    let mut mopts = Vec::with_capacity(4);

    use std::os::unix::ffi::OsStrExt;
    if opts.path.as_os_str().as_bytes() != b"mem:" {
        mopts.push(MountOption::FSName("persyfs_mem".to_owned()));
        mys = Box::new(persy_storage::open(&opts.path, opts.block_size, root_node_kind)?);
    } else {
        mopts.push(MountOption::FSName("persyfs".to_owned()));
        mys = Box::new(memory_storage::create(opts.block_size, root_node_kind));
    }

    if opts.auto_unmount {
        mopts.push(MountOption::AutoUnmount);
    }
    if opts.allow_other {
        mopts.push(MountOption::AllowOther);
    }
    if opts.allow_root {
        mopts.push(MountOption::AllowRoot);
    }
    if opts.readonly {
        mopts.push(MountOption::RO);
    }
    if opts.default_permissions {
        mopts.push(MountOption::DefaultPermissions);
    }
    for custom in  opts.custom_fuse_option {
        mopts.push(MountOption::CUSTOM(custom));
    }

    let myfs = filesystem::new(api::DynPersistenceLayer(mys));
    fuser::mount2(
        myfs,
        &opts.mountpoint,
        &mopts,
    )?;
    Ok(())
}
