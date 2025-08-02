//! MiniDfs Cluster
//!
//! MiniDFS provides a embedded HDFS cluster. It is only for testing.
//!
//! Since it's not supported to create multiple MiniDFS instances for parallel testing,
//! we only use a global one which can be get by ``get_dfs()``.
//! And when it is finally destroyed, it's ``drop()`` method will be invoked so that users don't need to care about the resource releasing.
//!
//! ## Example
//!
//! ```ignore
//! use hdfs::minidfs;
//!
//! let dfs = minidfs::get_dfs();
//! let port = dfs.namenode_port();
//! ...
//! ```

use lazy_static::lazy_static;
use libc::{c_char, c_int};
use std::ffi;
use std::mem;
use std::str;
use std::sync::Arc;

use crate::err::HdfsErr;
use crate::hdfs;
use crate::hdfs::HdfsFs;
use crate::native::*;

lazy_static! {
    static ref MINI_DFS: Arc<MiniDFS> = Arc::new(MiniDFS::new());
}

pub fn get_dfs() -> Arc<MiniDFS> {
    MINI_DFS.clone()
}

pub struct MiniDFS {
    cluster: *mut MiniDfsCluster,
}

unsafe impl Send for MiniDFS {}

unsafe impl Sync for MiniDFS {}

impl Drop for MiniDFS {
    fn drop(&mut self) {
        self.stop();
    }
}

impl MiniDFS {
    fn new() -> MiniDFS {
        let conf = new_mini_dfs_conf();
        MiniDFS::start(&conf).unwrap()
    }

    fn start(conf: &MiniDfsConf) -> Option<MiniDFS> {
        match unsafe { nmdCreate(conf) } {
            val if !val.is_null() => Some(MiniDFS { cluster: val }),
            _ => None,
        }
    }

    fn stop(&self) {
        // remove hdfs from global cache
        hdfs::unload_hdfs_cache_by_full_path(self.namenode_addr().as_str()).ok();
        unsafe {
            nmdShutdownClean(self.cluster);
            nmdFree(self.cluster);
        }
    }

    #[allow(dead_code)]
    fn wait_for_clusterup(&self) -> bool {
        unsafe { nmdWaitClusterUp(self.cluster) == 0 }
    }

    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn set_hdfs_builder(&self, builder: *mut hdfsBuilder) -> bool {
        unsafe { nmdConfigureHdfsBuilder(self.cluster, builder) == 0 }
    }

    pub fn namenode_port(&self) -> Option<i32> {
        match unsafe { nmdGetNameNodePort(self.cluster) } {
            val if val > 0 => Some(val),
            _ => None,
        }
    }

    pub fn namenode_addr(&self) -> String {
        if let Some(port) = self.namenode_port() {
            format!("hdfs://localhost:{port}")
        } else {
            "hdfs://localhost".to_string()
        }
    }

    pub fn namenode_http_addr(&self) -> Option<(&str, i32)> {
        let mut hostname: *const c_char = unsafe { mem::zeroed() };
        let mut port: c_int = 0;

        match unsafe { nmdGetNameNodeHttpAddress(self.cluster, &mut port, &mut hostname) }
        {
            0 => {
                let slice = unsafe { ffi::CStr::from_ptr(hostname) }.to_bytes();
                let str = str::from_utf8(slice).unwrap();

                Some((str, port))
            }
            _ => None,
        }
    }

    pub fn get_hdfs(&self) -> Result<Arc<HdfsFs>, HdfsErr> {
        hdfs::get_hdfs_by_full_path(self.namenode_addr().as_str())
    }
}

fn new_mini_dfs_conf() -> MiniDfsConf {
    MiniDfsConf {
        doFormat: 1,
        webhdfsEnabled: 0,
        namenodeHttpPort: 0,
        configureShortCircuit: 0,
    }
}
