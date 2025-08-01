// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! it's a modified version of hdfs-rs
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::fmt::Write;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::string::String;
use std::sync::{Arc, RwLock};

use lazy_static::lazy_static;
use libc::{c_char, c_int, c_short, c_void, time_t};
use log::info;
use url::Url;

pub use crate::err::HdfsErr;
use crate::native::*;

/// These flags should be consistent with the ones in fcntl.h
const O_RDONLY: c_int = 0;
const O_WRONLY: c_int = 1;
const O_APPEND: c_int = 8;

lazy_static! {
    static ref HDFS_MANAGER: HdfsManager = HdfsManager::new();
}

/// Create instance of HdfsFs with a global cache.
/// For each namenode uri, only one instance will be created
pub fn get_hdfs_by_full_path(path: &str) -> Result<Arc<HdfsFs>, HdfsErr> {
    HDFS_MANAGER.get_hdfs_by_full_path(path)
}

/// The default NameNode configuration will be used (from the XML configuration files)
pub fn get_hdfs() -> Result<Arc<HdfsFs>, HdfsErr> {
    HDFS_MANAGER.get_hdfs_by_full_path("default")
}

/// Remove an instance of HdfsFs from the cache by a specified path with uri
pub fn unload_hdfs_cache_by_full_path(
    path: &str,
) -> Result<Option<Arc<HdfsFs>>, HdfsErr> {
    HDFS_MANAGER.remove_hdfs_by_full_path(path)
}

/// Remove an instance of HdfsFs from the cache
pub fn unload_hdfs_cache(hdfs: Arc<HdfsFs>) -> Result<Option<Arc<HdfsFs>>, HdfsErr> {
    HDFS_MANAGER.remove_hdfs(hdfs)
}

/// Hdfs manager
/// All of the HdfsFs instances will be managed in a singleton HdfsManager
struct HdfsManager {
    hdfs_cache: Arc<RwLock<HashMap<String, Arc<HdfsFs>>>>,
}

impl HdfsManager {
    fn new() -> Self {
        Self {
            hdfs_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn get_hdfs_by_full_path(&self, path: &str) -> Result<Arc<HdfsFs>, HdfsErr> {
        let namenode_uri = match path {
            "default" => "default".to_owned(),
            _ => get_namenode_uri(path)?,
        };

        // Get if already exists
        if let Some(hdfs_fs) = {
            let cache = self.hdfs_cache.read().unwrap();
            cache.get(&namenode_uri).cloned()
        } {
            return Ok(hdfs_fs);
        }

        let mut cache = self.hdfs_cache.write().unwrap();
        // Check again if exists
        let ret = if let Some(hdfs_fs) = cache.get(&namenode_uri) {
            hdfs_fs.clone()
        } else {
            let hdfs_fs = unsafe {
                let hdfs_builder = hdfsNewBuilder();
                let cstr_uri = CString::new(namenode_uri.as_bytes()).unwrap();
                hdfsBuilderSetNameNode(hdfs_builder, cstr_uri.as_ptr());
                info!("Connecting to Namenode ({})", &namenode_uri);
                hdfsBuilderConnect(hdfs_builder)
            };

            if hdfs_fs.is_null() {
                return Err(HdfsErr::CannotConnectToNameNode(namenode_uri.clone()));
            }

            let hdfs_fs = Arc::new(HdfsFs {
                url: namenode_uri.clone(),
                raw: hdfs_fs,
                _marker: PhantomData,
            });
            cache.insert(namenode_uri.clone(), hdfs_fs.clone());
            hdfs_fs
        };

        Ok(ret)
    }

    fn remove_hdfs_by_full_path(
        &self,
        path: &str,
    ) -> Result<Option<Arc<HdfsFs>>, HdfsErr> {
        let namenode_uri = match path {
            "default" => String::from("default"),
            _ => get_namenode_uri(path)?,
        };

        self.remove_hdfs_inner(&namenode_uri)
    }

    fn remove_hdfs(&self, hdfs: Arc<HdfsFs>) -> Result<Option<Arc<HdfsFs>>, HdfsErr> {
        self.remove_hdfs_inner(hdfs.url())
    }

    fn remove_hdfs_inner(&self, hdfs_key: &str) -> Result<Option<Arc<HdfsFs>>, HdfsErr> {
        let mut cache = self.hdfs_cache.write().unwrap();
        Ok(cache.remove(hdfs_key))
    }
}

/// Hdfs Filesystem
///
/// It is basically thread safe because the native API for hdfsFs is thread-safe.
#[derive(Clone)]
pub struct HdfsFs {
    url: String,
    raw: hdfsFS,
    _marker: PhantomData<()>,
}

impl Debug for HdfsFs {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HdfsFs").field("url", &self.url).finish()
    }
}

impl HdfsFs {
    /// Get HDFS namenode url
    #[inline]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Get a raw pointer of JNI API's HdfsFs
    #[inline]
    pub fn raw(&self) -> hdfsFS {
        self.raw
    }

    /// Create HdfsFile from hdfsFile
    fn new_hdfs_file(&self, path: &str, file: hdfsFile) -> Result<HdfsFile, HdfsErr> {
        if file.is_null() {
            Err(HdfsErr::FileNotFound(format!(
                "Fail to create/open file {}",
                path
            )))
        } else {
            Ok(HdfsFile {
                fs: self.clone(),
                path: path.to_owned(),
                file,
                _marker: PhantomData,
            })
        }
    }

    /// open a file to read
    #[inline]
    pub fn open(&self, path: &str) -> Result<HdfsFile, HdfsErr> {
        self.open_with_buf_size(path, 0)
    }

    /// open a file to read with a buffer size
    pub fn open_with_buf_size(
        &self,
        path: &str,
        buf_size: i32,
    ) -> Result<HdfsFile, HdfsErr> {
        let file = unsafe {
            let cstr_path = CString::new(path).unwrap();
            hdfsOpenFile(
                self.raw,
                cstr_path.as_ptr(),
                O_RDONLY,
                buf_size as c_int,
                0,
                0,
            )
        };

        self.new_hdfs_file(path, file)
    }

    /// Get the file status, including file size, last modified time, etc
    pub fn get_file_status(&self, path: &str) -> Result<FileStatus, HdfsErr> {
        let ptr = unsafe {
            let cstr_path = CString::new(path).unwrap();
            hdfsGetPathInfo(self.raw, cstr_path.as_ptr())
        };

        if ptr.is_null() {
            Err(HdfsErr::FileNotFound(format!(
                "Fail to get file status for {}",
                path
            )))
        } else {
            Ok(FileStatus::new(ptr))
        }
    }

    /// Get the file status for each entry under the specified directory
    pub fn list_status(&self, path: &str) -> Result<Vec<FileStatus>, HdfsErr> {
        let mut entry_num: c_int = 0;

        let ptr = unsafe {
            let cstr_path = CString::new(path).unwrap();
            hdfsListDirectory(self.raw, cstr_path.as_ptr(), &mut entry_num)
        };

        let mut list = Vec::new();

        if ptr.is_null() {
            return Ok(list);
        }

        let shared_ptr = Arc::new(HdfsFileInfoPtr::new_array(ptr, entry_num));

        for idx in 0..entry_num {
            list.push(FileStatus::from_array(shared_ptr.clone(), idx as u32));
        }

        Ok(list)
    }

    /// Get the default blocksize.
    pub fn default_blocksize(&self) -> Result<usize, HdfsErr> {
        let block_sz = unsafe { hdfsGetDefaultBlockSize(self.raw) };

        if block_sz >= 0 {
            Ok(block_sz as usize)
        } else {
            Err(HdfsErr::Generic(
                "Fail to get default block size".to_owned(),
            ))
        }
    }

    /// Get the default blocksize at the filesystem indicated by a given path.
    pub fn block_size(&self, path: &str) -> Result<usize, HdfsErr> {
        let block_sz = unsafe {
            let cstr_path = CString::new(path).unwrap();
            hdfsGetDefaultBlockSizeAtPath(self.raw, cstr_path.as_ptr())
        };

        if block_sz >= 0 {
            Ok(block_sz as usize)
        } else {
            Err(HdfsErr::Generic(format!(
                "Fail to get block size for file {}",
                path
            )))
        }
    }

    /// Return the raw capacity of the filesystem.
    pub fn capacity(&self) -> Result<usize, HdfsErr> {
        let capacity = unsafe { hdfsGetCapacity(self.raw) };

        if capacity >= 0 {
            Ok(capacity as usize)
        } else {
            Err(HdfsErr::Generic("Fail to get capacity".to_owned()))
        }
    }

    /// Return the total raw size of all files in the filesystem.
    pub fn used(&self) -> Result<usize, HdfsErr> {
        let used = unsafe { hdfsGetUsed(self.raw) };

        if used >= 0 {
            Ok(used as usize)
        } else {
            Err(HdfsErr::Generic("Fail to get used size".to_owned()))
        }
    }

    /// Checks if a given path exsits on the filesystem
    pub fn exist(&self, path: &str) -> bool {
        (unsafe {
            let cstr_path = CString::new(path).unwrap();
            hdfsExists(self.raw, cstr_path.as_ptr())
        } == 0)
    }

    /// Get hostnames where a particular block (determined by
    /// pos & blocksize) of a file is stored. The last element in the array
    /// is NULL. Due to replication, a single block could be present on
    /// multiple hosts.
    pub fn get_hosts(
        &self,
        path: &str,
        start: usize,
        length: usize,
    ) -> Result<BlockHosts, HdfsErr> {
        let ptr = unsafe {
            let cstr_path = CString::new(path).unwrap();
            hdfsGetHosts(
                self.raw,
                cstr_path.as_ptr(),
                start as tOffset,
                length as tOffset,
            )
        };

        if !ptr.is_null() {
            Ok(BlockHosts { ptr })
        } else {
            Err(HdfsErr::Generic(format!(
                "Fail to get block hosts for file {} from {} with length {}",
                path, start, length
            )))
        }
    }

    #[inline]
    pub fn create(&self, path: &str) -> Result<HdfsFile, HdfsErr> {
        self.create_with_params(path, false, 0, 0, 0)
    }

    #[inline]
    pub fn create_with_overwrite(
        &self,
        path: &str,
        overwrite: bool,
    ) -> Result<HdfsFile, HdfsErr> {
        self.create_with_params(path, overwrite, 0, 0, 0)
    }

    pub fn create_with_params(
        &self,
        path: &str,
        overwrite: bool,
        buf_size: i32,
        replica_num: i16,
        block_size: i32,
    ) -> Result<HdfsFile, HdfsErr> {
        if !overwrite && self.exist(path) {
            return Err(HdfsErr::FileAlreadyExists(path.to_owned()));
        }

        let file = unsafe {
            let cstr_path = CString::new(path).unwrap();
            hdfsOpenFile(
                self.raw,
                cstr_path.as_ptr(),
                O_WRONLY,
                buf_size as c_int,
                replica_num as c_short,
                block_size as tSize,
            )
        };

        self.new_hdfs_file(path, file)
    }

    /// set permission
    pub fn chmod(&self, path: &str, mode: i16) -> bool {
        (unsafe {
            let cstr_path = CString::new(path).unwrap();
            hdfsChmod(self.raw, cstr_path.as_ptr(), mode as c_short)
        }) == 0
    }

    pub fn chown(&self, path: &str, owner: &str, group: &str) -> bool {
        (unsafe {
            let cstr_path = CString::new(path).unwrap();
            let cstr_owner = CString::new(owner).unwrap();
            let cstr_group = CString::new(group).unwrap();
            hdfsChown(
                self.raw,
                cstr_path.as_ptr(),
                cstr_owner.as_ptr(),
                cstr_group.as_ptr(),
            )
        }) == 0
    }

    /// Open a file for append
    pub fn append(&self, path: &str) -> Result<HdfsFile, HdfsErr> {
        if !self.exist(path) {
            return Err(HdfsErr::FileNotFound(path.to_owned()));
        }

        let file = unsafe {
            let cstr_path = CString::new(path).unwrap();
            hdfsOpenFile(self.raw, cstr_path.as_ptr(), O_APPEND | O_WRONLY, 0, 0, 0)
        };

        self.new_hdfs_file(path, file)
    }

    /// create a directory
    pub fn mkdir(&self, path: &str) -> Result<bool, HdfsErr> {
        if unsafe {
            let cstr_path = CString::new(path).unwrap();
            hdfsCreateDirectory(self.raw, cstr_path.as_ptr())
        } == 0
        {
            Ok(true)
        } else {
            Err(HdfsErr::Generic(format!(
                "Fail to create directory for {}",
                path
            )))
        }
    }

    /// Rename file.
    pub fn rename(&self, old_path: &str, new_path: &str, overwrite: bool) -> Result<bool, HdfsErr> {
        if unsafe {
            let cstr_old_path = CString::new(old_path).unwrap();
            let cstr_new_path = CString::new(new_path).unwrap();
            if overwrite {
                hdfsRenameOverwrite(self.raw, cstr_old_path.as_ptr(), cstr_new_path.as_ptr())
            } else {
                hdfsRename(self.raw, cstr_old_path.as_ptr(), cstr_new_path.as_ptr())
            }
        } == 0
        {
            Ok(true)
        } else {
            Err(HdfsErr::Generic(format!(
                "Fail to rename {} to {}",
                old_path, new_path
            )))
        }
    }

    /// Set the replication of the specified file to the supplied value
    pub fn set_replication(&self, path: &str, num: i16) -> Result<bool, HdfsErr> {
        if unsafe {
            let cstr_path = CString::new(path).unwrap();
            hdfsSetReplication(self.raw, cstr_path.as_ptr(), num)
        } == 0
        {
            Ok(true)
        } else {
            Err(HdfsErr::Generic(format!(
                "Fail to set replication {} for {}",
                num, path
            )))
        }
    }

    /// Delete file.
    pub fn delete(&self, path: &str, recursive: bool) -> Result<bool, HdfsErr> {
        if unsafe {
            let cstr_path = CString::new(path).unwrap();
            hdfsDelete(self.raw, cstr_path.as_ptr(), recursive as c_int)
        } == 0
        {
            Ok(true)
        } else {
            Err(HdfsErr::Generic(format!(
                "Fail to delete {} with recursive mode {}",
                path, recursive
            )))
        }
    }
}

/// since HDFS client handles are completely thread safe, here we implement Send+Sync trait for HdfsFs
unsafe impl Send for HdfsFs {}

unsafe impl Sync for HdfsFs {}

/// open hdfs file
#[derive(Clone)]
pub struct HdfsFile {
    fs: HdfsFs,
    path: String,
    file: hdfsFile,
    _marker: PhantomData<()>,
}

impl Debug for HdfsFile {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HdfsFile")
            .field("url", &self.fs.url)
            .field("path", &self.path)
            .finish()
    }
}

impl HdfsFile {
    /// Get HdfsFs
    #[inline]
    pub fn fs(&self) -> &HdfsFs {
        &self.fs
    }

    /// Return a file path
    #[inline]
    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn available(&self) -> Result<bool, HdfsErr> {
        if unsafe { hdfsAvailable(self.fs.raw, self.file) } == 0 {
            Ok(true)
        } else {
            Err(HdfsErr::Generic(format!(
                "File {} is not available",
                self.path()
            )))
        }
    }

    /// Close the opened file
    pub fn close(&self) -> Result<bool, HdfsErr> {
        if unsafe { hdfsCloseFile(self.fs.raw, self.file) } == 0 {
            Ok(true)
        } else {
            Err(HdfsErr::Generic(format!(
                "Fail to close file {}",
                self.path()
            )))
        }
    }

    /// Flush the data.
    pub fn flush(&self) -> bool {
        (unsafe { hdfsFlush(self.fs.raw, self.file) }) == 0
    }

    /// Flush out the data in client's user buffer. After the return of this
    /// call, new readers will see the data.
    pub fn hflush(&self) -> bool {
        (unsafe { hdfsHFlush(self.fs.raw, self.file) }) == 0
    }

    /// Similar to posix fsync, Flush out the data in client's
    /// user buffer. all the way to the disk device (but the disk may have
    /// it in its cache).
    pub fn hsync(&self) -> bool {
        (unsafe { hdfsHSync(self.fs.raw, self.file) }) == 0
    }

    /// Determine if a file is open for read.
    pub fn is_readable(&self) -> bool {
        (unsafe { hdfsFileIsOpenForRead(self.file) }) == 1
    }

    /// Determine if a file is open for write.
    pub fn is_writable(&self) -> bool {
        (unsafe { hdfsFileIsOpenForWrite(self.file) }) == 1
    }

    /// Get the file status, including file size, last modified time, etc
    pub fn get_file_status(&self) -> Result<FileStatus, HdfsErr> {
        self.fs.get_file_status(self.path())
    }

    /// Get the current offset in the file, in bytes.
    pub fn pos(&self) -> Result<u64, HdfsErr> {
        let pos = unsafe { hdfsTell(self.fs.raw, self.file) };

        if pos >= 0 {
            Ok(pos as u64)
        } else {
            Err(HdfsErr::Generic(format!(
                "Fail to get current offset of file {}",
                self.path()
            )))
        }
    }

    /// Read data from an open file.
    pub fn read(&self, buf: &mut [u8]) -> Result<i32, HdfsErr> {
        if buf.is_empty() {
            return Ok(0);
        }
        let read_len = unsafe {
            hdfsRead(
                self.fs.raw,
                self.file,
                buf.as_ptr() as *mut c_void,
                buf.len() as tSize,
            )
        };

        if read_len >= 0 {
            Ok(read_len)
        } else {
            Err(HdfsErr::Generic(format!(
                "Fail to read contents from {} with return code {}",
                self.path(),
                read_len
            )))
        }
    }

    /// Positional read of data from an open file.
    pub fn read_with_pos(&self, pos: i64, buf: &mut [u8]) -> Result<i32, HdfsErr> {
        if buf.is_empty() {
            return Ok(0);
        }
        let read_len = unsafe {
            hdfsPread(
                self.fs.raw,
                self.file,
                pos as tOffset,
                buf.as_ptr() as *mut c_void,
                buf.len() as tSize,
            )
        };

        if read_len >= 0 {
            Ok(read_len)
        } else {
            Err(HdfsErr::Generic(format!(
                "Fail to read contents from {} with offset {} and return code {}",
                self.path(),
                pos,
                read_len
            )))
        }
    }

    /// Seek to given offset in file.
    pub fn seek(&self, offset: u64) -> bool {
        (unsafe { hdfsSeek(self.fs.raw, self.file, offset as tOffset) }) == 0
    }

    /// Write data into an open file.
    pub fn write(&self, buf: &[u8]) -> Result<i32, HdfsErr> {
        if buf.is_empty() {
            return Ok(0);
        }
        let written_len = unsafe {
            hdfsWrite(
                self.fs.raw,
                self.file,
                buf.as_ptr() as *mut c_void,
                buf.len() as tSize,
            )
        };

        if written_len >= 0 {
            Ok(written_len)
        } else {
            Err(HdfsErr::Generic(format!(
                "Fail to write contents to file {}",
                self.path()
            )))
        }
    }
}

/// since HdfsFile is only the pointer to the file on Hdfs, here we implement Send+Sync trait
unsafe impl Send for HdfsFile {}

unsafe impl Sync for HdfsFile {}

/// Interface that represents the client side information for a file or directory.
#[derive(Clone)]
pub struct FileStatus {
    raw: Arc<HdfsFileInfoPtr>,
    idx: u32,
    _marker: PhantomData<()>,
}

impl FileStatus {
    /// create FileStatus from *const hdfsFileInfo
    #[inline]
    fn new(ptr: *const hdfsFileInfo) -> FileStatus {
        FileStatus {
            raw: Arc::new(HdfsFileInfoPtr::new(ptr)),
            idx: 0,
            _marker: PhantomData,
        }
    }

    /// create FileStatus from *const hdfsFileInfo which points
    /// to dynamically allocated array.
    #[inline]
    fn from_array(raw: Arc<HdfsFileInfoPtr>, idx: u32) -> FileStatus {
        FileStatus {
            raw,
            idx,
            _marker: PhantomData,
        }
    }

    /// Get the pointer to hdfsFileInfo
    #[inline]
    fn ptr(&self) -> *const hdfsFileInfo {
        unsafe { self.raw.ptr.offset(self.idx as isize) }
    }

    /// Get the name of the file
    #[inline]
    pub fn name(&self) -> &str {
        let slice = unsafe { CStr::from_ptr((*self.ptr()).mName) }.to_bytes();
        std::str::from_utf8(slice).unwrap()
    }

    /// Is this a file?
    #[inline]
    pub fn is_file(&self) -> bool {
        match unsafe { &*self.ptr() }.mKind {
            tObjectKind::kObjectKindFile => true,
            tObjectKind::kObjectKindDirectory => false,
        }
    }

    /// Is this a directory?
    #[inline]
    pub fn is_directory(&self) -> bool {
        match unsafe { &*self.ptr() }.mKind {
            tObjectKind::kObjectKindFile => false,
            tObjectKind::kObjectKindDirectory => true,
        }
    }

    /// Get the owner of the file
    #[inline]
    pub fn owner(&self) -> &str {
        let slice = unsafe { CStr::from_ptr((*self.ptr()).mOwner) }.to_bytes();
        std::str::from_utf8(slice).unwrap()
    }

    /// Get the group associated with the file
    #[inline]
    pub fn group(&self) -> &str {
        let slice = unsafe { CStr::from_ptr((*self.ptr()).mGroup) }.to_bytes();
        std::str::from_utf8(slice).unwrap()
    }

    /// Get the permissions associated with the file
    #[inline]
    pub fn permission(&self) -> i16 {
        unsafe { &*self.ptr() }.mPermissions
    }

    /// Get the length of this file, in bytes.
    #[allow(clippy::len_without_is_empty)]
    #[inline]
    pub fn len(&self) -> usize {
        unsafe { &*self.ptr() }.mSize as usize
    }

    /// Get the block size of the file.
    #[inline]
    pub fn block_size(&self) -> usize {
        unsafe { &*self.ptr() }.mBlockSize as usize
    }

    /// Get the replication factor of a file.
    #[inline]
    pub fn replica_count(&self) -> i16 {
        unsafe { &*self.ptr() }.mReplication
    }

    /// Get the last modification time for the file in seconds
    #[inline]
    pub fn last_modified(&self) -> time_t {
        unsafe { &*self.ptr() }.mLastMod
    }

    /// Get the last access time for the file in seconds
    #[inline]
    pub fn last_access(&self) -> time_t {
        unsafe { &*self.ptr() }.mLastAccess
    }
}

/// Safely deallocable hdfsFileInfo pointer
struct HdfsFileInfoPtr {
    pub ptr: *const hdfsFileInfo,
    pub len: i32,
}

/// for safe deallocation
impl Drop for HdfsFileInfoPtr {
    fn drop(&mut self) {
        unsafe { hdfsFreeFileInfo(self.ptr as *mut hdfsFileInfo, self.len) };
    }
}

impl HdfsFileInfoPtr {
    fn new(ptr: *const hdfsFileInfo) -> HdfsFileInfoPtr {
        HdfsFileInfoPtr { ptr, len: 1 }
    }

    pub fn new_array(ptr: *const hdfsFileInfo, len: i32) -> HdfsFileInfoPtr {
        HdfsFileInfoPtr { ptr, len }
    }
}

/// since HdfsFileInfoPtr is only the pointer to the Hdfs file info, here we implement Send+Sync trait
unsafe impl Send for HdfsFileInfoPtr {}

unsafe impl Sync for HdfsFileInfoPtr {}

/// Includes hostnames where a particular block of a file is stored.
pub struct BlockHosts {
    ptr: *mut *mut *mut c_char,
}

impl Drop for BlockHosts {
    fn drop(&mut self) {
        unsafe { hdfsFreeHosts(self.ptr) };
    }
}

pub const LOCAL_FS_SCHEME: &str = "file";
pub const HDFS_FS_SCHEME: &str = "hdfs";
pub const VIEW_FS_SCHEME: &str = "viewfs";

#[inline]
fn get_namenode_uri(path: &str) -> Result<String, HdfsErr> {
    match Url::parse(path) {
        Ok(url) => match url.scheme() {
            LOCAL_FS_SCHEME => Ok("file:///".to_string()),
            HDFS_FS_SCHEME | VIEW_FS_SCHEME => {
                if let Some(host) = url.host() {
                    let mut uri_builder = String::new();
                    write!(&mut uri_builder, "{}://{}", url.scheme(), host).unwrap();

                    if let Some(port) = url.port() {
                        write!(&mut uri_builder, ":{}", port).unwrap();
                    }
                    Ok(uri_builder)
                } else {
                    Err(HdfsErr::InvalidUrl(path.to_string()))
                }
            }
            _ => Err(HdfsErr::InvalidUrl(path.to_string())),
        },
        Err(_) => Err(HdfsErr::InvalidUrl(path.to_string())),
    }
}

#[inline]
pub fn get_uri(path: &str) -> Result<String, HdfsErr> {
    let path = if path.starts_with('/') {
        format!("{}://{}", LOCAL_FS_SCHEME, path)
    } else {
        path.to_string()
    };
    match Url::parse(&path) {
        Ok(url) => match url.scheme() {
            LOCAL_FS_SCHEME | HDFS_FS_SCHEME | VIEW_FS_SCHEME => Ok(url.to_string()),
            _ => Err(HdfsErr::InvalidUrl(path.to_string())),
        },
        Err(_) => Err(HdfsErr::InvalidUrl(path.to_string())),
    }
}

#[cfg(test)]
mod test {
    use uuid::Uuid;

    use crate::minidfs::get_dfs;

    #[cfg(feature = "use_existing_hdfs")]
    #[test]
    fn test_hdfs_default() {
        let fs = super::get_hdfs().ok().unwrap();

        let uuid = Uuid::new_v4().to_string();
        let test_file = uuid.as_str();
        let created_file = match fs.create(test_file) {
            Ok(f) => f,
            Err(_) => panic!("Couldn't create a file"),
        };
        assert!(created_file.close().is_ok());
        assert!(fs.exist(test_file));

        assert!(fs.delete(test_file, false).is_ok());
        assert!(!fs.exist(test_file));
    }

    #[test]
    fn test_hdfs() {
        let dfs = get_dfs();
        {
            let minidfs_addr = dfs.namenode_addr();
            let fs = dfs.get_hdfs().ok().unwrap();

            // Test hadoop file
            {
                // create a file, check existence, and close
                let uuid = Uuid::new_v4().to_string();
                let test_file = uuid.as_str();
                let created_file = match fs.create(test_file) {
                    Ok(f) => f,
                    Err(_) => panic!("Couldn't create a file"),
                };
                assert!(created_file.close().is_ok());
                assert!(fs.exist(test_file));

                // open a file and close
                let opened_file = fs.open(test_file).ok().unwrap();
                assert!(opened_file.close().is_ok());

                // Clean up
                assert!(fs.delete(test_file, false).is_ok());
            }

            // Test directory
            {
                let uuid = Uuid::new_v4().to_string();
                let test_dir = format!("/{}", uuid);

                match fs.mkdir(&test_dir) {
                    Ok(_) => println!("{} created", test_dir),
                    Err(_) => panic!("Couldn't create {} directory", test_dir),
                };

                let file_info = fs.get_file_status(&test_dir).ok().unwrap();

                let expected_path = format!("{}{}", minidfs_addr, test_dir);
                assert_eq!(&expected_path, file_info.name());
                assert!(!file_info.is_file());
                assert!(file_info.is_directory());

                let sub_dir_num = 3;
                let mut expected_list = Vec::new();
                for x in 0..sub_dir_num {
                    let filename = format!("{}/{}", test_dir, x);
                    expected_list.push(format!("{}{}/{}", minidfs_addr, test_dir, x));

                    match fs.mkdir(&filename) {
                        Ok(_) => println!("{} created", filename),
                        Err(_) => panic!("Couldn't create {} directory", filename),
                    };
                }

                let mut list = fs.list_status(&test_dir).ok().unwrap();
                assert_eq!(sub_dir_num, list.len());

                list.sort_by(|a, b| Ord::cmp(a.name(), b.name()));
                for (expected, name) in expected_list
                    .iter()
                    .zip(list.iter().map(|status| status.name()))
                {
                    assert_eq!(expected, name);
                }

                // Clean up
                assert!(fs.delete(&test_dir, true).is_ok());
            }
        }
    }

    #[test]
    fn test_list_status_with_empty_dir() {
        let dfs = get_dfs();
        {
            let fs = dfs.get_hdfs().ok().unwrap();

            // Test list status with empty directory
            {
                let uuid = Uuid::new_v4().to_string();
                let test_dir = format!("/{}", uuid);
                let empty_dir = format!("/{}", "_empty");

                match fs.mkdir(&test_dir) {
                    Ok(_) => println!("{} created", test_dir),
                    Err(_) => panic!("Couldn't create {} directory", test_dir),
                };

                match fs.mkdir(&empty_dir) {
                    Ok(_) => println!("{} created", test_dir),
                    Err(_) => panic!("Couldn't create {} directory", test_dir),
                };

                let test_file = format!("{}/{}", &test_dir, "test.txt");
                match fs.create(&test_file) {
                    Ok(_) => println!("{} created", test_file),
                    Err(_) => panic!("Couldn't create {} ", test_file),
                }

                let file_info = fs.list_status(&test_dir).ok().unwrap();
                assert_eq!(file_info.len(), 1);

                let file_info = fs.list_status(&empty_dir).ok().unwrap();
                assert_eq!(file_info.len(), 0);

                // Clean up
                assert!(fs.delete(&test_dir, true).is_ok());
            }
        }
    }

    #[test]
    fn test_write_read() {
        let dfs = get_dfs();
        let fs = dfs.get_hdfs().ok().unwrap();

        let data = "Hello World!!!".as_bytes();

        let uuid = Uuid::new_v4().to_string();
        let test_file = uuid.as_str();

        // Test write to hadoop file
        {
            // create a file, check existence, and close
            let created_file = match fs.create_with_params(test_file, false, 0, 1, 0) {
                Ok(f) => f,
                Err(_) => panic!("Couldn't create a hdfs file"),
            };
            match created_file.write(data) {
                Ok(len) => assert_eq!(len as usize, data.len()),
                Err(_) => panic!("Couldn't write contents to a hdfs file"),
            }
            assert!(created_file.close().is_ok());
        };

        // Test append to hadoop file
        {
            // create a file, check existence, and close
            let appended_file = match fs.append(test_file) {
                Ok(f) => f,
                Err(_) => panic!("Couldn't create a hdfs file"),
            };
            match appended_file.write(data) {
                Ok(len) => assert_eq!(len as usize, data.len()),
                Err(_) => panic!("Couldn't write contents to a hdfs file"),
            }
            assert!(appended_file.close().is_ok());
        };

        // Test read
        {
            assert!(fs.exist(test_file));

            let mut data2 = vec![];
            data2.append(&mut data.to_vec());
            data2.append(&mut data.to_vec());

            // open a file, read, check the contents and close
            let opened_file = fs.open(test_file).ok().unwrap();
            let mut buf = vec![0u8; data2.len()];
            assert!(opened_file.read(&mut buf).is_ok());
            assert_eq!(data2, buf);
            assert!(opened_file.close().is_ok());

            // open a file, read_with_pos, check the contents and close
            let opened_file = fs.open(test_file).ok().unwrap();
            let mut buf = vec![0u8; data2.len()];
            assert!(opened_file.read_with_pos(0, &mut buf).is_ok());
            assert_eq!(data2, buf);
            assert!(opened_file.close().is_ok());
        }

        // Clean up
        assert!(fs.delete(test_file, false).is_ok());
    }
}
