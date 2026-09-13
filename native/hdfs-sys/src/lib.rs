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

//! Raw bindings to the `libhdfs` C API, built from the vendored Apache Hadoop
//! sources under `libhdfs/hdfs_3_3/`.
//!
//! This crate stands in for the crates.io `hdfs-sys` crate through a
//! `[patch.crates-io]` entry in `native/Cargo.toml`. It exists only so Comet can
//! carry the HDFS-16021 thread-ownership fix, which no released `hdfs-sys`
//! contains. See `README.md` for the provenance and the removal condition.
//!
//! The declarations below are transcribed from the vendored
//! `libhdfs/hdfs_3_3/include/hdfs/hdfs.h` and cover the surface `hdrs`, the only
//! consumer in Comet's dependency graph, actually uses. Names match the
//! bindgen-style spelling that `hdrs` expects, notably the flattened
//! `tObjectKind_kObjectKind*` constants.

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

use std::os::raw::{c_char, c_int, c_long, c_short, c_void};

/// `typedef int32_t tSize` -- size of data for read/write io ops.
pub type tSize = i32;
/// `typedef time_t tTime` -- time type in seconds.
pub type tTime = c_long;
/// `typedef int64_t tOffset` -- offset within the file.
pub type tOffset = i64;
/// `typedef uint16_t tPort` -- port.
pub type tPort = u16;

/// `typedef enum tObjectKind`. A C enum with values that fit in an `unsigned
/// int`, which is how `hdrs` consumes `hdfsFileInfo::mKind`.
pub type tObjectKind = ::std::os::raw::c_uint;
/// `kObjectKindFile = 'F'`
pub const tObjectKind_kObjectKindFile: tObjectKind = 70;
/// `kObjectKindDirectory = 'D'`
pub const tObjectKind_kObjectKindDirectory: tObjectKind = 68;

/// Opaque `struct hdfsBuilder`.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct hdfsBuilder {
    _unused: [u8; 0],
}

/// Opaque `struct hdfs_internal`.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct hdfs_internal {
    _unused: [u8; 0],
}

/// `typedef struct hdfs_internal* hdfsFS`.
pub type hdfsFS = *mut hdfs_internal;

/// Opaque `struct hdfsFile_internal`.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct hdfsFile_internal {
    _unused: [u8; 0],
}

/// `typedef struct hdfsFile_internal* hdfsFile`.
pub type hdfsFile = *mut hdfsFile_internal;

/// Field order and widths mirror the `hdfsFileInfo` typedef in `hdfs.h`.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct hdfsFileInfo {
    /// file or directory
    pub mKind: tObjectKind,
    /// the name of the file
    pub mName: *mut c_char,
    /// the last modification time for the file in seconds
    pub mLastMod: tTime,
    /// the size of the file in bytes
    pub mSize: tOffset,
    /// the count of replicas
    pub mReplication: c_short,
    /// the block size for the file
    pub mBlockSize: tOffset,
    /// the owner of the file
    pub mOwner: *mut c_char,
    /// the group associated with the file
    pub mGroup: *mut c_char,
    /// the permissions associated with the file
    pub mPermissions: c_short,
    /// the last access time for the file in seconds
    pub mLastAccess: tTime,
}

unsafe extern "C" {
    pub fn hdfsNewBuilder() -> *mut hdfsBuilder;
    pub fn hdfsFreeBuilder(bld: *mut hdfsBuilder);
    pub fn hdfsBuilderSetNameNode(bld: *mut hdfsBuilder, nn: *const c_char);
    pub fn hdfsBuilderSetNameNodePort(bld: *mut hdfsBuilder, port: tPort);
    pub fn hdfsBuilderSetUserName(bld: *mut hdfsBuilder, userName: *const c_char);
    pub fn hdfsBuilderSetKerbTicketCachePath(
        bld: *mut hdfsBuilder,
        kerbTicketCachePath: *const c_char,
    );
    pub fn hdfsBuilderConnect(bld: *mut hdfsBuilder) -> hdfsFS;

    pub fn hdfsConnect(nn: *const c_char, port: tPort) -> hdfsFS;
    pub fn hdfsDisconnect(fs: hdfsFS) -> c_int;

    pub fn hdfsOpenFile(
        fs: hdfsFS,
        path: *const c_char,
        flags: c_int,
        bufferSize: c_int,
        replication: c_short,
        blocksize: tSize,
    ) -> hdfsFile;
    pub fn hdfsCloseFile(fs: hdfsFS, file: hdfsFile) -> c_int;
    pub fn hdfsExists(fs: hdfsFS, path: *const c_char) -> c_int;

    pub fn hdfsSeek(fs: hdfsFS, file: hdfsFile, desiredPos: tOffset) -> c_int;
    pub fn hdfsTell(fs: hdfsFS, file: hdfsFile) -> tOffset;
    pub fn hdfsRead(fs: hdfsFS, file: hdfsFile, buffer: *mut c_void, length: tSize) -> tSize;
    pub fn hdfsPread(
        fs: hdfsFS,
        file: hdfsFile,
        position: tOffset,
        buffer: *mut c_void,
        length: tSize,
    ) -> tSize;
    pub fn hdfsWrite(fs: hdfsFS, file: hdfsFile, buffer: *const c_void, length: tSize) -> tSize;
    pub fn hdfsFlush(fs: hdfsFS, file: hdfsFile) -> c_int;

    pub fn hdfsDelete(fs: hdfsFS, path: *const c_char, recursive: c_int) -> c_int;
    pub fn hdfsRename(fs: hdfsFS, oldPath: *const c_char, newPath: *const c_char) -> c_int;
    pub fn hdfsCreateDirectory(fs: hdfsFS, path: *const c_char) -> c_int;

    pub fn hdfsGetPathInfo(fs: hdfsFS, path: *const c_char) -> *mut hdfsFileInfo;
    pub fn hdfsListDirectory(
        fs: hdfsFS,
        path: *const c_char,
        numEntries: *mut c_int,
    ) -> *mut hdfsFileInfo;
    pub fn hdfsFreeFileInfo(hdfsFileInfo: *mut hdfsFileInfo, numEntries: c_int);
}

/// `hdrs` reads `hdfsFileInfo` fields out of an array libhdfs allocated, so a layout mismatch
/// between these declarations and the C typedef they transcribe would be silent memory corruption
/// rather than a link error. These are compile-time assertions rather than `#[test]`s so that every
/// build of the crate enforces them; the crate is not a default workspace member, so `cargo test`
/// would not otherwise reach it.
///
/// The expected offsets follow from the C field order under the usual 8-byte alignment:
/// `mKind`(u32) + 4 pad, `mName`(8), `mLastMod`(8), `mSize`(8), `mReplication`(2) + 6 pad,
/// `mBlockSize`(8), `mOwner`(8), `mGroup`(8), `mPermissions`(2) + 6 pad, `mLastAccess`(8).
const _: () = {
    use std::mem::{align_of, offset_of, size_of};

    assert!(size_of::<hdfsFileInfo>() == 80);
    assert!(align_of::<hdfsFileInfo>() == 8);
    assert!(offset_of!(hdfsFileInfo, mKind) == 0);
    assert!(offset_of!(hdfsFileInfo, mName) == 8);
    assert!(offset_of!(hdfsFileInfo, mLastMod) == 16);
    assert!(offset_of!(hdfsFileInfo, mSize) == 24);
    assert!(offset_of!(hdfsFileInfo, mReplication) == 32);
    assert!(offset_of!(hdfsFileInfo, mBlockSize) == 40);
    assert!(offset_of!(hdfsFileInfo, mOwner) == 48);
    assert!(offset_of!(hdfsFileInfo, mGroup) == 56);
    assert!(offset_of!(hdfsFileInfo, mPermissions) == 64);
    assert!(offset_of!(hdfsFileInfo, mLastAccess) == 72);

    // `hdfs.h` spells these as the character literals 'F' and 'D'.
    assert!(tObjectKind_kObjectKindFile == b'F' as tObjectKind);
    assert!(tObjectKind_kObjectKindDirectory == b'D' as tObjectKind);

    // `tTime` is `time_t`, and `hdrs` binds the two timestamp fields to `i64`.
    assert!(size_of::<tTime>() == 8);
};
