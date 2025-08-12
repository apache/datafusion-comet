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

//! Hdfs Utility

use crate::err::HdfsErr;
use crate::hdfs;
use crate::hdfs::{get_uri, HdfsFs};
use crate::minidfs::MiniDFS;
use crate::native::{hdfsCopy, hdfsMove};
use std::ffi::CString;
use std::sync::Arc;

/// Hdfs Utility
pub struct HdfsUtil;

impl HdfsUtil {
    /// Copy file to hdfs
    pub fn copy_file_to_hdfs(
        dfs: Arc<MiniDFS>,
        src_path: &str,
        dst_path: &str,
    ) -> Result<bool, HdfsErr> {
        let src_uri = get_uri(src_path)?;
        let src_fs = hdfs::get_hdfs_by_full_path(&src_uri)?;

        let dst_fs = dfs.get_hdfs()?;

        HdfsUtil::copy(&src_fs, src_path, &dst_fs, dst_path)
    }

    /// Copy file from hdfs
    pub fn copy_file_from_hdfs(
        dfs: Arc<MiniDFS>,
        src_path: &str,
        dst_path: &str,
    ) -> Result<bool, HdfsErr> {
        let src_fs = dfs.get_hdfs()?;

        let dst_uri = get_uri(dst_path)?;
        let dst_fs = hdfs::get_hdfs_by_full_path(&dst_uri)?;

        HdfsUtil::copy(&src_fs, src_path, &dst_fs, dst_path)
    }

    /// Move file to hdfs
    pub fn mv_file_to_hdfs(
        dfs: Arc<MiniDFS>,
        src_path: &str,
        dst_path: &str,
    ) -> Result<bool, HdfsErr> {
        let src_uri = get_uri(src_path)?;
        let src_fs = hdfs::get_hdfs_by_full_path(&src_uri)?;

        let dst_fs = dfs.get_hdfs()?;

        HdfsUtil::mv(&src_fs, src_path, &dst_fs, dst_path)
    }

    /// Move file from hdfs
    pub fn mv_file_from_hdfs(
        dfs: Arc<MiniDFS>,
        src_path: &str,
        dst_path: &str,
    ) -> Result<bool, HdfsErr> {
        let src_fs = dfs.get_hdfs()?;

        let dst_uri = get_uri(dst_path)?;
        let dst_fs = hdfs::get_hdfs_by_full_path(&dst_uri)?;

        HdfsUtil::mv(&src_fs, src_path, &dst_fs, dst_path)
    }

    /// Copy file from one filesystem to another.
    ///
    /// #### Params
    /// * ```srcFS``` - The handle to source filesystem.
    /// * ```src``` - The path of source file.
    /// * ```dstFS``` - The handle to destination filesystem.
    /// * ```dst``` - The path of destination file.
    pub fn copy(
        src_fs: &HdfsFs,
        src: &str,
        dst_fs: &HdfsFs,
        dst: &str,
    ) -> Result<bool, HdfsErr> {
        let res = unsafe {
            let cstr_src = CString::new(src).unwrap();
            let cstr_dst = CString::new(dst).unwrap();
            hdfsCopy(
                src_fs.raw(),
                cstr_src.as_ptr(),
                dst_fs.raw(),
                cstr_dst.as_ptr(),
            )
        };

        if res == 0 {
            Ok(true)
        } else {
            Err(HdfsErr::Generic(format!(
                "Fail to copy file from {src} to {dst}"
            )))
        }
    }

    /// Move file from one filesystem to another.
    ///
    /// #### Params
    /// * ```srcFS``` - The handle to source filesystem.
    /// * ```src``` - The path of source file.
    /// * ```dstFS``` - The handle to destination filesystem.
    /// * ```dst``` - The path of destination file.
    pub fn mv(
        src_fs: &HdfsFs,
        src: &str,
        dst_fs: &HdfsFs,
        dst: &str,
    ) -> Result<bool, HdfsErr> {
        let res = unsafe {
            let cstr_src = CString::new(src).unwrap();
            let cstr_dst = CString::new(dst).unwrap();
            hdfsMove(
                src_fs.raw(),
                cstr_src.as_ptr(),
                dst_fs.raw(),
                cstr_dst.as_ptr(),
            )
        };

        if res == 0 {
            Ok(true)
        } else {
            Err(HdfsErr::Generic(format!(
                "Fail to move file from {src} to {dst}"
            )))
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::minidfs::get_dfs;
    use std::path::Path;
    use tempfile::tempdir;
    use uuid::Uuid;

    #[test]
    fn test_from_local() {
        // Prepare file source from local file system
        let temp_file = tempfile::Builder::new().tempfile().unwrap();
        let src_path = temp_file.path();
        let src_file_name = src_path.file_name().unwrap().to_str().unwrap();
        let src_file = src_path.to_str().unwrap();

        let dfs = get_dfs();
        {
            // Source
            let src_file_uri = format!("file://{}", src_path.to_str().unwrap());
            let src_fs = hdfs::get_hdfs_by_full_path(&src_file_uri).ok().unwrap();

            // Destination
            let dst_file = format!("/{src_file_name}");
            let dst_fs = dfs.get_hdfs().ok().unwrap();

            // Test copy
            {
                assert!(
                    HdfsUtil::copy_file_to_hdfs(dfs.clone(), src_file, &dst_file).is_ok()
                );
                assert!(dst_fs.exist(&dst_file));
                assert!(src_fs.exist(src_file));
                assert!(Path::new(src_file).exists());
            }

            // Clean up
            assert!(dst_fs.delete(&dst_file, false).is_ok());

            // Test move
            {
                assert!(HdfsUtil::mv_file_to_hdfs(dfs, src_file, &dst_file).is_ok());
                assert!(dst_fs.exist(&dst_file));
                assert!(!src_fs.exist(src_file));
                assert!(!Path::new(src_file).exists());
            }

            // Clean up
            assert!(dst_fs.delete(&dst_file, false).is_ok());
        }
    }

    #[test]
    fn test_to_local() {
        let uuid = Uuid::new_v4().to_string();
        let file_name = uuid.as_str();

        let temp_dir = tempdir().unwrap();
        let dst_path = temp_dir.path().join(file_name);
        let dst_file = dst_path.to_str().unwrap();

        let dfs = get_dfs();
        {
            // Source
            let src_file = format!("/{file_name}");
            let src_fs = dfs.get_hdfs().ok().unwrap();
            src_fs.create(&src_file).ok().unwrap();

            // Destination
            let dst_file_uri = format!("file://{dst_file}");
            let dst_fs = hdfs::get_hdfs_by_full_path(&dst_file_uri).ok().unwrap();

            // Test copy
            {
                assert!(
                    HdfsUtil::copy_file_from_hdfs(dfs.clone(), &src_file, dst_file)
                        .is_ok()
                );
                assert!(src_fs.exist(&src_file));
                assert!(dst_fs.exist(dst_file));
                assert!(Path::new(dst_file).exists());
            }

            // Clean up
            assert!(dst_fs.delete(dst_file, false).is_ok());

            {
                assert!(HdfsUtil::mv_file_from_hdfs(dfs, &src_file, dst_file).is_ok());
                assert!(!src_fs.exist(&src_file));
                assert!(dst_fs.exist(dst_file));
                assert!(Path::new(dst_file).exists());
            }
        }
    }
}
