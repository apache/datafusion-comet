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

use std::sync::Arc;

use crate::err::HdfsErr;
use crate::hdfs;
use crate::hdfs::{FileStatus, HdfsFs};
use crate::walkdir::tree_iter::{IterOptions, TreeIter, TreeManager};

pub mod tree_iter;

#[derive(Debug)]
pub struct HdfsWalkDir {
    hdfs: Arc<HdfsFs>,
    root: String,
    opts: IterOptions,
}

impl HdfsWalkDir {
    pub fn new(root: String) -> Result<Self, HdfsErr> {
        let hdfs = hdfs::get_hdfs_by_full_path(&root)?;
        Ok(Self::new_with_hdfs(root, hdfs))
    }

    pub fn new_with_hdfs(root: String, hdfs: Arc<HdfsFs>) -> Self {
        Self {
            hdfs,
            root,
            opts: IterOptions {
                min_depth: 0,
                max_depth: usize::MAX,
            },
        }
    }

    /// Set the minimum depth of entries yielded by the iterator.
    ///
    /// The smallest depth is `0` and always corresponds to the path given
    /// to the `new` function on this type. Its direct descendents have depth
    /// `1`, and their descendents have depth `2`, and so on.
    pub fn min_depth(mut self, depth: usize) -> Self {
        self.opts.min_depth = depth;
        if self.opts.min_depth > self.opts.max_depth {
            self.opts.min_depth = self.opts.max_depth;
        }
        self
    }

    /// Set the maximum depth of entries yield by the iterator.
    ///
    /// The smallest depth is `0` and always corresponds to the path given
    /// to the `new` function on this type. Its direct descendents have depth
    /// `1`, and their descendents have depth `2`, and so on.
    ///
    /// Note that this will not simply filter the entries of the iterator, but
    /// it will actually avoid descending into directories when the depth is
    /// exceeded.
    pub fn max_depth(mut self, depth: usize) -> Self {
        self.opts.max_depth = depth;
        if self.opts.max_depth < self.opts.min_depth {
            self.opts.max_depth = self.opts.min_depth;
        }
        self
    }
}

impl IntoIterator for HdfsWalkDir {
    type Item = Result<FileStatus, HdfsErr>;
    type IntoIter = TreeIter<String, FileStatus, HdfsErr>;

    fn into_iter(self) -> TreeIter<String, FileStatus, HdfsErr> {
        TreeIter::new(
            Box::new(HdfsTreeManager {
                hdfs: self.hdfs.clone(),
            }),
            self.opts,
            self.root,
        )
    }
}

struct HdfsTreeManager {
    hdfs: Arc<HdfsFs>,
}

impl TreeManager<String, FileStatus, HdfsErr> for HdfsTreeManager {
    fn to_value(&self, v: String) -> Result<FileStatus, HdfsErr> {
        self.hdfs.get_file_status(&v)
    }

    fn get_children(&self, n: &FileStatus) -> Result<Vec<FileStatus>, HdfsErr> {
        self.hdfs.list_status(n.name())
    }

    fn is_leaf(&self, n: &FileStatus) -> bool {
        n.is_file()
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::hdfs::{HdfsErr, HdfsFs};
    use crate::minidfs::get_dfs;
    use crate::walkdir::HdfsWalkDir;

    #[test]
    fn test_hdfs_file_list() -> Result<(), HdfsErr> {
        let hdfs = set_up_hdfs_env()?;

        let hdfs_walk_dir =
            HdfsWalkDir::new_with_hdfs("/testing".to_owned(), hdfs.clone())
                .min_depth(0)
                .max_depth(2);
        let mut iter = hdfs_walk_dir.into_iter();

        let ret_vec = [
            "/testing",
            "/testing/c",
            "/testing/b",
            "/testing/b/3",
            "/testing/b/2",
            "/testing/b/1",
            "/testing/a",
            "/testing/a/3",
            "/testing/a/2",
            "/testing/a/1",
        ];
        for entry in ret_vec.into_iter() {
            assert_eq!(
                format!("{}{}", hdfs.url(), entry),
                iter.next().unwrap()?.name()
            );
        }
        assert!(iter.next().is_none());

        let hdfs_walk_dir =
            HdfsWalkDir::new_with_hdfs("/testing".to_owned(), hdfs.clone())
                .min_depth(2)
                .max_depth(3);
        let mut iter = hdfs_walk_dir.into_iter();

        let ret_vec = [
            "/testing/b/3",
            "/testing/b/2",
            "/testing/b/1",
            "/testing/a/3",
            "/testing/a/2",
            "/testing/a/2/11",
            "/testing/a/1",
            "/testing/a/1/12",
            "/testing/a/1/11",
        ];
        for entry in ret_vec.into_iter() {
            assert_eq!(
                format!("{}{}", hdfs.url(), entry),
                iter.next().unwrap()?.name()
            );
        }
        assert!(iter.next().is_none());

        Ok(())
    }

    fn set_up_hdfs_env() -> Result<Arc<HdfsFs>, HdfsErr> {
        let dfs = get_dfs();
        let hdfs = dfs.get_hdfs()?;

        hdfs.mkdir("/testing")?;
        hdfs.mkdir("/testing/a")?;
        hdfs.mkdir("/testing/b")?;
        hdfs.create("/testing/c")?;
        hdfs.mkdir("/testing/a/1")?;
        hdfs.mkdir("/testing/a/2")?;
        hdfs.create("/testing/a/3")?;
        hdfs.create("/testing/b/1")?;
        hdfs.create("/testing/b/2")?;
        hdfs.create("/testing/b/3")?;
        hdfs.create("/testing/a/1/11")?;
        hdfs.create("/testing/a/1/12")?;
        hdfs.create("/testing/a/2/11")?;

        Ok(hdfs)
    }
}
