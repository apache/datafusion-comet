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

//! The spill directories of a native plan.
//!
//! A [`DiskManager`] in `Directories` mode creates a `datafusion-*` directory in every local
//! directory as soon as it is built, and removes it recursively when it is dropped. Each native
//! plan has its own `DiskManager`, so building one in that mode costs a `stat` and a `mkdir` per
//! local directory when the plan is created, and a recursive delete when it is released, even if
//! the plan never spills (#7060). The plan's disk manager built here creates that `Directories`
//! mode `DiskManager` on the plan's first spill instead, so a plan that never spills touches no
//! directories, and a plan that spills gets the same directories, size limit and cleanup as
//! before.

use datafusion::common::Result;
use datafusion::execution::disk_manager::{DiskManager, DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::{SpillFile, TempFileFactory};
use parking_lot::Mutex;
use std::path::PathBuf;
use std::sync::Arc;

/// Returns the builder of a native plan's [`DiskManager`], which spills into a directory in each
/// of `local_dirs` and limits the plan's spill files to `max_temp_directory_size` bytes in total.
pub(crate) fn plan_disk_manager(
    local_dirs: Vec<PathBuf>,
    max_temp_directory_size: u64,
) -> DiskManagerBuilder {
    let builder = DiskManagerBuilder::default()
        .with_mode(DiskManagerMode::Directories(local_dirs))
        .with_max_temp_directory_size(max_temp_directory_size);
    DiskManagerBuilder::default()
        .with_temp_file_factory(Arc::new(LazyDiskManager {
            builder,
            disk_manager: Mutex::new(None),
        }))
        .with_max_temp_directory_size(max_temp_directory_size)
}

/// Creates spill files through a [`DiskManager`] that it builds for the first one. Every file
/// counts against that manager's size limit, and keeps the manager and its directories alive.
struct LazyDiskManager {
    builder: DiskManagerBuilder,
    disk_manager: Mutex<Option<Arc<DiskManager>>>,
}

impl TempFileFactory for LazyDiskManager {
    fn create_temp_file(&self, description: &str) -> Result<Arc<dyn SpillFile>> {
        let disk_manager = {
            let mut disk_manager = self.disk_manager.lock();
            match disk_manager.as_ref() {
                Some(disk_manager) => Arc::clone(disk_manager),
                None => Arc::clone(disk_manager.insert(Arc::new(self.builder.clone().build()?))),
            }
        };
        disk_manager.create_tmp_file(description)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::path::Path;

    fn entries(dir: &Path) -> Vec<PathBuf> {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect()
    }

    #[test]
    fn spill_directories_are_created_on_the_first_spill() -> Result<()> {
        let local_dirs = [tempfile::tempdir()?, tempfile::tempdir()?];
        let paths = local_dirs.iter().map(|dir| dir.path().to_path_buf());
        let disk_manager = Arc::new(plan_disk_manager(paths.collect(), 1 << 20).build()?);
        let spill_dirs = || -> Vec<PathBuf> {
            local_dirs
                .iter()
                .flat_map(|dir| entries(dir.path()))
                .collect()
        };
        assert!(disk_manager.tmp_files_enabled());
        assert!(spill_dirs().is_empty());

        // The first spill file creates a directory in each local directory, and later ones
        // reuse them.
        let first = disk_manager.create_tmp_file("first")?;
        let second = disk_manager.create_tmp_file("second")?;
        let dirs = spill_dirs();
        assert_eq!(dirs.len(), 2);
        for file in [&first, &second] {
            let parent = file.path().unwrap().parent().unwrap();
            assert!(dirs.iter().any(|dir| dir == parent));
        }

        drop((disk_manager, first, second));
        assert!(spill_dirs().is_empty());
        Ok(())
    }

    #[test]
    fn each_plan_has_its_own_spill_directories_and_size_limit() -> Result<()> {
        let local_dir = tempfile::tempdir()?;
        let plan = || plan_disk_manager(vec![local_dir.path().to_path_buf()], 10).build();
        let (first, second) = (Arc::new(plan()?), Arc::new(plan()?));

        let first_file = first.create_tmp_file("first")?;
        let mut first_writer = first_file.open_writer()?;
        first_writer.write_all(&[0; 8])?;
        // A plan's spill files count against its own limit ...
        assert!(first_writer.write_all(&[0; 8]).is_err());
        // ... and not against another plan's.
        let second_file = second.create_tmp_file("second")?;
        second_file.open_writer()?.write_all(&[0; 8])?;
        assert_eq!(entries(local_dir.path()).len(), 2);
        Ok(())
    }
}
