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

//! Operator policy for which native UDF libraries may be loaded.
//!
//! `spark.comet.nativeUdf.enabled` and `spark.comet.nativeUdf.allowedPaths` are enforced on the
//! driver, when `CometNativeUDF.register` validates a library, and again here on the executor
//! before a plan loads one. The library path travels in the plan, so an executor cannot assume the
//! driver that built the plan applied the policy.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::execution::spark_config::{COMET_NATIVE_UDF_ALLOWED_PATHS, COMET_NATIVE_UDF_ENABLED};

/// Which native UDF libraries a session may load.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeUdfPolicy {
    enabled: bool,
    /// Directories a library must be under. Empty means any path is allowed.
    allowed_dirs: Vec<PathBuf>,
}

impl Default for NativeUdfPolicy {
    /// Loading is on and unrestricted, which is the `CometConf` default.
    fn default() -> Self {
        Self {
            enabled: true,
            allowed_dirs: vec![],
        }
    }
}

/// Why a library may not be loaded. The wording of these messages is matched by
/// `CometNativeUDF.classifyNativeError` on the JVM, so keep the two in step.
#[derive(Debug, PartialEq, Eq)]
pub enum PolicyError {
    Disabled,
    /// With an allow-list set, the path must be absolute: a bare name is resolved by the dynamic
    /// loader's search path, which this check cannot see.
    NotAbsolute(String),
    /// The path could not be resolved, so it cannot be shown to be under an allowed directory.
    Unresolvable(String),
    OutsideAllowedPaths(String),
}

impl std::fmt::Display for PolicyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PolicyError::Disabled => write!(
                f,
                "native UDFs are disabled by {COMET_NATIVE_UDF_ENABLED}=false"
            ),
            PolicyError::NotAbsolute(p) => write!(
                f,
                "native UDF library '{p}' must be an absolute path when \
                 {COMET_NATIVE_UDF_ALLOWED_PATHS} is set"
            ),
            PolicyError::Unresolvable(p) => write!(
                f,
                "native UDF library '{p}' cannot be resolved, so it cannot be checked against \
                 {COMET_NATIVE_UDF_ALLOWED_PATHS}"
            ),
            PolicyError::OutsideAllowedPaths(p) => write!(
                f,
                "native UDF library '{p}' is outside {COMET_NATIVE_UDF_ALLOWED_PATHS}"
            ),
        }
    }
}

impl std::error::Error for PolicyError {}

impl NativeUdfPolicy {
    /// Build a policy from the config's two values. `allowed_paths` is the comma-separated list
    /// the config holds; blank entries are ignored.
    ///
    /// Each allowed directory is canonicalized so that a library cannot be reached through a
    /// symlink or a `..` that points out of it. A directory that does not resolve is kept as
    /// written: no resolved library path can be under it, so it allows nothing, and a list made
    /// only of such directories still denies every library rather than falling back to allowing
    /// all of them.
    pub fn new(enabled: bool, allowed_paths: &str) -> Self {
        let listed: Vec<&str> = allowed_paths
            .split(',')
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .collect();
        Self {
            enabled,
            allowed_dirs: listed
                .iter()
                .map(|p| {
                    Path::new(p)
                        .canonicalize()
                        .unwrap_or_else(|_| PathBuf::from(p))
                })
                .collect(),
        }
    }

    /// Read the policy from the config the JVM serialized for `createPlan`. A missing key means the
    /// JVM did not send one, so the default applies; a value that does not parse as a boolean
    /// disables loading rather than guess.
    pub(crate) fn from_spark_config(config: &HashMap<String, String>) -> Self {
        let enabled = config
            .get(COMET_NATIVE_UDF_ENABLED)
            .map(|v| v.parse::<bool>().unwrap_or(false))
            .unwrap_or(true);
        let allowed = config
            .get(COMET_NATIVE_UDF_ALLOWED_PATHS)
            .map(String::as_str)
            .unwrap_or("");
        Self::new(enabled, allowed)
    }

    /// Whether `library_path` may be loaded.
    pub fn check(&self, library_path: &str) -> Result<(), PolicyError> {
        if !self.enabled {
            return Err(PolicyError::Disabled);
        }
        if self.allowed_dirs.is_empty() {
            return Ok(());
        }
        let path = Path::new(library_path);
        if !path.is_absolute() {
            return Err(PolicyError::NotAbsolute(library_path.to_string()));
        }
        let resolved = path
            .canonicalize()
            .map_err(|_| PolicyError::Unresolvable(library_path.to_string()))?;
        if self
            .allowed_dirs
            .iter()
            .any(|dir| resolved.starts_with(dir))
        {
            Ok(())
        } else {
            Err(PolicyError::OutsideAllowedPaths(library_path.to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn touch(dir: &Path, name: &str) -> PathBuf {
        let p = dir.join(name);
        fs::write(&p, b"").unwrap();
        p
    }

    fn check(policy: &NativeUdfPolicy, p: &Path) -> Result<(), PolicyError> {
        policy.check(p.to_str().unwrap())
    }

    #[test]
    fn default_allows_any_path() {
        let policy = NativeUdfPolicy::default();
        assert_eq!(policy.check("/anywhere/libx.so"), Ok(()));
        assert_eq!(policy.check("libx.so"), Ok(()));
    }

    #[test]
    fn disabled_refuses_every_path() {
        let policy = NativeUdfPolicy::new(false, "");
        assert_eq!(
            policy.check("/anywhere/libx.so"),
            Err(PolicyError::Disabled)
        );
    }

    #[test]
    fn allows_a_library_under_an_allowed_directory() {
        let dir = tempfile::tempdir().unwrap();
        let lib = touch(dir.path(), "libx.so");
        let policy = NativeUdfPolicy::new(true, dir.path().to_str().unwrap());
        assert_eq!(check(&policy, &lib), Ok(()));
    }

    #[test]
    fn refuses_a_library_outside_every_allowed_directory() {
        let allowed = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let lib = touch(other.path(), "libx.so");
        let policy = NativeUdfPolicy::new(true, allowed.path().to_str().unwrap());
        assert!(matches!(
            check(&policy, &lib),
            Err(PolicyError::OutsideAllowedPaths(_))
        ));
    }

    #[test]
    fn a_directory_name_prefix_is_not_a_match() {
        let root = tempfile::tempdir().unwrap();
        let allowed = root.path().join("udfs");
        let sibling = root.path().join("udfs-other");
        fs::create_dir(&allowed).unwrap();
        fs::create_dir(&sibling).unwrap();
        let lib = touch(&sibling, "libx.so");
        let policy = NativeUdfPolicy::new(true, allowed.to_str().unwrap());
        assert!(matches!(
            check(&policy, &lib),
            Err(PolicyError::OutsideAllowedPaths(_))
        ));
    }

    #[test]
    fn dot_dot_cannot_escape_an_allowed_directory() {
        let root = tempfile::tempdir().unwrap();
        let allowed = root.path().join("udfs");
        fs::create_dir(&allowed).unwrap();
        let lib = touch(root.path(), "libx.so");
        let sneaky = allowed.join("..").join("libx.so");
        let policy = NativeUdfPolicy::new(true, allowed.to_str().unwrap());
        assert!(matches!(
            check(&policy, &sneaky),
            Err(PolicyError::OutsideAllowedPaths(_))
        ));
        assert!(lib.exists());
    }

    #[cfg(unix)]
    #[test]
    fn a_symlink_cannot_escape_an_allowed_directory() {
        let root = tempfile::tempdir().unwrap();
        let allowed = root.path().join("udfs");
        fs::create_dir(&allowed).unwrap();
        let target = touch(root.path(), "libx.so");
        let link = allowed.join("liblink.so");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let policy = NativeUdfPolicy::new(true, allowed.to_str().unwrap());
        assert!(matches!(
            check(&policy, &link),
            Err(PolicyError::OutsideAllowedPaths(_))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn an_allowed_directory_given_as_a_symlink_is_followed() {
        let root = tempfile::tempdir().unwrap();
        let real = root.path().join("real");
        fs::create_dir(&real).unwrap();
        let alias = root.path().join("alias");
        std::os::unix::fs::symlink(&real, &alias).unwrap();
        let lib = touch(&real, "libx.so");
        let policy = NativeUdfPolicy::new(true, alias.to_str().unwrap());
        assert_eq!(check(&policy, &lib), Ok(()));
    }

    #[test]
    fn with_an_allow_list_a_relative_path_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let policy = NativeUdfPolicy::new(true, dir.path().to_str().unwrap());
        assert!(matches!(
            policy.check("libx.so"),
            Err(PolicyError::NotAbsolute(_))
        ));
    }

    #[test]
    fn with_an_allow_list_a_missing_library_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("libmissing.so");
        let policy = NativeUdfPolicy::new(true, dir.path().to_str().unwrap());
        assert!(matches!(
            check(&policy, &missing),
            Err(PolicyError::Unresolvable(_))
        ));
    }

    #[test]
    fn an_allow_list_of_missing_directories_denies_everything() {
        let dir = tempfile::tempdir().unwrap();
        let lib = touch(dir.path(), "libx.so");
        let policy = NativeUdfPolicy::new(true, "/no/such/dir");
        assert!(matches!(
            check(&policy, &lib),
            Err(PolicyError::OutsideAllowedPaths(_))
        ));
    }

    #[test]
    fn several_directories_are_comma_separated_and_blanks_ignored() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        let lib = touch(b.path(), "libx.so");
        let list = format!(" {} , ,{}", a.path().display(), b.path().display());
        let policy = NativeUdfPolicy::new(true, &list);
        assert_eq!(check(&policy, &lib), Ok(()));
    }

    #[test]
    fn a_blank_allow_list_is_unrestricted() {
        assert_eq!(
            NativeUdfPolicy::new(true, " , ").check("/anywhere/libx.so"),
            Ok(())
        );
    }

    #[test]
    fn reads_the_serialized_spark_config() {
        let dir = tempfile::tempdir().unwrap();
        let lib = touch(dir.path(), "libx.so");
        let config: HashMap<String, String> = [
            (COMET_NATIVE_UDF_ENABLED.to_string(), "true".to_string()),
            (
                COMET_NATIVE_UDF_ALLOWED_PATHS.to_string(),
                dir.path().to_str().unwrap().to_string(),
            ),
        ]
        .into();
        let policy = NativeUdfPolicy::from_spark_config(&config);
        assert_eq!(check(&policy, &lib), Ok(()));
        assert!(policy.check("/elsewhere/libx.so").is_err());

        assert_eq!(
            NativeUdfPolicy::from_spark_config(&HashMap::new()),
            NativeUdfPolicy::default()
        );
        let off: HashMap<String, String> =
            [(COMET_NATIVE_UDF_ENABLED.to_string(), "false".to_string())].into();
        assert_eq!(
            NativeUdfPolicy::from_spark_config(&off).check("/x.so"),
            Err(PolicyError::Disabled)
        );
        let garbled: HashMap<String, String> =
            [(COMET_NATIVE_UDF_ENABLED.to_string(), "TRUE".to_string())].into();
        assert_eq!(
            NativeUdfPolicy::from_spark_config(&garbled).check("/x.so"),
            Err(PolicyError::Disabled)
        );
    }
}
