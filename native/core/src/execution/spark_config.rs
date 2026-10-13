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

use std::collections::HashMap;

use crate::errors::{CometError, CometResult};

pub(crate) const COMET_EXEC_SEQUENCE_MAX_BYTES_PER_INVOCATION: &str =
    "spark.comet.exec.sequence.maxBytesPerInvocation";
pub(crate) const COMET_TRACING_ENABLED: &str = "spark.comet.tracing.enabled";
pub(crate) const COMET_DEBUG_ENABLED: &str = "spark.comet.debug.enabled";
pub(crate) const COMET_EXPLAIN_NATIVE_ENABLED: &str = "spark.comet.explain.native.enabled";
pub(crate) const COMET_MAX_TEMP_DIRECTORY_SIZE: &str = "spark.comet.maxTempDirectorySize";
pub(crate) const COMET_DEBUG_MEMORY: &str = "spark.comet.debug.memory";
pub(crate) const COMET_PARQUET_ROW_FILTER_PUSHDOWN_ENABLED: &str =
    "spark.comet.parquet.rowFilterPushdown.enabled";
pub(crate) const COMET_EXEC_AGGREGATE_SKIP_PARTIAL_ENABLED: &str =
    "spark.comet.exec.aggregate.skipPartial.enabled";
pub(crate) const COMET_EXEC_SPILL_COMPRESSION_CODEC: &str =
    "spark.comet.exec.spill.compression.codec";
pub(crate) const COMET_STRICT_TIMESTAMP_LABELS: &str =
    "spark.comet.testing.strictTimestampLabels.enabled";
pub(crate) const SPARK_EXECUTOR_CORES: &str = "spark.executor.cores";

/// The JVM resolves the task's default and byte units before sending this value.
pub(crate) fn sequence_max_bytes(config: &HashMap<String, String>) -> CometResult<Option<usize>> {
    match config
        .get(COMET_EXEC_SEQUENCE_MAX_BYTES_PER_INVOCATION)
        .map(String::as_str)
    {
        None | Some("-1") => Ok(None),
        Some(value) => value.parse::<usize>().map(Some).map_err(|_| {
            CometError::Config(format!(
                "Invalid resolved {COMET_EXEC_SEQUENCE_MAX_BYTES_PER_INVOCATION}: {value}"
            ))
        }),
    }
}

/// Comet configs read through this trait must be resolved by the JVM first:
/// `CometExecIterator.serializeCometSQLConfs` sends booleans as `true` or `false` and sizes as a
/// bare byte count. A config missing from its list arrives exactly as the user wrote it, and a
/// value such as `10g` or `TRUE` then silently parses as the default.
pub(crate) trait SparkConfig {
    fn get_bool(&self, name: &str) -> bool;
    fn get_u64(&self, name: &str, default_value: u64) -> u64;
    fn get_usize(&self, name: &str, default_value: usize) -> usize;
}

impl SparkConfig for HashMap<String, String> {
    fn get_bool(&self, name: &str) -> bool {
        self.get(name)
            .and_then(|str_val| str_val.parse::<bool>().ok())
            .unwrap_or(false)
    }

    fn get_u64(&self, name: &str, default_value: u64) -> u64 {
        self.get(name)
            .and_then(|str_val| str_val.parse::<u64>().ok())
            .unwrap_or(default_value)
    }

    fn get_usize(&self, name: &str, default_value: usize) -> usize {
        self.get(name)
            .and_then(|str_val| str_val.parse::<usize>().ok())
            .unwrap_or(default_value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequence_ceiling_requires_resolved_bytes() {
        let mut config = HashMap::new();
        assert_eq!(sequence_max_bytes(&config).unwrap(), None);
        for (value, expected) in [("-1", None), ("0", Some(0)), ("1024", Some(1024))] {
            config.insert(
                COMET_EXEC_SEQUENCE_MAX_BYTES_PER_INVOCATION.into(),
                value.into(),
            );
            assert_eq!(sequence_max_bytes(&config).unwrap(), expected);
        }
        for value in ["-2", "1k", "invalid", "18446744073709551616"] {
            config.insert(
                COMET_EXEC_SEQUENCE_MAX_BYTES_PER_INVOCATION.into(),
                value.into(),
            );
            assert!(sequence_max_bytes(&config).is_err(), "{value}");
        }
    }
}
