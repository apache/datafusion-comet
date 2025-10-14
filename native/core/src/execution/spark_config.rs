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

pub(crate) const COMET_TRACING_ENABLED: &str = "spark.comet.tracing.enabled";
pub(crate) const COMET_DEBUG_ENABLED: &str = "spark.comet.debug.enabled";
pub(crate) const COMET_EXPLAIN_NATIVE_ENABLED: &str = "spark.comet.explain.native.enabled";
pub(crate) const COMET_MAX_TEMP_DIRECTORY_SIZE: &str = "spark.comet.maxTempDirectorySize";
pub(crate) const COMET_DEBUG_MEMORY: &str = "spark.comet.debug.memory";

pub(crate) trait SparkConfig {
    fn get_bool(&self, name: &str) -> bool;
    fn get_u64(&self, name: &str, default_value: u64) -> u64;
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
}
