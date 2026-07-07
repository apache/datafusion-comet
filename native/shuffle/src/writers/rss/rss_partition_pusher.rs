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

use datafusion_comet_jni_bridge::{jni_call, JVMClasses};
use jni::objects::{Global, JObject};
use std::io::Write;
use std::sync::Arc;

#[derive(Debug)]
pub struct RssPartitionPusher {
    pid: i32,
    jobject: Arc<Global<JObject<'static>>>,
}

impl RssPartitionPusher {
    pub fn try_new(jobject: Arc<Global<JObject<'static>>>) -> datafusion::common::Result<Self> {
        Ok(RssPartitionPusher { pid: -1, jobject })
    }

    pub fn clone_with_pid(&self, pid: i32) -> Self {
        RssPartitionPusher {
            pid,
            jobject: self.jobject.clone(),
        }
    }

    pub fn push_partition_data(&mut self, buf: &[u8]) -> datafusion::common::Result<i32> {
        let length = buf.len() as i32;
        JVMClasses::with_env(|env| {
            let jbytes = env.byte_array_from_slice(buf).unwrap();
            let length: i32 = unsafe {
                jni_call!(env,
                    shuffle_partition_pusher(self.jobject.as_ref()).push_partition_data(self.pid, &jbytes, length) -> i32)?
            };
            Ok(length)
        })
    }
}

impl Clone for RssPartitionPusher {
    fn clone(&self) -> Self {
        RssPartitionPusher {
            pid: self.pid,
            jobject: self.jobject.clone(),
        }
    }
}

impl Write for RssPartitionPusher {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.push_partition_data(buf)
            .map(|n| n as usize)
            .map_err(std::io::Error::other)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
