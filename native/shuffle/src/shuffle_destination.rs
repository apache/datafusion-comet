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

use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use crate::PartitionPusher;

/// Resolved output destination for a native shuffle writer.
///
/// Unlike the protobuf destination, RSS carries an owned pusher, not a numeric handle.
/// Resolving and validating task-owned handles belongs to the caller. Cloning an RSS
/// destination shares the same pusher; a different task attempt needs its own pusher.
/// Holding it keeps its Rust state alive, but does not provide remote commit or cancellation
/// semantics.
#[derive(Clone)]
pub enum ShuffleDestination {
    /// One local data file and its partition-offset index.
    Local {
        output_data_file: String,
        output_index_file: String,
        /// Size of the local file write buffer in bytes.
        write_buffer_size: usize,
    },
    /// Complete encoded frames sent through a task-owned callback.
    Rss {
        pusher: Arc<dyn PartitionPusher>,
        /// Maximum encoded bytes in one complete Comet frame.
        max_frame_bytes: usize,
    },
}

impl Debug for ShuffleDestination {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local {
                output_data_file,
                output_index_file,
                write_buffer_size,
            } => f
                .debug_struct("Local")
                .field("output_data_file", output_data_file)
                .field("output_index_file", output_index_file)
                .field("write_buffer_size", write_buffer_size)
                .finish(),
            Self::Rss {
                max_frame_bytes, ..
            } => f
                .debug_struct("Rss")
                .field("max_frame_bytes", max_frame_bytes)
                .finish_non_exhaustive(),
        }
    }
}
