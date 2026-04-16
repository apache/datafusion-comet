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

//! Error types for the delta module.
//!
//! Kept local rather than folded into `CometError` because `delta_kernel`
//! lives in an isolated dep subtree — we don't want kernel's error type
//! leaking into `errors.rs` where it could pull kernel's arrow-57 into the
//! main error path.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum DeltaError {
    #[error("invalid delta table URL '{url}': {source}")]
    InvalidUrl {
        url: String,
        #[source]
        source: url::ParseError,
    },

    #[error("cannot resolve local path '{path}': {source}")]
    PathResolution {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("cannot convert path to URL: {path}")]
    PathToUrl { path: String },

    #[error("unsupported URL scheme '{scheme}' for delta table: {url}")]
    UnsupportedScheme { scheme: String, url: String },

    #[error("missing bucket/container in URL: {url}")]
    MissingBucket { url: String },

    #[error("object store construction failed: {0}")]
    ObjectStore(#[from] object_store_kernel::Error),

    #[error("delta kernel error: {0}")]
    Kernel(#[from] delta_kernel::Error),

    #[error("{0}")]
    Internal(String),
}

pub type DeltaResult<T> = std::result::Result<T, DeltaError>;
