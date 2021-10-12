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

#[macro_use]
pub mod bit;

use crate::TypeTrait;

/// Getter APIs for Comet vectors.
trait ValueGetter<T: TypeTrait> {
    /// Gets the non-null value at `idx`.
    ///
    /// Note that null check needs to be done before the call, to ensure the value at `idx` is
    /// not null.
    fn value(&self, idx: usize) -> T::Native;
}

/// Setter APIs for Comet mutable vectors.
trait ValueSetter<T: TypeTrait> {
    /// Appends a non-null value `v` to the end of this vector.
    fn append_value(&mut self, v: &T::Native);
}

mod vector;

mod buffer;
pub use buffer::*;

mod mutable_vector;
pub use mutable_vector::*;
