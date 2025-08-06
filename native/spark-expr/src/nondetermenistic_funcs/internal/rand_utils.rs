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

use arrow::array::{Float64Array, Float64Builder};
use datafusion::logical_expr::ColumnarValue;
use std::ops::Deref;
use std::sync::{Arc, Mutex};

pub fn evaluate_batch_for_rand<R, S>(
    state_holder: &Arc<Mutex<Option<S>>>,
    seed: i64,
    num_rows: usize,
) -> datafusion::common::Result<ColumnarValue>
where
    R: StatefulSeedValueGenerator<S, f64>,
    S: Copy,
{
    let seed_state = state_holder.lock().unwrap();
    let mut rnd = R::from_state_ref(seed_state, seed);
    let mut arr_builder = Float64Builder::with_capacity(num_rows);
    std::iter::repeat_with(|| rnd.next_value())
        .take(num_rows)
        .for_each(|v| arr_builder.append_value(v));
    let array_ref = Arc::new(Float64Array::from(arr_builder.finish()));
    let mut seed_state = state_holder.lock().unwrap();
    seed_state.replace(rnd.get_current_state());
    Ok(ColumnarValue::Array(array_ref))
}

pub trait StatefulSeedValueGenerator<State: Copy, Value>: Sized {
    fn from_init_seed(init_seed: i64) -> Self;

    fn from_stored_state(stored_state: State) -> Self;

    fn next_value(&mut self) -> Value;

    fn get_current_state(&self) -> State;

    fn from_state_ref(state: impl Deref<Target = Option<State>>, init_value: i64) -> Self {
        if state.is_none() {
            Self::from_init_seed(init_value)
        } else {
            Self::from_stored_state(state.unwrap())
        }
    }
}
