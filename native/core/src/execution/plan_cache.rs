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

//! Executor-local reuse of immutable protobuf plans and bounded single-flight cache (#1204).
//!
//! The definition cache here owns protobuf data only. `shared_pipeline` separately uses the
//! generic cache for audited immutable physical trees. Neither cache may retain task resources.
//! Different partitions' scan payloads must remain different definition-cache keys.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::Instant;

use datafusion_comet_proto::spark_operator::Operator;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;

use super::operators::ExecutionError;
use super::serde::deserialize_op;

const MAX_ENTRIES: usize = 64;
const MAX_ENCODED_BYTES: usize = 8 * 1024 * 1024;

// Process-local because tasks do not share a SessionContext. This cache owns only immutable
// protobuf data, never storage clients, JNI references or task resources. Exact bytes are
// compared, so neither hash collisions nor another query's configuration can change the decoded
// result. Retention is bounded by entry count and encoded bytes, and release_runtime clears it.
// The byte budget accounts for keys, not the decoded Rust heap (which can be larger).
static PLAN_CACHE: LazyLock<PlanCache<Operator>> =
    LazyLock::new(|| PlanCache::new(MAX_ENTRIES, MAX_ENCODED_BYTES));

pub(super) fn decode_plan(
    bytes: &[u8],
    cache_enabled: bool,
) -> Result<Arc<Operator>, ExecutionError> {
    if cache_enabled {
        PLAN_CACHE.get_or_build(bytes, deserialize_op)
    } else {
        deserialize_op(bytes).map(Arc::new)
    }
}

pub(super) fn clear_plan_cache() {
    PLAN_CACHE.clear();
}

type PlanSlot<T> = Arc<OnceCell<Arc<T>>>;

struct CacheEntry<T> {
    plan: PlanSlot<T>,
    last_used: Instant,
}

struct CacheState<T> {
    entries: HashMap<Arc<[u8]>, CacheEntry<T>>,
    encoded_bytes: usize,
}

impl<T> Default for CacheState<T> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            encoded_bytes: 0,
        }
    }
}

pub(super) struct PlanCache<T> {
    state: Mutex<CacheState<T>>,
    max_entries: usize,
    max_encoded_bytes: usize,
}

impl<T> PlanCache<T> {
    pub(super) fn new(max_entries: usize, max_encoded_bytes: usize) -> Self {
        Self {
            state: Mutex::new(CacheState::default()),
            max_entries,
            max_encoded_bytes,
        }
    }

    pub(super) fn get_or_build(
        &self,
        bytes: &[u8],
        build: impl FnOnce(&[u8]) -> Result<T, ExecutionError>,
    ) -> Result<Arc<T>, ExecutionError> {
        // A large plan must not evict the entire cache or circumvent the admission budget.
        if self.max_entries == 0 || bytes.len() > self.max_encoded_bytes {
            return build(bytes).map(Arc::new);
        }

        let slot = {
            let mut state = self.state.lock();
            if let Some(entry) = state.entries.get_mut(bytes) {
                entry.last_used = Instant::now();
                Arc::clone(&entry.plan)
            } else {
                while state.entries.len() >= self.max_entries
                    || bytes.len() > self.max_encoded_bytes - state.encoded_bytes
                {
                    let oldest = state
                        .entries
                        .iter()
                        .min_by_key(|(_, entry)| entry.last_used)
                        .map(|(key, _)| Arc::clone(key))
                        .expect("an over-budget cache has an entry to evict");
                    state.entries.remove(&oldest);
                    state.encoded_bytes -= oldest.len();
                }
                let slot = Arc::new(OnceCell::new());
                state.entries.insert(
                    Arc::from(bytes),
                    CacheEntry {
                        plan: Arc::clone(&slot),
                        last_used: Instant::now(),
                    },
                );
                state.encoded_bytes += bytes.len();
                slot
            }
        };

        // Only one successful build per resident entry, even on concurrent first use. The
        // cache mutex is not held while building or waiting; unrelated plans can make progress.
        let result = slot.get_or_try_init(|| build(bytes).map(Arc::new)).cloned();
        if result.is_err() {
            // Do not retain malformed plans. An eviction/clear and a new insertion may have
            // happened while building: this failed caller must not remove the replacement.
            let mut state = self.state.lock();
            if state
                .entries
                .get(bytes)
                .is_some_and(|entry| Arc::ptr_eq(&entry.plan, &slot))
            {
                state.entries.remove(bytes);
                state.encoded_bytes -= bytes.len();
            }
        }
        result
    }

    pub(super) fn clear(&self) {
        let old = std::mem::take(&mut *self.state.lock());
        // Active attempts retain their own Arc. Dropping the cache never invalidates a task.
        drop(old);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{mpsc, Barrier};
    use std::time::Duration;

    fn encoded(id: u32) -> Vec<u8> {
        Operator {
            plan_id: id,
            ..Default::default()
        }
        .encode_to_vec()
    }

    #[test]
    fn sequential_attempts_reuse_definition_after_previous_attempt_finishes() {
        let cache = PlanCache::new(2, 1024);
        let bytes = encoded(1);
        let first = cache.get_or_build(&bytes, deserialize_op).unwrap();
        let weak = Arc::downgrade(&first);
        drop(first);
        let second = cache
            .get_or_build(&bytes, |_| panic!("decoded again between task waves"))
            .unwrap();
        assert!(Arc::ptr_eq(&weak.upgrade().unwrap(), &second));
    }

    #[test]
    fn concurrent_first_use_decodes_once() {
        let cache = PlanCache::new(2, 1024);
        let decodes = AtomicUsize::new(0);
        let start = Barrier::new(8);
        let plans = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        start.wait();
                        cache
                            .get_or_build(&encoded(7), |bytes| {
                                decodes.fetch_add(1, Ordering::SeqCst);
                                deserialize_op(bytes)
                            })
                            .unwrap()
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert_eq!(decodes.load(Ordering::SeqCst), 1);
        assert!(plans.iter().all(|p| Arc::ptr_eq(p, &plans[0])));
    }

    #[test]
    fn different_plan_bytes_do_not_alias() {
        let cache = PlanCache::new(2, 1024);
        let a = cache.get_or_build(&encoded(1), deserialize_op).unwrap();
        // Spark operator IDs alone are not a key: the payload can change across plans.
        let different_payload = Operator {
            plan_id: 1,
            sql_text_pool: vec!["a different query".into()],
            ..Default::default()
        }
        .encode_to_vec();
        let b = cache
            .get_or_build(&different_payload, deserialize_op)
            .unwrap();
        assert!(!Arc::ptr_eq(&a, &b));
        assert_eq!(a.plan_id, b.plan_id);
        assert!(a.sql_text_pool.is_empty());
        assert_eq!(b.sql_text_pool, ["a different query"]);
    }

    #[test]
    fn least_recently_used_entry_is_evicted_without_invalidating_its_task() {
        let cache = PlanCache::new(2, 1024);
        let a = cache.get_or_build(&encoded(1), deserialize_op).unwrap();
        let b = cache.get_or_build(&encoded(2), deserialize_op).unwrap();
        cache.get_or_build(&encoded(1), deserialize_op).unwrap();
        cache.get_or_build(&encoded(3), deserialize_op).unwrap();
        let a_again = cache.get_or_build(&encoded(1), deserialize_op).unwrap();
        let b_again = cache.get_or_build(&encoded(2), deserialize_op).unwrap();
        assert!(Arc::ptr_eq(&a, &a_again));
        assert!(!Arc::ptr_eq(&b, &b_again));
        assert_eq!(b.plan_id, 2);
    }

    #[test]
    fn encoded_byte_budget_evicts_and_oversized_plans_bypass_cache() {
        let bytes = encoded(1);
        let cache = PlanCache::new(10, bytes.len());
        let a = cache.get_or_build(&bytes, deserialize_op).unwrap();
        cache.get_or_build(&encoded(2), deserialize_op).unwrap();
        let a_again = cache.get_or_build(&bytes, deserialize_op).unwrap();
        assert!(!Arc::ptr_eq(&a, &a_again));
        let large = Operator {
            sql_text_pool: vec!["longer than the admission budget".to_owned()],
            ..Default::default()
        }
        .encode_to_vec();
        let large_a = cache.get_or_build(&large, deserialize_op).unwrap();
        let large_b = cache.get_or_build(&large, deserialize_op).unwrap();
        assert!(!Arc::ptr_eq(&large_a, &large_b));
        assert!(Arc::ptr_eq(
            &a_again,
            &cache.get_or_build(&bytes, deserialize_op).unwrap()
        ));
        assert_eq!(cache.state.lock().encoded_bytes, bytes.len());
    }

    #[test]
    fn malformed_plan_does_not_poison_or_occupy_cache() {
        let cache = PlanCache::new(2, 1024);
        for _ in 0..2 {
            assert!(cache.get_or_build(&[0xff], deserialize_op).is_err());
            assert!(cache.state.lock().entries.is_empty());
            assert_eq!(cache.state.lock().encoded_bytes, 0);
        }
        assert_eq!(
            cache
                .get_or_build(&encoded(1), deserialize_op)
                .unwrap()
                .plan_id,
            1
        );
    }

    #[test]
    fn clear_releases_idle_plans_but_keeps_active_attempts_valid() {
        let cache = PlanCache::new(2, 1024);
        let active = cache.get_or_build(&encoded(1), deserialize_op).unwrap();
        let idle = cache.get_or_build(&encoded(2), deserialize_op).unwrap();
        let idle_weak = Arc::downgrade(&idle);
        drop(idle);
        cache.clear();
        assert!(idle_weak.upgrade().is_none());
        assert_eq!(active.plan_id, 1);
        assert_eq!(cache.state.lock().encoded_bytes, 0);
        assert!(!Arc::ptr_eq(
            &active,
            &cache.get_or_build(&encoded(1), deserialize_op).unwrap()
        ));
    }

    #[test]
    fn decoding_does_not_lock_out_unrelated_plans() {
        let cache = PlanCache::new(2, 1024);
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (other_tx, other_rx) = mpsc::channel();
        std::thread::scope(|scope| {
            let cache = &cache;
            scope.spawn(move || {
                cache
                    .get_or_build(&encoded(1), |bytes| {
                        started_tx.send(()).unwrap();
                        release_rx.recv().unwrap();
                        deserialize_op(bytes)
                    })
                    .unwrap()
            });
            started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            scope.spawn(|| {
                cache.get_or_build(&encoded(2), deserialize_op).unwrap();
                other_tx.send(()).unwrap();
            });
            let progressed = other_rx.recv_timeout(Duration::from_secs(10));
            release_tx.send(()).unwrap();
            assert!(
                progressed.is_ok(),
                "unrelated decode blocked on the cache mutex"
            );
        });
    }

    #[test]
    fn failure_after_clear_does_not_remove_replacement() {
        let cache = PlanCache::new(2, 1024);
        let bytes = encoded(1);
        let mut replacement = None;
        let result = cache.get_or_build(&bytes, |_| {
            cache.clear();
            replacement = Some(cache.get_or_build(&bytes, deserialize_op).unwrap());
            deserialize_op(&[0xff])
        });
        assert!(result.is_err());
        assert!(Arc::ptr_eq(
            &replacement.unwrap(),
            &cache
                .get_or_build(&bytes, |_| panic!("replacement removed"))
                .unwrap()
        ));
    }

    #[test]
    fn shared_definition_keeps_partition_counters_inputs_and_metrics_attempt_local() {
        use crate::execution::operators::InputBatch;
        use crate::execution::planner::PhysicalPlanner;
        use arrow::array::{Int32Array, Int64Array};
        use datafusion::prelude::SessionContext;
        use datafusion_comet_proto::spark_expression::{
            expr::ExprStruct, DataType, EmptyExpr, Expr,
        };
        use datafusion_comet_proto::spark_operator::{operator::OpStruct, Projection, Scan};
        use futures::StreamExt;

        let bytes = Operator {
            children: vec![Operator {
                op_struct: Some(OpStruct::Scan(Scan {
                    fields: vec![DataType {
                        type_id: 4,
                        type_info: None,
                    }],
                    source: "attempt-isolation".into(),
                })),
                ..Default::default()
            }],
            op_struct: Some(OpStruct::Projection(Projection {
                project_list: vec![
                    Expr {
                        expr_struct: Some(ExprStruct::SparkPartitionId(EmptyExpr {})),
                        ..Default::default()
                    },
                    Expr {
                        expr_struct: Some(ExprStruct::MonotonicallyIncreasingId(EmptyExpr {})),
                        ..Default::default()
                    },
                ],
            })),
            ..Default::default()
        }
        .encode_to_vec();
        let cache = PlanCache::new(2, 4096);
        let shared = cache.get_or_build(&bytes, deserialize_op).unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        // The final attempt retries partition 7 while the earlier plans are still alive.
        let mut roots = vec![];
        for (partition, rows) in [(7, 2), (9, 3), (7, 1)] {
            let definition = cache.get_or_build(&bytes, deserialize_op).unwrap();
            assert!(Arc::ptr_eq(&shared, &definition));
            let session = Arc::new(SessionContext::new());
            let planner = PhysicalPlanner::new(Arc::clone(&session), partition);
            let (mut scans, _, root) = planner.create_plan(&definition, &mut vec![], 10).unwrap();
            let mut stream = root.native_plan.execute(0, session.task_ctx()).unwrap();
            scans[0].set_input_batch(InputBatch::Batch(
                vec![Arc::new(Int64Array::from(vec![42; rows]))],
                rows,
            ));
            let batch = runtime.block_on(stream.next()).unwrap().unwrap();
            let partitions = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let ids = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(partitions.values().as_ref(), vec![partition; rows]);
            assert_eq!(
                ids.values().as_ref(),
                (0..rows)
                    .map(|i| ((partition as i64) << 33) + i as i64)
                    .collect::<Vec<_>>()
            );
            scans[0].set_input_batch(InputBatch::EOF);
            assert!(runtime.block_on(stream.next()).is_none());
            assert_eq!(
                root.native_plan.metrics().unwrap().output_rows(),
                Some(rows)
            );
            roots.push(root);
        }
        assert!(!Arc::ptr_eq(&roots[0].native_plan, &roots[2].native_plan));
        assert_eq!(
            roots[0].native_plan.metrics().unwrap().output_rows(),
            Some(2)
        );
    }

    #[test]
    fn disabled_cache_does_not_share_definitions() {
        let a = decode_plan(&encoded(1), false).unwrap();
        let b = decode_plan(&encoded(1), false).unwrap();
        assert!(!Arc::ptr_eq(&a, &b));
    }
}
