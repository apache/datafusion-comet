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

use crate::errors::CometError;
use crate::execution::spark_plan::SparkPlan;
use datafusion::physical_plan::metrics::MetricValue;
use datafusion_comet_proto::spark_metric::NativeMetricNode;
use jni::{objects::JObject, Env};
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// The name of the metric, reported on the root of a native plan that runs on a Tokio task, that
/// holds the CPU time the plan's stream used on Comet's Tokio worker threads. Spark's
/// `executorCpuTime` counts only the Spark task thread, which waits for such a plan's batches.
pub(crate) const NATIVE_CPU_TIME: &str = "native_cpu_time";

/// The CPU time the calling thread has used so far, read from its CPU-time clock. `None` where
/// there is no such clock.
pub(crate) fn thread_cpu_time() -> Option<Duration> {
    #[cfg(unix)]
    {
        let mut time = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: `time` is a valid `timespec` for the call to write to.
        let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut time) };
        (rc == 0).then(|| Duration::new(time.tv_sec as u64, time.tv_nsec as u32))
    }
    #[cfg(not(unix))]
    {
        None
    }
}

/// Updates the metrics of a CometMetricNode. This function is called recursively to
/// update the metrics of all the children nodes. The metrics are pulled from the
/// native execution plan and pushed to the Java side through JNI.
///
/// `native_cpu_time`, when given, is reported on the root node as [`NATIVE_CPU_TIME`], in
/// nanoseconds.
pub(crate) fn update_comet_metric(
    env: &mut Env,
    metric_node: &JObject,
    spark_plan: &Arc<SparkPlan>,
    native_cpu_time: Option<usize>,
) -> Result<(), CometError> {
    if metric_node.is_null() {
        return Ok(());
    }

    let mut native_metric = to_native_metric_node(spark_plan)?;
    if let Some(nanos) = native_cpu_time {
        native_metric
            .metrics
            .insert(NATIVE_CPU_TIME.to_string(), nanos as i64);
    }
    let jbytes = env.byte_array_from_slice(&native_metric.encode_to_vec())?;

    unsafe { jni_call!(env, comet_metric_node(metric_node).set_all_from_bytes(&jbytes) -> ()) }
}

pub(crate) fn to_native_metric_node(
    spark_plan: &Arc<SparkPlan>,
) -> Result<NativeMetricNode, CometError> {
    let node_metrics = if spark_plan.additional_native_plans.is_empty() {
        spark_plan.native_plan.metrics()
    } else {
        let mut metrics = spark_plan.native_plan.metrics().unwrap_or_default();
        for plan in &spark_plan.additional_native_plans {
            let additional_metrics = plan.metrics().unwrap_or_default();
            for c in additional_metrics.iter() {
                match c.value() {
                    MetricValue::OutputRows(_) => {
                        // we do not want to double count output rows
                    }
                    _ => metrics.push(c.to_owned()),
                }
            }
        }
        Some(metrics.aggregate_by_name())
    };

    let children = spark_plan.children();
    let mut native_metric_node = NativeMetricNode {
        // Most operator metric maps are well under 20 entries (e.g. hash-join: 9,
        // native-scan: ~20). Pre-sizing to 16 avoids the default-capacity rehash.
        metrics: HashMap::with_capacity(16),
        children: Vec::with_capacity(children.len()),
    };

    // Aggregate metrics by name using DataFusion's aggregate_by_name(), which
    // correctly handles duplicate metric names (e.g. BaselineMetrics registered
    // by both FileStream and ParquetMorselizer on the same ExecutionPlanMetricsSet).
    // The additional_native_plans branch below already does this.
    node_metrics
        .unwrap_or_default()
        .aggregate_by_name()
        .iter()
        .for_each(|m| insert_metric_value(&mut native_metric_node.metrics, m.value()));

    for child_plan in children {
        let child_node = to_native_metric_node(child_plan)?;
        native_metric_node.children.push(child_node);
    }

    Ok(native_metric_node)
}

/// Expand a `MetricValue` into one or more `(name, i64)` entries.
///
/// `MetricValue::as_usize()` returns `0` for `PruningMetrics` and `Ratio` (DF 54.0
/// reserves their aggregation to `MetricsSet`); without expanding them every parquet
/// scan metric of those types surfaces as `0` on the Spark side. Scala-side metrics
/// that aren't declared by `nativeScanMetrics` are silently dropped, so the Spark
/// layer controls what's visible.
fn insert_metric_value(metrics: &mut HashMap<String, i64>, value: &MetricValue) {
    match value {
        MetricValue::PruningMetrics {
            name,
            pruning_metrics,
        } => {
            let pruned = name.as_ref();
            let matched = pruned.replace("pruned", "matched");
            metrics.insert(pruned.to_string(), pruning_metrics.pruned() as i64);
            if matched != pruned {
                metrics.insert(matched, pruning_metrics.matched() as i64);
            }
        }
        MetricValue::Ratio {
            name,
            ratio_metrics,
        } => {
            // Spark's SQLMetric has no ratio type, so we expose numerator and
            // denominator separately and let the Scala side pick what to surface.
            let base = name.as_ref();
            metrics.insert(format!("{base}_part"), ratio_metrics.part() as i64);
            metrics.insert(format!("{base}_total"), ratio_metrics.total() as i64);
        }
        _ => {
            metrics.insert(value.name().to_string(), value.as_usize() as i64);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::metrics::{
        ExecutionPlanMetricsSet, MetricBuilder, MetricCategory, SpillMetrics,
    };

    fn metric_values(metrics: &ExecutionPlanMetricsSet) -> HashMap<String, i64> {
        let mut values = HashMap::new();
        metrics
            .clone_inner()
            .aggregate_by_name()
            .iter()
            .for_each(|metric| insert_metric_value(&mut values, metric.value()));
        values
    }

    #[cfg(unix)]
    #[test]
    fn thread_cpu_time_counts_only_the_calling_threads_cpu() {
        // A thread's CPU time can't run ahead of the wall clock.
        let wall = std::time::Instant::now();
        let start = thread_cpu_time().unwrap();
        while thread_cpu_time().unwrap() - start < Duration::from_millis(20) {}
        assert!(wall.elapsed() >= Duration::from_millis(20));

        // Another thread's work doesn't count, as it would with a clock for the whole process.
        let start = thread_cpu_time().unwrap();
        std::thread::spawn(|| {
            let start = thread_cpu_time().unwrap();
            while thread_cpu_time().unwrap() - start < Duration::from_millis(100) {}
        })
        .join()
        .unwrap();
        let waiting_cpu = thread_cpu_time().unwrap() - start;
        assert!(
            waiting_cpu < Duration::from_millis(50),
            "waiting for 100 ms of another thread's work used {waiting_cpu:?} of CPU"
        );
    }

    #[test]
    fn preserves_absent_and_registered_aggregate_metrics() {
        let aggregate_metric_names = [
            "spill_count",
            "spilled_bytes",
            "spilled_rows",
            "peak_mem_used",
        ];

        let absent = metric_values(&ExecutionPlanMetricsSet::new());
        for name in aggregate_metric_names {
            assert!(!absent.contains_key(name));
        }

        let metrics = ExecutionPlanMetricsSet::new();
        let spill_metrics = SpillMetrics::new(&metrics, 0);
        let peak_mem_used = MetricBuilder::new(&metrics)
            .with_category(MetricCategory::Bytes)
            .gauge("peak_mem_used", 0);

        let registered_zero = metric_values(&metrics);
        for name in aggregate_metric_names {
            assert_eq!(registered_zero.get(name), Some(&0));
        }

        spill_metrics.spill_file_count.add(2);
        spill_metrics.spilled_bytes.add(4096);
        spill_metrics.spilled_rows.add(128);
        peak_mem_used.set(8192);

        let updated = metric_values(&metrics);
        assert_eq!(updated.get("spill_count"), Some(&2));
        assert_eq!(updated.get("spilled_bytes"), Some(&4096));
        assert_eq!(updated.get("spilled_rows"), Some(&128));
        assert_eq!(updated.get("peak_mem_used"), Some(&8192));
    }
}
