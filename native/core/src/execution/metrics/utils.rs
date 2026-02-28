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

use crate::execution::spark_plan::SparkPlan;
use crate::{errors::CometError, jvm_bridge::jni_call};
use datafusion::physical_plan::metrics::MetricValue;
use jni::objects::{GlobalRef, JIntArray, JLongArray, JObject, JObjectArray};
use jni::JNIEnv;
use std::collections::HashMap;
use std::sync::Arc;

/// Pre-computed layout mapping metric names to indices in a flat array.
/// Built once at plan creation, reused on every metric update.
pub(crate) struct MetricLayout {
    /// Per SparkPlan node (DFS order), maps metric name to index in the flat values array
    node_indices: Vec<HashMap<String, usize>>,
    /// Flat array of metric values, written by native and bulk-copied to JVM
    values: Vec<i64>,
    /// Global reference to the JVM long[] array (kept alive for the lifetime of the plan)
    jarray: Arc<GlobalRef>,
}

/// Builds a MetricLayout by calling JNI methods on the CometMetricNode to retrieve
/// the flattened metric names, node offsets, and a reference to the pre-allocated long[].
pub(crate) fn build_metric_layout(
    env: &mut JNIEnv,
    metric_node: &JObject,
) -> Result<MetricLayout, CometError> {
    // Get metric names array (String[])
    let names_obj: JObject =
        unsafe { jni_call!(env, comet_metric_node(metric_node).get_metric_names() -> JObject) }?;
    let names_array = JObjectArray::from(names_obj);
    let num_metrics = env.get_array_length(&names_array)? as usize;

    let mut metric_names = Vec::with_capacity(num_metrics);
    for i in 0..num_metrics {
        let jstr = env.get_object_array_element(&names_array, i as i32)?;
        let name: String = env.get_string((&jstr).into())?.into();
        metric_names.push(name);
    }

    // Get node offsets array (int[])
    let offsets_obj: JObject =
        unsafe { jni_call!(env, comet_metric_node(metric_node).get_node_offsets() -> JObject) }?;
    let offsets_array = JIntArray::from(offsets_obj);
    let num_offsets = env.get_array_length(&offsets_array)? as usize;
    let mut offsets = vec![0i32; num_offsets];
    env.get_int_array_region(&offsets_array, 0, &mut offsets)?;

    // Get values array reference (long[])
    let values_obj: JObject =
        unsafe { jni_call!(env, comet_metric_node(metric_node).get_values_array() -> JObject) }?;
    let jarray = Arc::new(env.new_global_ref(values_obj)?);

    // Build per-node index maps
    let num_nodes = num_offsets - 1;
    let mut node_indices = Vec::with_capacity(num_nodes);
    for node_idx in 0..num_nodes {
        let start = offsets[node_idx] as usize;
        let end = offsets[node_idx + 1] as usize;
        let mut map = HashMap::with_capacity(end - start);
        for (i, name) in metric_names.iter().enumerate().take(end).skip(start) {
            map.insert(name.clone(), i);
        }
        node_indices.push(map);
    }

    Ok(MetricLayout {
        node_indices,
        values: vec![-1i64; num_metrics],
        jarray,
    })
}

/// Recursively fills the values array from DataFusion metrics on the SparkPlan tree.
fn fill_metric_values(
    spark_plan: &Arc<SparkPlan>,
    layout: &mut MetricLayout,
    node_idx: &mut usize,
) {
    let current_node = *node_idx;
    *node_idx += 1;

    if current_node >= layout.node_indices.len() {
        // Skip if node index exceeds layout (shouldn't happen with correct setup)
        for child in spark_plan.children() {
            fill_metric_values(child, layout, node_idx);
        }
        return;
    }

    let indices = &layout.node_indices[current_node];

    // Collect metrics from the native plan (and additional plans)
    let node_metrics = if spark_plan.additional_native_plans.is_empty() {
        spark_plan.native_plan.metrics()
    } else {
        let mut metrics = spark_plan.native_plan.metrics().unwrap_or_default();
        for plan in &spark_plan.additional_native_plans {
            let additional_metrics = plan.metrics().unwrap_or_default();
            for c in additional_metrics.iter() {
                match c.value() {
                    MetricValue::OutputRows(_) => {
                        // do not double count output rows
                    }
                    _ => metrics.push(c.to_owned()),
                }
            }
        }
        Some(metrics.aggregate_by_name())
    };

    // Write metric values into their pre-assigned slots
    if let Some(metrics) = node_metrics {
        for m in metrics.iter() {
            let value = m.value();
            let name = value.name();
            if let Some(&idx) = indices.get(name) {
                layout.values[idx] = value.as_usize() as i64;
            }
        }
    }

    // Recurse into children
    for child in spark_plan.children() {
        fill_metric_values(child, layout, node_idx);
    }
}

/// Updates metrics by filling the flat values array and bulk-copying to JVM.
pub(crate) fn update_comet_metric(
    env: &mut JNIEnv,
    metric_node: &JObject,
    spark_plan: &Arc<SparkPlan>,
    layout: &mut MetricLayout,
) -> Result<(), CometError> {
    if metric_node.is_null() {
        return Ok(());
    }

    // Fill values from native metrics
    let mut node_idx = 0;
    fill_metric_values(spark_plan, layout, &mut node_idx);

    // Bulk copy values to JVM long[] via SetLongArrayRegion
    let local_ref = env.new_local_ref(layout.jarray.as_obj())?;
    let jlong_array = JLongArray::from(local_ref);
    env.set_long_array_region(&jlong_array, 0, &layout.values)?;

    // Call updateFromValues() on the JVM side
    unsafe { jni_call!(env, comet_metric_node(metric_node).update_from_values() -> ()) }
}
