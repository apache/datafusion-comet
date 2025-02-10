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
use crate::jvm_bridge::jni_new_global_ref;
use crate::{
    errors::CometError,
    jvm_bridge::{jni_call, jni_new_string},
};
use datafusion::physical_plan::metrics::MetricValue;
use jni::objects::{GlobalRef, JString};
use jni::{objects::JObject, JNIEnv};
use std::collections::HashMap;
use std::sync::Arc;

/// Updates the metrics of a CometMetricNode. This function is called recursively to
/// update the metrics of all the children nodes. The metrics are pulled from the
/// native execution plan and pushed to the Java side through JNI.
pub fn update_comet_metric(
    env: &mut JNIEnv,
    metric_node: &JObject,
    spark_plan: &Arc<SparkPlan>,
    metrics_jstrings: &mut HashMap<String, Arc<GlobalRef>>,
) -> Result<(), CometError> {
    // combine all metrics from all native plans for this SparkPlan
    let metrics = if spark_plan.additional_native_plans.is_empty() {
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

    update_metrics(
        env,
        metric_node,
        &metrics
            .unwrap_or_default()
            .iter()
            .map(|m| m.value())
            .map(|m| (m.name(), m.as_usize() as i64))
            .collect::<Vec<_>>(),
        metrics_jstrings,
    )?;

    unsafe {
        for (i, child_plan) in spark_plan.children().iter().enumerate() {
            let child_metric_node: JObject = jni_call!(env,
                comet_metric_node(metric_node).get_child_node(i as i32) -> JObject
            )?;
            if child_metric_node.is_null() {
                continue;
            }
            update_comet_metric(env, &child_metric_node, child_plan, metrics_jstrings)?;
        }
    }
    Ok(())
}

#[inline]
fn update_metrics(
    env: &mut JNIEnv,
    metric_node: &JObject,
    metric_values: &[(&str, i64)],
    metrics_jstrings: &mut HashMap<String, Arc<GlobalRef>>,
) -> Result<(), CometError> {
    unsafe {
        for &(name, value) in metric_values {
            // Perform a lookup in the jstrings cache.
            if let Some(map_global_ref) = metrics_jstrings.get(name) {
                // Cache hit. Extract the jstring from the global ref.
                let jobject = map_global_ref.as_obj();
                let jstring = JString::from_raw(**jobject);
                // Update the metrics using the jstring as a key.
                jni_call!(env, comet_metric_node(metric_node).set(&jstring, value) -> ())?;
            } else {
                // Cache miss. Allocate a new string, promote to global ref, and insert into cache.
                let local_jstring = jni_new_string!(env, &name)?;
                let global_ref = jni_new_global_ref!(env, local_jstring)?;
                let arc_global_ref = Arc::new(global_ref);
                metrics_jstrings.insert(name.to_string(), Arc::clone(&arc_global_ref));
                let jobject = arc_global_ref.as_obj();
                let jstring = JString::from_raw(**jobject);
                // Update the metrics using the jstring as a key.
                jni_call!(env, comet_metric_node(metric_node).set(&jstring, value) -> ())?;
            }
        }
    }
    Ok(())
}
