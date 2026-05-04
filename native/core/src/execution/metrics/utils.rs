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

/// Updates the metrics of a CometMetricNode. This function is called recursively to
/// update the metrics of all the children nodes. The metrics are pulled from the
/// native execution plan and pushed to the Java side through JNI.
pub(crate) fn update_comet_metric(
    env: &mut Env,
    metric_node: &JObject,
    spark_plan: &Arc<SparkPlan>,
) -> Result<(), CometError> {
    if metric_node.is_null() {
        return Ok(());
    }

    let native_metric = to_native_metric_node(spark_plan);
    let jbytes = env.byte_array_from_slice(&native_metric?.encode_to_vec())?;

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

    if let Some(metrics) = node_metrics {
        for m in metrics.iter() {
            let value = m.value();
            native_metric_node
                .metrics
                .insert(value.name().to_string(), value.as_usize() as i64);
        }
    }

    for child_plan in children {
        let child_node = to_native_metric_node(child_plan)?;
        native_metric_node.children.push(child_node);
    }

    Ok(native_metric_node)
}
