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
use datafusion_comet_proto::spark_metric::NativeMetricNode;
use jni::{objects::JObject, JNIEnv};
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;

/// Updates the metrics of a CometMetricNode. This function is called recursively to
/// update the metrics of all the children nodes. The metrics are pulled from the
/// native execution plan and pushed to the Java side through JNI.
pub fn update_comet_metric(
    env: &mut JNIEnv,
    metric_node: &JObject,
    spark_plan: &Arc<SparkPlan>,
) -> Result<(), CometError> {
    unsafe {
        let native_metric = to_native_metric_node(spark_plan);
        let jbytes = env.byte_array_from_slice(&native_metric?.encode_to_vec())?;
        jni_call!(env, comet_metric_node(metric_node).set_all_from_bytes(&jbytes) -> ())?;
    }
    Ok(())
}

pub fn to_native_metric_node(spark_plan: &Arc<SparkPlan>) -> Result<NativeMetricNode, CometError> {
    let mut native_metric_node = NativeMetricNode {
        metrics: HashMap::new(),
        children: Vec::new(),
    };

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

    // add metrics
    node_metrics
        .unwrap_or_default()
        .iter()
        .map(|m| m.value())
        .map(|m| (m.name(), m.as_usize() as i64))
        .for_each(|(name, value)| {
            native_metric_node.metrics.insert(name.to_string(), value);
        });

    // add children
    spark_plan.children().iter().for_each(|child_plan| {
        let child_node = to_native_metric_node(child_plan).unwrap();
        native_metric_node.children.push(child_node);
    });

    Ok(native_metric_node)
}
