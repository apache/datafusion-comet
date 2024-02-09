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

use crate::{
    errors::CometError,
    jvm_bridge::{jni_call, jni_new_string},
};
use datafusion::physical_plan::ExecutionPlan;
use jni::{objects::JObject, JNIEnv};
use std::sync::Arc;

/// Updates the metrics of a CometMetricNode. This function is called recursively to
/// update the metrics of all the children nodes. The metrics are pulled from the
/// DataFusion execution plan and pushed to the Java side through JNI.
pub fn update_comet_metric(
    env: &JNIEnv,
    metric_node: JObject,
    execution_plan: &Arc<dyn ExecutionPlan>,
) -> Result<(), CometError> {
    update_metrics(
        env,
        metric_node,
        &execution_plan
            .metrics()
            .unwrap_or_default()
            .iter()
            .map(|m| m.value())
            .map(|m| (m.name(), m.as_usize() as i64))
            .collect::<Vec<_>>(),
    )?;

    for (i, child_plan) in execution_plan.children().iter().enumerate() {
        let child_metric_node: JObject = jni_call!(env,
            comet_metric_node(metric_node).get_child_node(i as i32) -> JObject
        )?;
        if child_metric_node.is_null() {
            continue;
        }
        update_comet_metric(env, child_metric_node, child_plan)?;
    }
    Ok(())
}

#[inline]
fn update_metrics(
    env: &JNIEnv,
    metric_node: JObject,
    metric_values: &[(&str, i64)],
) -> Result<(), CometError> {
    for &(name, value) in metric_values {
        let jname = jni_new_string!(env, &name)?;
        jni_call!(env, comet_metric_node(metric_node).add(jname, value) -> ())?;
    }
    Ok(())
}
