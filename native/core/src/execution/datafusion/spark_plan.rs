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

use arrow_schema::SchemaRef;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Wrapper around a native plan that maps to a Spark plan and can optionally contain
/// references to other native plans that should contribute to the Spark SQL metrics
/// for the root plan (such as CopyExec and ScanExec nodes)
#[derive(Debug, Clone)]
pub(crate) struct SparkPlan {
    /// Spark plan ID which is passed down in the protobuf
    pub(crate) plan_id: u32,
    /// The root native plan that was generated for this Spark plan
    pub(crate) native_plan: Arc<dyn ExecutionPlan>,
    /// Child Spark plans
    pub(crate) children: Vec<Arc<SparkPlan>>,
    /// Additional native plans that were generated for this Spark plan that we need
    /// to collect metrics for (such as CopyExec and ScanExec)
    pub(crate) additional_native_plans: Vec<Arc<dyn ExecutionPlan>>,
}

impl SparkPlan {
    /// Create a SparkPlan that consists of a single native plan
    pub(crate) fn new(
        plan_id: u32,
        native_plan: Arc<dyn ExecutionPlan>,
        children: Vec<Arc<SparkPlan>>,
    ) -> Self {
        Self {
            plan_id,
            native_plan,
            children,
            additional_native_plans: vec![],
        }
    }

    /// Create a SparkPlan that consists of more than one native plan
    pub(crate) fn new_with_additional(
        plan_id: u32,
        native_plan: Arc<dyn ExecutionPlan>,
        children: Vec<Arc<SparkPlan>>,
        additional_native_plans: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Self {
        Self {
            plan_id,
            native_plan,
            children,
            additional_native_plans,
        }
    }

    /// Get the schema of the native plan
    pub(crate) fn schema(&self) -> SchemaRef {
        self.native_plan.schema()
    }

    /// Get the child SparkPlan instances
    pub(crate) fn children(&self) -> &Vec<Arc<SparkPlan>> {
        &self.children
    }
}
