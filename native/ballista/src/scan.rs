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

use std::fmt;
use std::sync::Arc;

use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion_ffi::execution_plan::ForeignExecutionPlan;
use tokio::runtime::Handle;

use comet::execution::ffi::comet_ffi_plan_from_proto;

/// A DataFusion leaf that carries a Comet plan protobuf and executes it via the
/// `datafusion-ffi` boundary. Serializable through `CometPhysicalCodec` by its
/// proto bytes, so Ballista can ship it to executors.
#[derive(Debug)]
pub struct CometScanExec {
    proto: Vec<u8>,
    inner: Arc<dyn ExecutionPlan>,
    props: Arc<PlanProperties>,
}

impl CometScanExec {
    /// Build from Comet proto bytes: run Comet's planner via FFI to get the plan,
    /// wrap it as a `ForeignExecutionPlan` (forcing the real FFI vtable path).
    pub fn try_new(proto: Vec<u8>) -> Result<Self> {
        let ffi = comet_ffi_plan_from_proto(&proto, Handle::try_current().ok())
            .map_err(DataFusionError::Execution)?;
        let inner: Arc<dyn ExecutionPlan> = Arc::new(
            ForeignExecutionPlan::try_from(ffi)
                .map_err(|e| DataFusionError::Execution(format!("ForeignExecutionPlan: {e}")))?,
        );
        let props = Arc::clone(inner.properties());
        Ok(Self {
            proto,
            inner,
            props,
        })
    }

    pub fn proto(&self) -> &[u8] {
        &self.proto
    }
}

impl DisplayAs for CometScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CometScanExec(proto={} bytes)", self.proto.len())
    }
}

impl ExecutionPlan for CometScanExec {
    fn name(&self) -> &str {
        "CometScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }
}
