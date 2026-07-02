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
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};

use comet::execution::fragment::{build_native_fragment, native_fragment_plan_properties};

/// A DataFusion node that runs a Comet plan fragment (carried as `Operator`
/// proto bytes) whose input-leaf `Scan` operators are fed by this node's
/// DataFusion `children`.
///
/// In a Ballista stage those children are shuffle readers; a childless fragment
/// (whose leaf is a self-contained `NativeScan`) behaves like [`super::scan::CometScanExec`],
/// but reached through the native (non-FFI) path since the executor and Comet
/// share a DataFusion build.
///
/// Serializable through [`super::codec::CometPhysicalCodec`] by its proto bytes;
/// the children round-trip via datafusion-proto and are handed back on decode.
#[derive(Debug)]
pub struct CometFragmentExec {
    proto: Vec<u8>,
    children: Vec<Arc<dyn ExecutionPlan>>,
    props: Arc<PlanProperties>,
}

impl CometFragmentExec {
    /// Build from Comet proto bytes and the fragment's DataFusion children. The
    /// schema/ordering are derived by building the fragment plan once (without
    /// executing it or requiring the child streams).
    ///
    /// A Comet fragment is internally single-partition, but as a DataFusion node
    /// it is a *per-partition* transform: [`execute`](Self::execute) runs the
    /// fragment once for each output partition, feeding that partition's child
    /// streams into the fragment's `Scan` leaves. So when the fragment has
    /// children (e.g. a Ballista shuffle reader with `N` partitions), its output
    /// partition count must match the children's — otherwise consumers (and the
    /// distributed planner / result fetch) would only ever drive partition 0 and
    /// silently drop the other `N-1` partitions' rows. A childless fragment
    /// (self-contained `NativeScan` leaf) keeps the built plan's own partitioning.
    pub fn try_new(proto: Vec<u8>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Self> {
        let base = native_fragment_plan_properties(&proto).map_err(DataFusionError::Execution)?;
        let props = match children.first() {
            Some(child) => {
                let n = child.properties().partitioning.partition_count();
                Arc::new(PlanProperties::new(
                    base.eq_properties.clone(),
                    Partitioning::UnknownPartitioning(n),
                    base.emission_type,
                    base.boundedness,
                ))
            }
            None => base,
        };
        Ok(Self {
            proto,
            children,
            props,
        })
    }

    pub fn proto(&self) -> &[u8] {
        &self.proto
    }
}

impl DisplayAs for CometFragmentExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CometFragmentExec(proto={} bytes, children={})",
            self.proto.len(),
            self.children.len()
        )
    }
}

impl ExecutionPlan for CometFragmentExec {
    fn name(&self) -> &str {
        "CometFragmentExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.children.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CometFragmentExec::try_new(
            self.proto.clone(),
            children,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Materialize one input stream per child for the requested output
        // partition; these feed the fragment's `Scan` input leaves in order.
        let inputs = self
            .children
            .iter()
            .map(|child| child.execute(partition, Arc::clone(&context)))
            .collect::<Result<Vec<_>>>()?;

        build_native_fragment(&self.proto, context, inputs).map_err(DataFusionError::Execution)
    }
}
