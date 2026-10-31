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

//! Serializes Comet's operators inside the stage and task plans Ballista sends to executors.

use std::sync::Arc;

use ballista_core::serde::BallistaPhysicalExtensionCodec;
use comet::execution::native_executors::CometFragmentExec;
use datafusion::common::{internal_datafusion_err, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_comet_proto::spark_native_executors::{Fragment, Sample};
use datafusion_proto::physical_plan::{PhysicalExtensionCodec, PhysicalProtoConverterExtension};
use prost::Message;

use crate::range::SampleExec;

/// Prefix of an encoded [`CometFragmentExec`], which tells it apart from Ballista's own nodes.
/// The rest is its `Fragment`; its children travel separately, as the plan's.
const FRAGMENT_MAGIC: &[u8] = b"COMET_FRAGMENT\0";

/// Prefix of an encoded [`SampleExec`]. The rest is its `Sample`; its input travels separately.
const SAMPLE_MAGIC: &[u8] = b"COMET_SAMPLE\0";

/// Ballista's physical extension codec, extended with Comet's operators.
#[derive(Debug, Default)]
pub struct CometPhysicalCodec {
    ballista: BallistaPhysicalExtensionCodec,
}

impl PhysicalExtensionCodec for CometPhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(buf) = buf.strip_prefix(SAMPLE_MAGIC) {
            let sample = Sample::decode(buf)
                .map_err(|e| internal_datafusion_err!("Cannot decode a SampleExec: {e}"))?;
            let [input] = inputs else {
                return Err(internal_datafusion_err!("A SampleExec takes one input"));
            };
            return Ok(Arc::new(SampleExec::try_new(
                Arc::clone(input),
                sample.keys.iter().map(|key| *key as usize).collect(),
                sample.per_partition as usize,
            )?));
        }
        let Some(buf) = buf.strip_prefix(FRAGMENT_MAGIC) else {
            return self.ballista.try_decode(buf, inputs, ctx, proto_converter);
        };
        let fragment = Fragment::decode(buf)
            .map_err(|e| internal_datafusion_err!("Cannot decode a CometFragmentExec: {e}"))?;
        Ok(Arc::new(CometFragmentExec::try_new(
            Arc::new(fragment),
            inputs.to_vec(),
        )?))
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        if let Some(sample) = node.downcast_ref::<SampleExec>() {
            buf.extend_from_slice(SAMPLE_MAGIC);
            return Sample {
                per_partition: sample.per_partition() as u64,
                keys: sample.keys().iter().map(|key| *key as u64).collect(),
            }
            .encode(buf)
            .map_err(|e| internal_datafusion_err!("Cannot encode a SampleExec: {e}"));
        }
        let Some(fragment) = node.downcast_ref::<CometFragmentExec>() else {
            return self.ballista.try_encode(node, buf, proto_converter);
        };
        buf.extend_from_slice(FRAGMENT_MAGIC);
        fragment
            .fragment()
            .encode(buf)
            .map_err(|e| internal_datafusion_err!("Cannot encode a CometFragmentExec: {e}"))
    }
}
