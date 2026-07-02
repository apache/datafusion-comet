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

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Extension;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::TableReference;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

use ballista_core::serde::{BallistaLogicalExtensionCodec, BallistaPhysicalExtensionCodec};

use crate::fragment::CometFragmentExec;
use crate::scan::CometScanExec;
use crate::table_provider::CometTableProvider;

/// Marks a payload as a Comet node so the codec can tell it apart from a
/// Ballista/DataFusion node it should delegate.
///
/// Prefix-sniffing this is safe because Ballista/DataFusion codec payloads
/// are protobuf tag streams that never begin with these bytes — the
/// embedded NUL in particular makes a collision effectively impossible.
pub const COMET_MAGIC: &[u8] = b"CMET1\0";

/// Marks a payload as a [`CometFragmentExec`] (a Comet fragment fed by
/// DataFusion children), distinct from [`COMET_MAGIC`] for the childless
/// [`CometScanExec`]. Same collision-safety argument as `COMET_MAGIC`.
pub const COMET_FRAGMENT_MAGIC: &[u8] = b"CMETF\0";

/// Serializes `CometScanExec` as its Comet proto bytes (tagged with `COMET_MAGIC`)
/// and reconstructs it on decode by re-running Comet's planner via FFI. All other
/// nodes (including Ballista's own shuffle operators) delegate to Ballista's codec.
#[derive(Debug, Default)]
pub struct CometPhysicalCodec {
    inner: BallistaPhysicalExtensionCodec,
}

impl PhysicalExtensionCodec for CometPhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(rest) = buf.strip_prefix(COMET_FRAGMENT_MAGIC) {
            // `inputs` are the already-decoded DataFusion children that feed the
            // fragment's `Scan` input leaves.
            return Ok(Arc::new(CometFragmentExec::try_new(
                rest.to_vec(),
                inputs.to_vec(),
            )?));
        }
        if let Some(rest) = buf.strip_prefix(COMET_MAGIC) {
            return Ok(Arc::new(CometScanExec::try_new(rest.to_vec())?));
        }
        self.inner.try_decode(buf, inputs, ctx)
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        if let Some(fragment) = node.downcast_ref::<CometFragmentExec>() {
            buf.extend_from_slice(COMET_FRAGMENT_MAGIC);
            buf.extend_from_slice(fragment.proto());
            return Ok(());
        }
        if let Some(scan) = node.downcast_ref::<CometScanExec>() {
            buf.extend_from_slice(COMET_MAGIC);
            buf.extend_from_slice(scan.proto());
            return Ok(());
        }
        self.inner.try_encode(node, buf)
    }
}

/// Serializes `CometTableProvider` (as its Comet proto bytes, tagged with
/// `COMET_MAGIC`) so a query's logical plan can be shipped client -> scheduler
/// and reconstructed there. Everything else delegates to Ballista's codec.
#[derive(Debug, Default)]
pub struct CometLogicalCodec {
    inner: BallistaLogicalExtensionCodec,
}

impl LogicalExtensionCodec for CometLogicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[datafusion::logical_expr::LogicalPlan],
        ctx: &TaskContext,
    ) -> Result<Extension> {
        self.inner.try_decode(buf, inputs, ctx)
    }

    fn try_encode(&self, node: &Extension, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode(node, buf)
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &TableReference,
        schema: SchemaRef,
        ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        if let Some(rest) = buf.strip_prefix(COMET_MAGIC) {
            return Ok(Arc::new(CometTableProvider::new(rest.to_vec(), schema)));
        }
        self.inner
            .try_decode_table_provider(buf, table_ref, schema, ctx)
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        if let Some(provider) = node.downcast_ref::<CometTableProvider>() {
            buf.extend_from_slice(COMET_MAGIC);
            buf.extend_from_slice(provider.proto());
            return Ok(());
        }
        self.inner.try_encode_table_provider(table_ref, node, buf)
    }
}
