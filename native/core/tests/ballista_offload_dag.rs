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
// Builder tests for the general DAG offload plan (`build_offload_plan`). No
// cluster is started; these only assert the shape of the built plan (and that
// the build-time leaf-count guard fires), mirroring `ballista_fragment_child_input.rs`.

#![cfg(feature = "ballista")]

use comet::execution::ballista::build_offload_plan;
use datafusion::physical_plan::displayable;
use datafusion_comet_proto::spark_operator::{
    CometBallistaOffloadPlan, OffloadFragment, OffloadInput,
};
use prost::Message;

mod common;
use common::{build_native_scan_proto, build_scan_leaf_block_proto, write_test_parquet};

/// A two-fragment DAG: fragment 0 is a `NativeScan` producer (no inputs) reading
/// Parquet column `a`; fragment 1 is a consumer whose block is a `Scan`(#100) leaf
/// fed by a hash `RepartitionExec` over fragment 0's output column `a` (ordinal 0).
/// `build_offload_plan` must fold this into
/// `CometFragmentExec(consumer, [RepartitionExec::Hash([a@0], 4)(CometFragmentExec(producer, []))])`.
#[test]
fn two_stage_aggregate_builds_hash_repartition_dag() {
    let parquet = std::env::temp_dir().join("comet_ffi_ballista_offload_dag.parquet");
    write_test_parquet(&parquet).expect("write test parquet");
    let producer = build_native_scan_proto(&parquet).expect("build NativeScan producer block");
    let consumer = build_scan_leaf_block_proto();

    let plan = CometBallistaOffloadPlan {
        num_partitions: 4,
        fragments: vec![
            OffloadFragment {
                block_proto: producer,
                inputs: vec![],
            },
            OffloadFragment {
                block_proto: consumer,
                inputs: vec![OffloadInput {
                    producer: 0,
                    hash_key_ordinals: vec![0],
                }],
            },
        ],
    };

    let built = build_offload_plan(&plan.encode_to_vec()).expect("build_offload_plan");
    let rendered = format!("{}", displayable(built.as_ref()).indent(false));
    assert!(rendered.contains("CometFragmentExec"), "got:\n{rendered}");
    assert!(
        rendered.contains("RepartitionExec: partitioning=Hash([a@0], 4)"),
        "got:\n{rendered}"
    );
}

/// A fragment's block must declare exactly as many `OffloadInput`s as it has
/// `Scan`(#100) leaves. Here fragment 1's block is a `NativeScan` (0 leaves), but
/// the descriptor declares 1 input — `build_offload_plan` must fail fast at BUILD
/// time (not lazily inside `CometFragmentExec::execute`).
#[test]
fn leaf_count_mismatch_fails_fast() {
    let parquet = std::env::temp_dir().join("comet_ffi_ballista_offload_dag_mismatch.parquet");
    write_test_parquet(&parquet).expect("write test parquet");
    let producer = build_native_scan_proto(&parquet).expect("build NativeScan producer block");
    // A second NativeScan block: 0 `Scan` leaves, but we wire it up as a consumer
    // with 1 declared input.
    let mismatched = build_native_scan_proto(&parquet).expect("build NativeScan block");

    let plan = CometBallistaOffloadPlan {
        num_partitions: 2,
        fragments: vec![
            OffloadFragment {
                block_proto: producer,
                inputs: vec![],
            },
            OffloadFragment {
                block_proto: mismatched,
                inputs: vec![OffloadInput {
                    producer: 0,
                    hash_key_ordinals: vec![0],
                }],
            },
        ],
    };

    let err =
        build_offload_plan(&plan.encode_to_vec()).expect_err("must fail fast on leaf mismatch");
    assert!(
        err.contains("Scan input leaves"),
        "expected leaf-count mismatch error, got: {err}"
    );
}
