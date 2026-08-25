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

use datafusion_comet_proto::spark_operator::ShuffleWriter;
use datafusion_comet_proto::spark_partitioning::{
    partition_writer::PartitionWriterStruct, partitioning::PartitioningStruct,
    LocalPartitionWriter, PartitionWriter, Partitioning, RssPartitionWriter, SinglePartition,
};
use prost::Message;

// The original ShuffleWriter schema, frozen independently of the generated current schema.
#[derive(Clone, PartialEq, Message)]
struct LegacyShuffleWriter {
    #[prost(message, optional, tag = "1")]
    partitioning: Option<Partitioning>,
    #[prost(string, tag = "3")]
    output_data_file: String,
    #[prost(string, tag = "4")]
    output_index_file: String,
}

fn single_partition() -> Option<Partitioning> {
    Some(Partitioning {
        partitioning_struct: Some(PartitioningStruct::SinglePartition(SinglePartition {})),
    })
}

fn local_writer() -> PartitionWriter {
    PartitionWriter {
        partition_writer_struct: Some(PartitionWriterStruct::LocalPartitionWriter(
            LocalPartitionWriter {
                output_data_file: "data".to_string(),
                output_index_file: "index".to_string(),
            },
        )),
    }
}

#[test]
fn legacy_shuffle_writer_wire_format_is_unchanged() {
    let legacy = LegacyShuffleWriter {
        partitioning: single_partition(),
        output_data_file: "data".to_string(),
        output_index_file: "index".to_string(),
    };
    let bytes = legacy.encode_to_vec();
    assert_eq!(bytes, b"\x0a\x02\x12\x00\x1a\x04data\x22\x05index");

    let decoded = ShuffleWriter::decode(bytes.as_slice()).unwrap();
    assert_eq!(decoded.partitioning, legacy.partitioning);
    assert!(decoded.partition_writer.is_none());
    assert_eq!(decoded.output_data_file, legacy.output_data_file);
    assert_eq!(decoded.output_index_file, legacy.output_index_file);
    assert_eq!(decoded.encode_to_vec(), bytes);
}

#[test]
fn dual_written_local_destination_is_readable_by_legacy_decoder() {
    let writer = ShuffleWriter {
        partitioning: single_partition(),
        partition_writer: Some(local_writer()),
        output_data_file: "data".to_string(),
        output_index_file: "index".to_string(),
        ..Default::default()
    };
    let bytes = writer.encode_to_vec();
    assert_eq!(ShuffleWriter::decode(bytes.as_slice()).unwrap(), writer);

    let legacy = LegacyShuffleWriter::decode(bytes.as_slice()).unwrap();
    assert_eq!(legacy.partitioning, writer.partitioning);
    assert_eq!(legacy.output_data_file, writer.output_data_file);
    assert_eq!(legacy.output_index_file, writer.output_index_file);
}

#[test]
fn partition_writer_uses_the_existing_rss_prototype_tags() {
    let local = ShuffleWriter {
        partition_writer: Some(local_writer()),
        ..Default::default()
    };
    // ShuffleWriter field 2 -> local variant 1 -> data/index fields 1/2.
    let local_bytes = b"\x12\x0f\x0a\x0d\x0a\x04data\x12\x05index";
    assert_eq!(local.encode_to_vec(), local_bytes);
    assert_eq!(
        ShuffleWriter::decode(local_bytes.as_slice()).unwrap(),
        local
    );

    let rss = ShuffleWriter {
        partition_writer: Some(PartitionWriter {
            partition_writer_struct: Some(PartitionWriterStruct::RssPartitionWriter(
                RssPartitionWriter {
                    rss_partition_pusher: 7,
                },
            )),
        }),
        ..Default::default()
    };
    // ShuffleWriter field 2 -> RSS variant 2 -> opaque handle field 1.
    let rss_bytes = b"\x12\x04\x12\x02\x08\x07";
    assert_eq!(rss.encode_to_vec(), rss_bytes);
    assert_eq!(ShuffleWriter::decode(rss_bytes.as_slice()).unwrap(), rss);

    // Older native libraries cannot distinguish RSS from an absent destination. A producer
    // therefore needs a capability handshake before it is allowed to emit an RSS plan.
    let legacy = LegacyShuffleWriter::decode(rss_bytes.as_slice()).unwrap();
    assert!(legacy.output_data_file.is_empty());
    assert!(legacy.output_index_file.is_empty());
}

#[test]
fn unknown_destination_stays_distinct_from_legacy_local() {
    // A future field 3 in the PartitionWriter oneof is unknown to this decoder.
    let decoded = ShuffleWriter::decode(b"\x12\x02\x1a\x00".as_slice()).unwrap();
    let destination = decoded.partition_writer.expect("field 2 must stay present");
    assert!(destination.partition_writer_struct.is_none());
}
