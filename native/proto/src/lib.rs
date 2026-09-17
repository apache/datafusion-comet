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

// The clippy throws an error if the reference clone not wrapped into `Arc::clone`
// The lint makes easier for code reader/reviewer separate references clones from more heavyweight ones
#![deny(clippy::clone_on_ref_ptr)]

// Include generated modules from .proto files.
#[allow(missing_docs)]
#[allow(clippy::large_enum_variant)]
pub mod spark_expression {
    include!(concat!("generated", "/spark.spark_expression.rs"));
}

// Include generated modules from .proto files.
#[allow(missing_docs)]
pub mod spark_partitioning {
    include!(concat!("generated", "/spark.spark_partitioning.rs"));
}

// Include generated modules from .proto files.
#[allow(missing_docs)]
#[allow(clippy::large_enum_variant)]
pub mod spark_operator {
    include!(concat!("generated", "/spark.spark_operator.rs"));
}

// Include generated modules from .proto files.
#[allow(missing_docs)]
pub mod spark_metric {
    include!(concat!("generated", "/spark.spark_metric.rs"));
}

// Include generated modules from .proto files.
#[allow(missing_docs)]
pub mod spark_config {
    include!(concat!("generated", "/spark.spark_config.rs"));
}

#[cfg(test)]
mod tests {
    use super::spark_operator::{
        partition_writer, LocalPartitionWriter, PartitionWriter, RssPartitionWriter, ShuffleWriter,
    };
    use prost::Message;

    #[derive(Clone, PartialEq, prost::Message)]
    struct LegacyShuffleWriter {
        #[prost(string, tag = "3")]
        output_data_file: String,
        #[prost(string, tag = "4")]
        output_index_file: String,
    }

    fn local_shuffle_writer() -> ShuffleWriter {
        let output_data_file = "/tmp/shuffle.data".to_string();
        let output_index_file = "/tmp/shuffle.index".to_string();

        ShuffleWriter {
            output_data_file: output_data_file.clone(),
            output_index_file: output_index_file.clone(),
            partition_writer: Some(PartitionWriter {
                writer: Some(partition_writer::Writer::Local(LocalPartitionWriter {
                    output_data_file,
                    output_index_file,
                })),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn shuffle_partition_writer_round_trips_local_destination() {
        let encoded = local_shuffle_writer().encode_to_vec();
        let decoded = ShuffleWriter::decode(encoded.as_slice()).unwrap();

        assert_eq!(decoded.output_data_file, "/tmp/shuffle.data");
        assert_eq!(decoded.output_index_file, "/tmp/shuffle.index");
        let Some(partition_writer::Writer::Local(local)) =
            decoded.partition_writer.and_then(|writer| writer.writer)
        else {
            panic!("expected a local shuffle partition writer");
        };
        assert_eq!(local.output_data_file, "/tmp/shuffle.data");
        assert_eq!(local.output_index_file, "/tmp/shuffle.index");
    }

    #[test]
    fn shuffle_partition_writer_round_trips_rss_destination() {
        let writer = ShuffleWriter {
            partition_writer: Some(PartitionWriter {
                writer: Some(partition_writer::Writer::Rss(RssPartitionWriter {})),
            }),
            ..Default::default()
        };
        let decoded = ShuffleWriter::decode(writer.encode_to_vec().as_slice()).unwrap();

        assert!(matches!(
            decoded.partition_writer.and_then(|writer| writer.writer),
            Some(partition_writer::Writer::Rss(_))
        ));
    }

    #[test]
    fn legacy_shuffle_writer_decodes_new_plan_using_compatibility_paths() {
        let encoded = local_shuffle_writer().encode_to_vec();
        let decoded = LegacyShuffleWriter::decode(encoded.as_slice()).unwrap();

        assert_eq!(decoded.output_data_file, "/tmp/shuffle.data");
        assert_eq!(decoded.output_index_file, "/tmp/shuffle.index");
    }

    #[test]
    fn new_shuffle_writer_decodes_legacy_plan_without_destination() {
        let legacy = LegacyShuffleWriter {
            output_data_file: "/tmp/legacy.data".to_string(),
            output_index_file: "/tmp/legacy.index".to_string(),
        };
        let decoded = ShuffleWriter::decode(legacy.encode_to_vec().as_slice()).unwrap();

        assert_eq!(decoded.output_data_file, "/tmp/legacy.data");
        assert_eq!(decoded.output_index_file, "/tmp/legacy.index");
        assert!(decoded.partition_writer.is_none());
    }
}
