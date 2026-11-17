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

use super::*;
use datafusion::config::{ConfigFileDecryptionProperties, ParquetEncryptionOptions};
use parquet::encryption::decrypt::FileDecryptionProperties;
use parquet::encryption::encrypt::FileEncryptionProperties;

fn encrypted_file() -> (tempfile::NamedTempFile, ConfigFileDecryptionProperties) {
    // Public test key, shared only by this generated fixture and its reader.
    let key = b"0123456789012345".to_vec();
    let encryption = FileEncryptionProperties::builder(key.clone())
        .build()
        .unwrap();
    let decryption = FileDecryptionProperties::builder(key).build().unwrap();
    let properties = WriterProperties::builder()
        .with_file_encryption_properties(encryption)
        .set_key_value_metadata(Some(vec![KeyValue::new(
            "padding".to_owned(),
            "x".repeat(8 * 1024),
        )]))
        .build();
    let schema = Arc::new(Schema::new(vec![
        Field::new("payload", DataType::Int32, false),
        Field::new("key", DataType::Int32, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![0])),
            Arc::new(Int32Array::from(vec![1])),
        ],
    )
    .unwrap();
    let file = tempfile::NamedTempFile::new().unwrap();
    let mut writer =
        ArrowWriter::try_new(file.reopen().unwrap(), schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    (
        file,
        ConfigFileDecryptionProperties::try_from(&decryption).unwrap(),
    )
}

fn scan_with_crypto(
    file: &tempfile::NamedTempFile,
    session: &Arc<SessionContext>,
    crypto: ParquetEncryptionOptions,
) -> (Arc<DataSourceExec>, ExecutionPlanMetricsSet) {
    let (scan, metrics) = instrumented_scan(&[file], session, false);
    let (config, source, _) = concrete_parquet_source(&scan).unwrap();
    let mut options = source.table_parquet_options().clone();
    options.crypto = crypto;
    let mut config = config.clone();
    config.file_source = Arc::new(source.clone().with_table_parquet_options(options));
    (
        Arc::new(scan.as_ref().clone().with_data_source(Arc::new(config))),
        metrics,
    )
}

#[tokio::test]
async fn encrypted_scan_skips_preflight_and_reads_footer_once() {
    let (file, decryption) = encrypted_file();
    let crypto = ParquetEncryptionOptions {
        file_decryption: Some(decryption),
        ..Default::default()
    };
    let baseline_session = session(1);
    let (baseline_scan, baseline_metrics) =
        scan_with_crypto(&file, &baseline_session, crypto.clone());
    let baseline = aggregate(baseline_scan, false);
    let batches = collect(Arc::new(baseline), baseline_session.task_ctx())
        .await
        .unwrap();
    assert_eq!(result(&batches), Some(1));
    assert_eq!(io_metric(&baseline_metrics, "scan_io_footer_reads"), 1);
    let baseline_bytes = io_metric(&baseline_metrics, "scan_io_metadata_bytes");
    assert!(baseline_bytes > 8 * 1024);

    let session = session(1);
    let (scan, metrics) = scan_with_crypto(&file, &session, crypto);
    let wrapper: Arc<dyn ExecutionPlan> = Arc::new(wrapper(&aggregate(scan, false)));
    let batches = collect(Arc::clone(&wrapper), session.task_ctx())
        .await
        .unwrap();
    assert_eq!(result(&batches), Some(1));
    assert_eq!(
        io_metric(&metrics, "scan_io_metadata_bytes"),
        baseline_bytes
    );
    assert_eq!(io_metric(&metrics, "scan_io_footer_reads"), 1);
    assert_eq!(
        metric(wrapper.as_ref(), "dynamic_filter_minmax_filters_skipped"),
        1
    );
    assert_eq!(
        metric(wrapper.as_ref(), "dynamic_filter_minmax_filters_attached"),
        0
    );
}

#[tokio::test]
async fn encryption_factory_skips_preflight_before_reading_metadata() {
    let session = session(1);
    let (file, _) = encrypted_file();
    let crypto = ParquetEncryptionOptions {
        factory_id: Some("minmax-test-encryption".to_owned()),
        ..Default::default()
    };
    let (scan, metrics) = scan_with_crypto(&file, &session, crypto);
    let input: Arc<dyn ExecutionPlan> = scan;
    let (prepared, enabled) = prepare_scan(&input, &input, 0).await;
    assert!(!enabled);
    assert!(Arc::ptr_eq(&prepared, &input));
    assert_eq!(io_metric(&metrics, "scan_io_object_store_get_calls"), 0);
    assert_eq!(io_metric(&metrics, "scan_io_metadata_bytes"), 0);
}
