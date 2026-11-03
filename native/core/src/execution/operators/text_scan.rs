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

//! Native Text file scan, mirroring Spark's `text` data source.
//!
//! Spark's text reader produces a single `value: string` column. With the default options each
//! line of the file is one row (split on `\n`, `\r\n`, or `\r`, with the terminator stripped).
//! With `wholetext=true` the whole file is a single row. A custom `lineSep` splits on exactly that
//! byte sequence instead of the universal-newline set. See the `TextOptions`/`HadoopFileLinesReader`
//! logic in Spark's `org.apache.spark.sql.execution.datasources.text` package.

use crate::execution::operators::ExecutionError;
use arrow::array::StringBuilder;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use bytes::Bytes;
use datafusion::common::Result;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder};
use datafusion::physical_plan::DisplayFormatType;
use datafusion_comet_common::decode_utf8_spark_lossy;
use datafusion_comet_proto::spark_operator::TextOptions;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::projection::{ProjectionOpener, SplitProjection};
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::{as_file_source, PartitionedFile, TableSchema};
use futures::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt};
use std::fmt;
use std::sync::Arc;

/// Batch size used when the execution framework does not set one before opening the file.
const DEFAULT_TEXT_BATCH_SIZE: usize = 8192;

/// UTF-8 byte-order mark. Spark's line reader strips a leading one; wholetext keeps it.
const UTF8_BOM: &[u8] = &[0xEF, 0xBB, 0xBF];

/// Cap on a batch's decoded `value` buffer bytes, kept well below Arrow's 2GB 32-bit offset limit
/// so a batch of many rows (or a few long lines) cannot overflow. Measured against the decoded
/// string length, since a lossy UTF-8 decode can expand ill-formed bytes up to 3x.
const MAX_BATCH_VALUE_BYTES: usize = 1 << 30;

pub fn init_text_datasource_exec(
    object_store_url: ObjectStoreUrl,
    file_groups: Vec<Vec<PartitionedFile>>,
    data_schema: SchemaRef,
    projection_vector: Vec<usize>,
    text_options: &TextOptions,
) -> Result<Arc<DataSourceExec>, ExecutionError> {
    let text_source = TextSource::new(data_schema, text_options);

    let file_groups = file_groups.into_iter().map(FileGroup::new).collect();

    let file_scan_config = FileScanConfigBuilder::new(object_store_url, Arc::new(text_source))
        .with_file_groups(file_groups)
        .with_projection_indices(Some(projection_vector))?
        .build();

    Ok(DataSourceExec::from_data_source(file_scan_config))
}

/// A [`FileSource`] for Spark's text format. The file schema is always the single `value` column;
/// projection (and partition columns, if any) is handled by [`ProjectionOpener`], exactly as the
/// CSV source does.
#[derive(Debug, Clone)]
struct TextSource {
    table_schema: TableSchema,
    projection: SplitProjection,
    batch_size: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
    whole_text: bool,
    // The raw separator bytes when a custom `lineSep` is set; None means universal newline
    // (`\n`, `\r\n`, `\r`).
    line_sep: Option<Vec<u8>>,
}

impl TextSource {
    fn new(file_schema: SchemaRef, options: &TextOptions) -> Self {
        let table_schema: TableSchema = file_schema.into();
        // `line_sep` arrives as the exact separator bytes Spark computed (`lineSeparatorInRead`,
        // already encoded with the file's charset), so we split on them directly.
        let line_sep = options.line_sep.clone().filter(|sep| !sep.is_empty());
        Self {
            projection: SplitProjection::unprojected(&table_schema),
            table_schema,
            batch_size: None,
            metrics: ExecutionPlanMetricsSet::new(),
            whole_text: options.whole_text,
            line_sep,
        }
    }
}

impl From<TextSource> for Arc<dyn FileSource> {
    fn from(source: TextSource) -> Self {
        as_file_source(source)
    }
}

impl FileSource for TextSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        partition_index: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        // The inner opener must emit batches whose schema is the file schema projected by
        // `file_indices` (that is what ProjectionOpener feeds its projector). For text that is
        // either the single `value` column or, for a projection-less count, an empty schema.
        let file_schema = self.table_schema.file_schema();
        let projected_file_schema = Arc::new(file_schema.project(&self.projection.file_indices)?);
        let opener = Arc::new(TextOpener {
            projected_file_schema,
            object_store,
            partition_index,
            batch_size: self.batch_size.unwrap_or(DEFAULT_TEXT_BATCH_SIZE),
            metrics: self.metrics.clone(),
            whole_text: self.whole_text,
            line_sep: self.line_sep.clone(),
        }) as Arc<dyn FileOpener>;
        ProjectionOpener::try_new(self.projection.clone(), opener, file_schema)
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.batch_size = Some(batch_size);
        Arc::new(conf)
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        let new_projection = self.projection.source.try_merge(projection)?;
        source.projection = SplitProjection::new(self.table_schema.file_schema(), &new_projection);
        Ok(Some(Arc::new(source)))
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection.source)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        "text"
    }

    // A custom line separator can straddle a byte-range boundary, and we read whole files rather
    // than boundary-aligned ranges, so this source must not let DataFusion repartition files by
    // byte range.
    fn supports_repartitioning(&self) -> bool {
        false
    }

    fn fmt_extra(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, ", whole_text={}", self.whole_text)
            }
            DisplayFormatType::TreeRender => Ok(()),
        }
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_plan::PhysicalExpr>,
        ) -> Result<datafusion::common::tree_node::TreeNodeRecursion>,
    ) -> Result<datafusion::common::tree_node::TreeNodeRecursion> {
        datafusion::physical_plan::apply_expression_roots(self.projection.source.iter(), f)
    }
}

/// A [`FileOpener`] that reads a whole text file and yields the projected file schema in batches.
struct TextOpener {
    // The file schema projected by `file_indices`: the single `value` column, or an empty schema
    // for a projection-less count.
    projected_file_schema: SchemaRef,
    object_store: Arc<dyn ObjectStore>,
    partition_index: usize,
    batch_size: usize,
    metrics: ExecutionPlanMetricsSet,
    whole_text: bool,
    line_sep: Option<Vec<u8>>,
}

impl FileOpener for TextOpener {
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        let store = Arc::clone(&self.object_store);
        let projected_file_schema = Arc::clone(&self.projected_file_schema);
        let batch_size = self.batch_size;
        let whole_text = self.whole_text;
        let line_sep = self.line_sep.clone();
        let baseline_metrics = BaselineMetrics::new(&self.metrics, self.partition_index);
        // Feeds the task's input bytes in the Spark UI (see CometMetricNode.reportScanInputMetrics).
        let bytes_scanned =
            MetricBuilder::new(&self.metrics).counter("bytes_scanned", self.partition_index);

        Ok(Box::pin(async move {
            let location = partitioned_file.object_meta.location;
            let buf: Bytes = store.get(&location).await?.bytes().await?;
            bytes_scanned.add(buf.len());

            // NOTE: the whole object is read into memory. CometScanRule only admits unsplit,
            // uncompressed files under a size cap, so in practice this reads each small file once.
            // Lines are found and batches built on demand, so only the bytes and the current
            // batch are held.
            //
            // Spark's line reader (Hadoop LineRecordReader.skipUtfByteOrderMark) strips a leading
            // UTF-8 BOM from the first record, whatever the separator. wholetext keeps it.
            let reader = TextBatchReader::new(
                buf,
                projected_file_schema,
                batch_size,
                TextLayout {
                    whole_text,
                    line_sep,
                    strip_bom: !whole_text,
                },
            );

            Ok(futures::stream::unfold(
                (reader, baseline_metrics),
                |(mut reader, metrics)| async move {
                    // The timer covers only building this batch, not the wait for the next poll.
                    let next = {
                        let _timer = metrics.elapsed_compute().timer();
                        reader.next_batch()
                    };
                    match next {
                        Ok(Some(batch)) => Some((Ok(batch), (reader, metrics))),
                        Ok(None) => None,
                        Err(e) => {
                            // Report the error once, then end the stream.
                            reader.finished = true;
                            Some((Err(e), (reader, metrics)))
                        }
                    }
                },
            )
            .boxed())
        }))
    }
}

/// How the bytes of one file split into rows.
struct TextLayout {
    /// One row for the whole file, separators included.
    whole_text: bool,
    /// A custom line separator's exact bytes, or `None` for the universal newline set.
    line_sep: Option<Vec<u8>>,
    /// Strip a leading UTF-8 BOM from the first record (line mode).
    strip_bom: bool,
}

/// Reads a file's bytes as batches of `value` rows, one batch per call.
///
/// Lines are found by scanning forward from `pos`, so nothing is kept per line: the memory beyond
/// the file bytes is the batch being built. A file of nothing but newlines would otherwise need a
/// slice descriptor per byte.
struct TextBatchReader {
    buf: Bytes,
    schema: SchemaRef,
    batch_rows: usize,
    layout: TextLayout,
    /// Offset of the next unread byte in line mode.
    pos: usize,
    /// wholetext: the single row has been returned.
    whole_taken: bool,
    /// The first-record BOM check has not run yet.
    first_record: bool,
    /// A row read but held back because it would have overflowed the current batch.
    pending: Option<(usize, usize)>,
    finished: bool,
}

impl TextBatchReader {
    fn new(buf: Bytes, schema: SchemaRef, batch_size: usize, layout: TextLayout) -> Self {
        let first_record = layout.strip_bom;
        Self {
            buf,
            schema,
            batch_rows: batch_size.max(1),
            layout,
            pos: 0,
            whole_taken: false,
            first_record,
            pending: None,
            finished: false,
        }
    }

    /// Offsets `(start, end)` of the next line, terminator excluded, or `None` at the end.
    ///
    /// Default separators are `\n`, `\r\n` and `\r`. A custom separator is an exact byte match. A
    /// trailing terminator does not start another, empty line (Hadoop LineRecordReader).
    fn scan_line(&mut self) -> Option<(usize, usize)> {
        let buf = &self.buf[..];
        if self.layout.whole_text {
            if self.whole_taken {
                return None;
            }
            self.whole_taken = true;
            return Some((0, buf.len()));
        }
        if self.pos >= buf.len() {
            return None;
        }
        let start = self.pos;
        match &self.layout.line_sep {
            None => match buf[start..].iter().position(|&b| b == b'\n' || b == b'\r') {
                None => {
                    self.pos = buf.len();
                    Some((start, buf.len()))
                }
                Some(i) => {
                    let end = start + i;
                    self.pos = if buf[end] == b'\r' && buf.get(end + 1) == Some(&b'\n') {
                        end + 2
                    } else {
                        end + 1
                    };
                    Some((start, end))
                }
            },
            Some(sep) => match find_subslice(&buf[start..], sep) {
                None => {
                    self.pos = buf.len();
                    Some((start, buf.len()))
                }
                Some(i) => {
                    let end = start + i;
                    self.pos = end + sep.len();
                    Some((start, end))
                }
            },
        }
    }

    /// Offsets of the next row to emit, after the first-record BOM strip.
    fn next_row(&mut self) -> Option<(usize, usize)> {
        if let Some(row) = self.pending.take() {
            return Some(row);
        }
        let (start, end) = self.scan_line()?;
        let mut row_start = start;
        // Hadoop reads the first record and then drops the BOM from its text. When that leaves
        // nothing and the record was not terminated (the file is only the BOM), it counts as end
        // of input, so the file has no rows. A BOM followed by a separator leaves an empty row. A
        // custom `lineSep` equal to the BOM never reaches the strip, because the BOM is then the
        // separator and the first record does not start with it.
        if self.first_record {
            self.first_record = false;
            if self.buf[start..end].starts_with(UTF8_BOM) {
                if self.buf.len() == UTF8_BOM.len() {
                    return None;
                }
                row_start = start + UTF8_BOM.len();
            }
        }
        Some((row_start, end))
    }

    /// The next batch of up to `batch_size` rows, or `None` when the file is exhausted.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.finished {
            return Ok(None);
        }
        if self.schema.fields().is_empty() {
            // The count(*) case: rows with no columns. Count lines without decoding them.
            let mut rows = 0;
            while rows < self.batch_rows && self.next_row().is_some() {
                rows += 1;
            }
            if rows == 0 {
                self.finished = true;
                return Ok(None);
            }
            let options = RecordBatchOptions::new().with_row_count(Some(rows));
            return Ok(Some(RecordBatch::try_new_with_options(
                Arc::clone(&self.schema),
                vec![],
                &options,
            )?));
        }

        // Decode each line once (decode_utf8_spark_lossy borrows for valid UTF-8, allocates only
        // for ill-formed input) and append it straight to the builder. Count the DECODED size,
        // since a lossy decode can expand ill-formed bytes up to 3x, and start a new batch before
        // the value buffer would reach Arrow's 32-bit offset limit. Always take at least one line
        // so progress is guaranteed; if a single decoded value alone would overflow, fail cleanly
        // rather than panic in Arrow. A handle to the bytes lets the decoded value borrow them
        // while the reader advances.
        let buf = self.buf.clone();
        let mut builder = StringBuilder::with_capacity(self.batch_rows.min(buf.len().max(1)), 0);
        let mut rows = 0usize;
        let mut data_bytes = 0usize;
        while rows < self.batch_rows {
            let (start, end) = match self.next_row() {
                Some(row) => row,
                None => break,
            };
            let value = decode_utf8_spark_lossy(&buf[start..end]);
            let len = value.len();
            if len > i32::MAX as usize {
                return Err(datafusion::common::DataFusionError::Execution(format!(
                    "Text value of {len} bytes exceeds Arrow's 2GB string-offset limit"
                )));
            }
            if rows > 0 && data_bytes + len > MAX_BATCH_VALUE_BYTES {
                self.pending = Some((start, end));
                break;
            }
            data_bytes += len;
            builder.append_value(value);
            rows += 1;
        }
        if rows == 0 {
            self.finished = true;
            return Ok(None);
        }
        Ok(Some(RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![Arc::new(builder.finish())],
        )?))
    }
}

/// Offset of the first occurrence of `needle` in `haystack`.
fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn value_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]))
    }

    /// Read `buf` the way the opener does and return every batch.
    fn read_batches(
        buf: &[u8],
        schema: SchemaRef,
        batch_size: usize,
        layout: TextLayout,
    ) -> Vec<RecordBatch> {
        let mut reader =
            TextBatchReader::new(Bytes::copy_from_slice(buf), schema, batch_size, layout);
        let mut batches = Vec::new();
        while let Some(batch) = reader.next_batch().unwrap() {
            batches.push(batch);
        }
        batches
    }

    fn layout(whole_text: bool, line_sep: Option<&[u8]>) -> TextLayout {
        TextLayout {
            whole_text,
            line_sep: line_sep.map(|s| s.to_vec()),
            strip_bom: false,
        }
    }

    fn values(batches: &[RecordBatch]) -> Vec<String> {
        batches
            .iter()
            .flat_map(|batch| {
                let col = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                (0..col.len())
                    .map(|i| col.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn lines_as_str(buf: &[u8], whole_text: bool, line_sep: Option<&[u8]>) -> Vec<String> {
        values(&read_batches(
            buf,
            value_schema(),
            8192,
            layout(whole_text, line_sep),
        ))
    }

    #[test]
    fn universal_newline_variants() {
        // \n, \r\n, and \r all split; the terminator is stripped.
        assert_eq!(
            lines_as_str(b"a\nb\r\nc\rd", false, None),
            vec!["a", "b", "c", "d"]
        );
    }

    #[test]
    fn trailing_terminator_no_empty_line() {
        assert_eq!(lines_as_str(b"a\nb\n", false, None), vec!["a", "b"]);
        assert_eq!(lines_as_str(b"a\r\n", false, None), vec!["a"]);
        assert_eq!(lines_as_str(b"a\r", false, None), vec!["a"]);
    }

    #[test]
    fn empty_interior_lines_are_kept() {
        assert_eq!(lines_as_str(b"a\n\nb", false, None), vec!["a", "", "b"]);
        // A lone newline is one empty record.
        assert_eq!(lines_as_str(b"\n", false, None), vec![""]);
    }

    #[test]
    fn empty_buffer_has_no_lines() {
        assert!(lines_as_str(b"", false, None).is_empty());
    }

    #[test]
    fn no_terminator_is_single_line() {
        assert_eq!(
            lines_as_str(b"only one line", false, None),
            vec!["only one line"]
        );
    }

    #[test]
    fn custom_single_char_separator() {
        // A comma and a bare newline are literal content when lineSep is '|'.
        assert_eq!(
            lines_as_str(b"a,b|c\nd|e", false, Some(b"|")),
            vec!["a,b", "c\nd", "e"]
        );
        // Trailing separator does not add an empty line.
        assert_eq!(lines_as_str(b"a|b|", false, Some(b"|")), vec!["a", "b"]);
    }

    #[test]
    fn custom_multi_byte_separator() {
        assert_eq!(
            lines_as_str(b"a<>b<>c", false, Some(b"<>")),
            vec!["a", "b", "c"]
        );
    }

    #[test]
    fn whole_text_is_one_row_with_separators_preserved() {
        assert_eq!(lines_as_str(b"a\nb\nc", true, None), vec!["a\nb\nc"]);
    }

    #[test]
    fn non_ascii_is_preserved() {
        assert_eq!(
            lines_as_str("café\n日本語".as_bytes(), false, None),
            vec!["café", "日本語"]
        );
    }

    #[test]
    fn batches_split_rows_by_batch_size() {
        let batches = read_batches(b"x\ny\nz", value_schema(), 2, layout(false, None));
        // batch_size 2 -> two batches (2 rows, 1 row).
        assert_eq!(batches.len(), 2);
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
        assert_eq!(values(&batches), vec!["x", "y", "z"]);
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn empty_projection_keeps_row_count() {
        // The count(*) case: an empty projected schema, rows preserved with no columns.
        let batches = read_batches(
            b"x\ny\nz",
            Arc::new(Schema::empty()),
            8192,
            layout(false, None),
        );
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_columns(), 0);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[test]
    fn newline_only_input_counts_rows_in_bounded_batches() {
        // A file of only newlines has one empty row per byte. Counting must not need a table
        // that grows with the input: the rows come out in batches of at most batch_size.
        let buf = vec![b'\n'; 100_000];
        let batches = read_batches(&buf, Arc::new(Schema::empty()), 8192, layout(false, None));
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 100_000);
        assert!(batches.iter().all(|b| b.num_rows() <= 8192));
    }

    #[test]
    fn reader_returns_one_batch_per_call() {
        // Batches are built on demand: after the first call the reader still holds the rest.
        let mut reader = TextBatchReader::new(
            Bytes::from_static(b"a\nb\nc\nd\ne"),
            value_schema(),
            2,
            layout(false, None),
        );
        assert_eq!(
            values(&[reader.next_batch().unwrap().unwrap()]),
            vec!["a", "b"]
        );
        assert_eq!(
            values(&[reader.next_batch().unwrap().unwrap()]),
            vec!["c", "d"]
        );
        assert_eq!(values(&[reader.next_batch().unwrap().unwrap()]), vec!["e"]);
        assert!(reader.next_batch().unwrap().is_none());
        assert!(reader.next_batch().unwrap().is_none());
    }

    /// Read with the first-record BOM strip, as the opener sets it (line mode only).
    fn lines_after_bom(buf: &[u8], whole_text: bool, line_sep: Option<&[u8]>) -> Vec<String> {
        let mut layout = layout(whole_text, line_sep);
        layout.strip_bom = !whole_text;
        values(&read_batches(buf, value_schema(), 8192, layout))
    }

    #[test]
    fn strips_first_record_bom_in_line_mode() {
        let bom = [0xEFu8, 0xBB, 0xBF];
        let with_hello = [&bom[..], b"hello"].concat();
        // Default separator at file start: the BOM is stripped.
        assert_eq!(lines_after_bom(&with_hello, false, None), vec!["hello"]);
        // BOM then newline -> ["", "hello"].
        let bom_nl = [&bom[..], b"\nhello"].concat();
        assert_eq!(lines_after_bom(&bom_nl, false, None), vec!["", "hello"]);
        // BOM-only file, default separator -> no rows.
        assert!(lines_after_bom(&bom, false, None).is_empty());
        // BOM only plus its terminator -> one empty row.
        let bom_only_nl = [&bom[..], b"\n"].concat();
        assert_eq!(lines_after_bom(&bom_only_nl, false, None), vec![""]);
        // Custom separator: the BOM is still stripped from the first record.
        let bom_a_b = [&bom[..], b"a|b"].concat();
        assert_eq!(lines_after_bom(&bom_a_b, false, Some(b"|")), vec!["a", "b"]);
        // BOM-only file with a custom separator -> no rows.
        assert!(lines_after_bom(&bom, false, Some(b"|")).is_empty());
        // BOM then separator -> an empty row, then the rest.
        let bom_sep_x = [&bom[..], b"|x"].concat();
        assert_eq!(
            lines_after_bom(&bom_sep_x, false, Some(b"|")),
            vec!["", "x"]
        );
        // lineSep equal to the BOM: the BOM is the separator, so the first record does not start
        // with it and nothing is stripped. `BOM a BOM b` -> ["", "a", "b"].
        let bom_a_bom_b = [&bom[..], b"a", &bom[..], b"b"].concat();
        assert_eq!(
            lines_after_bom(&bom_a_bom_b, false, Some(&bom)),
            vec!["", "a", "b"]
        );
        // BOM-only file with lineSep=BOM -> one empty record (trailing separator dropped).
        assert_eq!(lines_after_bom(&bom, false, Some(&bom)), vec![""]);
        // Not stripped: wholetext keeps the BOM, and a file with no BOM is unchanged.
        assert_eq!(
            lines_after_bom(&with_hello, true, None).concat().as_bytes(),
            &with_hello[..]
        );
        assert_eq!(lines_after_bom(b"hello", false, None), vec!["hello"]);
    }

    #[test]
    fn invalid_utf8_is_replaced() {
        // Ill-formed bytes become U+FFFD (matches Spark's new String(bytes, UTF_8) on collect).
        let bad: &[u8] = &[b'a', 0xff, b'b'];
        assert_eq!(lines_as_str(bad, false, None), vec!["a\u{FFFD}b"]);
    }
}
