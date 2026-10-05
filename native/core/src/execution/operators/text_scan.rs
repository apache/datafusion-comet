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
use datafusion::common::Result;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion::physical_plan::DisplayFormatType;
use datafusion_comet_common::decode_utf8_spark_lossy;
use datafusion_comet_proto::spark_operator::TextOptions;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::projection::{ProjectionOpener, SplitProjection};
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::{as_file_source, PartitionedFile, TableSchema};
use futures::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt};
use std::borrow::Cow;
use std::fmt;
use std::io::Read;
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
    _partition_schema: SchemaRef,
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
        base_config: &FileScanConfig,
        partition_index: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        // The inner opener must emit batches whose schema is the file schema projected by
        // `file_indices` (that is what ProjectionOpener feeds its projector). For text that is
        // either the single `value` column or, for a projection-less count, an empty schema.
        let file_schema = self.table_schema.file_schema();
        let projected_file_schema = Arc::new(file_schema.project(&self.projection.file_indices)?);
        let opener = Arc::new(TextOpener {
            projected_file_schema,
            file_compression_type: base_config.file_compression_type,
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
    file_compression_type: FileCompressionType,
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
        let file_compression_type = self.file_compression_type;
        let batch_size = self.batch_size;
        let whole_text = self.whole_text;
        let line_sep = self.line_sep.clone();
        let baseline_metrics = BaselineMetrics::new(&self.metrics, self.partition_index);

        Ok(Box::pin(async move {
            let range = partitioned_file.range.clone();
            let location = partitioned_file.object_meta.location;
            let raw = store.get(&location).await?.bytes().await?;

            // Decompress only when the file is actually compressed. On Comet's plan config the file
            // is always UNCOMPRESSED (CometScanRule falls back for compressed files), so we borrow
            // the fetched bytes directly and avoid a full-file copy; the decompress arm is a
            // defensive handler should compression ever be plumbed through. A byte range never
            // coexists with decompression (compressed files are non-splittable).
            //
            // NOTE: the whole object is read into memory. CometScanRule restricts native text to
            // unsplit files (it falls back when Spark splits a file into byte ranges), so in
            // practice this reads each small file once; the range filter below is a correctness
            // safety net for any split that still reaches here.
            let buf: Cow<[u8]> = if file_compression_type.is_compressed() {
                let mut decoder = file_compression_type.convert_read(std::io::Cursor::new(raw))?;
                let mut decoded = Vec::new();
                decoder.read_to_end(&mut decoded)?;
                Cow::Owned(decoded)
            } else {
                Cow::Borrowed(raw.as_ref())
            };

            let mut timer = baseline_metrics.elapsed_compute().timer();
            // Spark's line reader (Hadoop LineRecordReader.skipUtfByteOrderMark) strips a leading
            // UTF-8 BOM at the start of the file. Mirror that in line mode for the split that starts
            // at file offset 0 (native only reads unsplit files, so start is always 0; a custom
            // lineSep still strips, matching Hadoop). wholetext keeps the BOM, as Spark's
            // WholeTextFileRecordReader does.
            let at_file_start = range.as_ref().map(|r| r.start == 0).unwrap_or(true);
            let content = strip_leading_bom(&buf, whole_text, at_file_start);
            let mut lines = split_lines(content, whole_text, line_sep.as_deref());
            // Spark may split an uncompressed file into byte ranges (TextScan.isSplitable). A line
            // belongs to the split whose [start, end) contains the line's start offset -- so each
            // line is emitted by exactly one split, with no duplication or gaps across splits. See
            // DataFusion's CsvOpener for the same rule. wholetext files are never split.
            if !whole_text {
                if let Some(range) = range {
                    lines = filter_lines_by_range(content, lines, range.start, range.end);
                }
            }
            let batches = build_batches(&projected_file_schema, lines, batch_size)?;
            timer.stop();

            Ok(futures::stream::iter(batches.into_iter().map(Ok)).boxed())
        }))
    }
}

/// Strip a single leading UTF-8 BOM, matching Hadoop LineRecordReader.skipUtfByteOrderMark: only in
/// line mode (not wholetext) and only for the split that starts at file offset 0.
fn strip_leading_bom(buf: &[u8], whole_text: bool, at_file_start: bool) -> &[u8] {
    if !whole_text && at_file_start && buf.starts_with(UTF8_BOM) {
        &buf[UTF8_BOM.len()..]
    } else {
        buf
    }
}

/// Split a decompressed file buffer into text lines following Spark's semantics.
fn split_lines<'a>(buf: &'a [u8], whole_text: bool, line_sep: Option<&[u8]>) -> Vec<&'a [u8]> {
    if whole_text {
        // One row per file; the whole content (including internal separators) is a single value.
        return vec![buf];
    }
    match line_sep {
        Some(sep) => split_on_separator(buf, sep),
        None => split_universal(buf),
    }
}

/// Universal-newline splitting: a line ends at `\n`, `\r\n`, or `\r`, and the terminator is
/// stripped. A trailing terminator does not produce a final empty line (Hadoop LineRecordReader).
fn split_universal(buf: &[u8]) -> Vec<&[u8]> {
    let mut lines = Vec::new();
    let mut start = 0;
    let mut i = 0;
    while i < buf.len() {
        match buf[i] {
            b'\n' => {
                lines.push(&buf[start..i]);
                i += 1;
                start = i;
            }
            b'\r' => {
                lines.push(&buf[start..i]);
                i += 1;
                if i < buf.len() && buf[i] == b'\n' {
                    i += 1;
                }
                start = i;
            }
            _ => i += 1,
        }
    }
    if start < buf.len() {
        lines.push(&buf[start..]);
    }
    lines
}

/// Split on an exact separator byte sequence; the trailing separator does not produce an empty
/// final line.
fn split_on_separator<'a>(buf: &'a [u8], sep: &[u8]) -> Vec<&'a [u8]> {
    let mut lines = Vec::new();
    let mut start = 0;
    let mut i = 0;
    while i + sep.len() <= buf.len() {
        if &buf[i..i + sep.len()] == sep {
            lines.push(&buf[start..i]);
            i += sep.len();
            start = i;
        } else {
            i += 1;
        }
    }
    if start < buf.len() {
        lines.push(&buf[start..]);
    }
    lines
}

/// Keep only the lines whose start offset falls in the split's byte range `[start, end)`.
/// `lines` must be sub-slices of `buf` (they are, as produced by the split helpers), so a line's
/// start offset is its pointer distance from the buffer start. The split helpers emit lines in
/// strictly increasing start-offset order, so the qualifying lines are a contiguous run; find its
/// bounds by binary search and keep that slice in place without a second allocation.
fn filter_lines_by_range<'a>(
    buf: &[u8],
    mut lines: Vec<&'a [u8]>,
    start: i64,
    end: i64,
) -> Vec<&'a [u8]> {
    let base = buf.as_ptr() as usize;
    let start = start.max(0) as usize;
    let end = end.max(0) as usize;
    let hi = lines.partition_point(|line| (line.as_ptr() as usize - base) < end);
    let lo = lines
        .partition_point(|line| (line.as_ptr() as usize - base) < start)
        .min(hi);
    lines.truncate(hi);
    lines.drain(..lo);
    lines
}

/// Build record batches from raw line bytes. When `projected_schema` has the `value` column, each
/// line becomes a string; when it is empty (a projection-less count), only the row count is kept.
///
/// Arrow strings must be valid UTF-8, so ill-formed bytes are replaced with U+FFFD via
/// `decode_utf8_spark_lossy`. Spark's text reader instead stores the raw line bytes and only
/// substitutes on decode, so a plain read + collect matches, but byte-level operations over
/// invalid UTF-8 (octet_length, cast to binary, byte-exact predicates) can differ. This is an
/// inherent limitation of representing text bytes as an Arrow string.
fn build_batches(
    projected_schema: &SchemaRef,
    lines: Vec<&[u8]>,
    batch_size: usize,
) -> Result<Vec<RecordBatch>> {
    let emit_value = !projected_schema.fields().is_empty();
    let batch_rows = batch_size.max(1);
    let mut batches = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let batch = if emit_value {
            // Decode each line once (decode_utf8_spark_lossy borrows for valid UTF-8, allocates only
            // for ill-formed input) and accumulate against the DECODED size, since a lossy decode can
            // expand ill-formed bytes up to 3x. Start a new batch before the value buffer would reach
            // Arrow's 32-bit offset limit. Always take at least one line so progress is guaranteed;
            // if a single decoded value alone would overflow, fail cleanly rather than panic in Arrow.
            let mut decoded: Vec<std::borrow::Cow<str>> = Vec::new();
            let mut data_bytes = 0usize;
            while i < lines.len() && decoded.len() < batch_rows {
                let value = decode_utf8_spark_lossy(lines[i]);
                let len = value.len();
                if len > i32::MAX as usize {
                    return Err(datafusion::common::DataFusionError::Execution(format!(
                        "Text value of {len} bytes exceeds Arrow's 2GB string-offset limit"
                    )));
                }
                if !decoded.is_empty() && data_bytes + len > MAX_BATCH_VALUE_BYTES {
                    break;
                }
                data_bytes += len;
                decoded.push(value);
                i += 1;
            }
            let mut builder = StringBuilder::with_capacity(decoded.len(), data_bytes);
            for value in &decoded {
                builder.append_value(value);
            }
            RecordBatch::try_new(
                Arc::clone(projected_schema),
                vec![Arc::new(builder.finish())],
            )?
        } else {
            let take = (lines.len() - i).min(batch_rows);
            let options = RecordBatchOptions::new().with_row_count(Some(take));
            i += take;
            RecordBatch::try_new_with_options(Arc::clone(projected_schema), vec![], &options)?
        };
        batches.push(batch);
    }
    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn lines_as_str(buf: &[u8], whole_text: bool, line_sep: Option<&[u8]>) -> Vec<String> {
        split_lines(buf, whole_text, line_sep)
            .iter()
            .map(|l| String::from_utf8_lossy(l).into_owned())
            .collect()
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
    fn build_batches_value_column() {
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
        let lines: Vec<&[u8]> = vec![b"x", b"y", b"z"];
        let batches = build_batches(&schema, lines, 2).unwrap();
        // batch_size 2 -> two batches (2 rows, 1 row).
        assert_eq!(batches.len(), 2);
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "x");
        assert_eq!(col.value(1), "y");
    }

    #[test]
    fn build_batches_empty_projection_keeps_row_count() {
        // The count(*) case: an empty projected schema, rows preserved with no columns.
        let schema = Arc::new(Schema::empty());
        let lines: Vec<&[u8]> = vec![b"x", b"y", b"z"];
        let batches = build_batches(&schema, lines, 8192).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_columns(), 0);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[test]
    fn strips_leading_utf8_bom_in_line_mode() {
        let bom = [0xEFu8, 0xBB, 0xBF];
        let with_hello = [&bom[..], b"hello"].concat();
        // Line mode at file start: the BOM is stripped.
        assert_eq!(strip_leading_bom(&with_hello, false, true), b"hello");
        // BOM then newline -> ["", "hello"] after split.
        let bom_nl = [&bom[..], b"\nhello"].concat();
        assert_eq!(
            lines_as_str(strip_leading_bom(&bom_nl, false, true), false, None),
            vec!["", "hello"]
        );
        // BOM-only file -> empty after strip -> zero lines.
        assert!(split_lines(strip_leading_bom(&bom, false, true), false, None).is_empty());
        // Not stripped: wholetext mode, a non-start split, or no BOM present.
        assert_eq!(strip_leading_bom(&with_hello, true, true), &with_hello[..]);
        assert_eq!(
            strip_leading_bom(&with_hello, false, false),
            &with_hello[..]
        );
        assert_eq!(strip_leading_bom(b"hello", false, true), b"hello");
    }

    #[test]
    fn build_batches_replaces_invalid_utf8() {
        // Ill-formed bytes become U+FFFD (matches Spark's new String(bytes, UTF_8) on collect).
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
        let bad: &[u8] = &[b'a', 0xff, b'b'];
        let lines: Vec<&[u8]> = vec![bad];
        let batches = build_batches(&schema, lines, 8192).unwrap();
        assert_eq!(batches.len(), 1);
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "a\u{FFFD}b");
    }

    #[test]
    fn range_split_partitions_lines_without_duplication() {
        // Two contiguous splits over one file must together yield every line exactly once.
        let buf = b"aaa\nbbb\nccc\nddd"; // offsets: aaa=0 bbb=4 ccc=8 ddd=12
        let lines = split_universal(buf);
        let first = filter_lines_by_range(buf, lines.clone(), 0, 6);
        let second = filter_lines_by_range(buf, lines.clone(), 6, 14);
        let as_str = |v: Vec<&[u8]>| {
            v.iter()
                .map(|l| String::from_utf8_lossy(l).into_owned())
                .collect::<Vec<_>>()
        };
        assert_eq!(as_str(first), vec!["aaa", "bbb"]);
        assert_eq!(as_str(second), vec!["ccc", "ddd"]);
    }

    #[test]
    fn range_split_line_straddling_boundary_belongs_to_first_split() {
        // A line whose start offset is in the first split but that extends past the split end is
        // read fully by the first split and NOT repeated by the next.
        let buf = b"abcdef\nghij"; // abcdef=0, ghij=7
        let lines = split_universal(buf);
        let first = filter_lines_by_range(buf, lines.clone(), 0, 3);
        let second = filter_lines_by_range(buf, lines.clone(), 3, 11);
        let as_str = |v: Vec<&[u8]>| {
            v.iter()
                .map(|l| String::from_utf8_lossy(l).into_owned())
                .collect::<Vec<_>>()
        };
        assert_eq!(as_str(first), vec!["abcdef"]);
        assert_eq!(as_str(second), vec!["ghij"]);
    }
}
