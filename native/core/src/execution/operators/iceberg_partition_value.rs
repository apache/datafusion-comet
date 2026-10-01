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

//! Partition-value computation for the native Iceberg writer.
//!
//! [`PartitionValueCalculator`] stands in for iceberg-rust's calculator of the same name. It
//! projects each partition field's source column and applies the field's transform the same way,
//! but computes the time transforms of dates and timestamps with Comet's own kernels, so that they
//! get iceberg-java's partition values where iceberg-rust's differ.

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, StructArray};
use arrow::datatypes::{DataType, Fields};
use datafusion_comet_spark_expr::SparkIcebergTemporalTransform;
use iceberg::arrow::record_batch_projector::RecordBatchProjector;
use iceberg::arrow::type_to_arrow_type;
use iceberg::spec::{PartitionSpec, PrimitiveType, SchemaRef, StructType, Transform, Type};
use iceberg::transform::{create_transform_function, BoxedTransformFunction};
use iceberg::{Error, ErrorKind, Result};

/// Computes a batch's partition values: one row of the partition struct per input row.
///
/// Matches iceberg-rust's `PartitionValueCalculator` except for the time transforms of a `date`,
/// `timestamp`, or `timestamptz` source, which go through Comet's `iceberg_years` /
/// `iceberg_months` / `iceberg_days` / `iceberg_hours` kernels instead: the ones the sort in front
/// of a clustered write runs, pinned against iceberg-java's `DateTimeUtil` over the whole domain.
/// iceberg-rust's transforms differ from iceberg-java's in three ways:
///
/// - `year` and `month` split the calendar with Arrow's `date_part`, which returns NULL for
///   anything `chrono` cannot represent -- past year 262142 -- whereas iceberg-java goes through
///   `LocalDate` and covers every Spark date (to year 5881580) and timestamp (to year 294247). The
///   NULL did not fail the write: the data file was committed claiming a NULL partition for rows
///   whose source value is not NULL (apache/datafusion-comet#6145).
/// - All four floor a pre-epoch timestamp that lies exactly 999999 microseconds into a unit, which
///   iceberg-java puts in the unit before, so `1969-01-01T00:00:00.999999` belongs in the 1968
///   partitions (apache/datafusion-comet#6426).
/// - `day` moves a timestamp from the last second of a day before 1969-12-31 into the next day,
///   unless its microsecond of second is 0 or 999999: it takes the whole seconds with a truncating
///   division and the microseconds with a flooring one (apache/iceberg-rust#3315).
///
/// A partition value that differs from the sort key can also fail a clustered write, which rejects
/// a row whose partition it has already closed. Everywhere else the two implementations agree, so
/// no other value changes.
///
/// `day` of a `date` is the date itself and stays on iceberg-rust. So do the nanosecond timestamp
/// types, whose `i64` range (years 1677 to 2262) lies inside `chrono`'s and which Comet's kernels
/// do not accept.
pub(crate) struct PartitionValueCalculator {
    projector: RecordBatchProjector,
    transforms: Vec<FieldTransform>,
    partition_type: StructType,
    /// `partition_type` as Arrow fields, which every batch's partition struct is built from.
    fields: Fields,
}

impl PartitionValueCalculator {
    pub(crate) fn try_new(partition_spec: &PartitionSpec, schema: &SchemaRef) -> Result<Self> {
        let transforms = partition_spec
            .fields()
            .iter()
            .map(|field| {
                let source_type = schema
                    .field_by_id(field.source_id)
                    .map(|source| source.field_type.as_ref());
                FieldTransform::try_new(field.transform, source_type)
            })
            .collect::<Result<Vec<_>>>()?;
        let source_ids: Vec<i32> = partition_spec
            .fields()
            .iter()
            .map(|field| field.source_id)
            .collect();
        let projector = RecordBatchProjector::from_iceberg_schema(Arc::clone(schema), &source_ids)?;
        let partition_type = partition_spec.partition_type(schema)?;
        let DataType::Struct(fields) = type_to_arrow_type(&Type::Struct(partition_type.clone()))?
        else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Expected partition type must be a struct",
            ));
        };
        Ok(Self {
            projector,
            transforms,
            partition_type,
            fields,
        })
    }

    /// The spec's partition type, resolved against the schema.
    pub(crate) fn partition_type(&self) -> &StructType {
        &self.partition_type
    }

    /// The partition values of `batch`, as a `StructArray` of the spec's partition type.
    pub(crate) fn calculate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let values = self
            .projector
            .project_column(batch.columns())?
            .iter()
            .zip(&self.transforms)
            .map(|(source, transform)| transform.apply(source))
            .collect::<Result<Vec<_>>>()?;
        let partition_values =
            StructArray::try_new(self.fields.clone(), values, None).map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Failed to create partition struct array: {e}"),
                )
            })?;
        Ok(Arc::new(partition_values))
    }
}

/// How one partition field's value is computed from its source column.
#[derive(Debug)]
enum FieldTransform {
    IcebergRust(BoxedTransformFunction),
    Comet(SparkIcebergTemporalTransform),
}

impl FieldTransform {
    fn try_new(transform: Transform, source_type: Option<&Type>) -> Result<Self> {
        let timestamp_source = matches!(
            source_type,
            Some(Type::Primitive(
                PrimitiveType::Timestamp | PrimitiveType::Timestamptz
            ))
        );
        let calendar_source =
            timestamp_source || matches!(source_type, Some(Type::Primitive(PrimitiveType::Date)));
        Ok(match transform {
            Transform::Year if calendar_source => {
                Self::Comet(SparkIcebergTemporalTransform::years())
            }
            Transform::Month if calendar_source => {
                Self::Comet(SparkIcebergTemporalTransform::months())
            }
            Transform::Day if timestamp_source => {
                Self::Comet(SparkIcebergTemporalTransform::days())
            }
            Transform::Hour if timestamp_source => {
                Self::Comet(SparkIcebergTemporalTransform::hours())
            }
            _ => Self::IcebergRust(create_transform_function(&transform)?),
        })
    }

    fn apply(&self, source: &ArrayRef) -> Result<ArrayRef> {
        match self {
            Self::IcebergRust(function) => function.transform(Arc::clone(source)),
            Self::Comet(kernel) => kernel.transform(source).map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to apply a partition transform",
                )
                .with_source(e)
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::temporal_conversions::timestamp_us_to_datetime;
    use arrow::array::{
        Array, AsArray, Date32Array, Int32Array, Int64Array, StringArray,
        TimestampMicrosecondArray, TimestampNanosecondArray,
    };
    use arrow::datatypes::Date32Type;
    use iceberg::arrow::arrow_type_to_type;
    use iceberg::spec::{NestedField, Schema};

    use super::*;

    /// The first and last days `chrono` -- and so Arrow's `date_part` -- can represent,
    /// -262143-01-01 and +262142-12-31, as epoch days.
    const CHRONO_MIN_DAY: i32 = -96_465_292;
    const CHRONO_MAX_DAY: i32 = 95_026_236;
    /// The first and last microseconds of those two days.
    const CHRONO_MIN_MICROS: i64 = -8_334_601_228_800_000_000;
    const CHRONO_MAX_MICROS: i64 = 8_210_266_876_799_999_999;

    /// One step outside `chrono`'s calendar on either side, the
    /// `timestamp_micros(9000000000000000000)` of the issue, and both ends of Spark's timestamp
    /// domain.
    const INSTANTS_PAST_CHRONO: [Option<i64>; 6] = [
        Some(CHRONO_MIN_MICROS - 1),
        Some(CHRONO_MAX_MICROS + 1),
        Some(9_000_000_000_000_000_000),
        Some(i64::MAX),
        Some(i64::MIN),
        None,
    ];

    #[test]
    fn chrono_bounds_are_where_chrono_stops() {
        assert!(Date32Type::to_naive_date_opt(CHRONO_MIN_DAY).is_some());
        assert!(Date32Type::to_naive_date_opt(CHRONO_MIN_DAY - 1).is_none());
        assert!(Date32Type::to_naive_date_opt(CHRONO_MAX_DAY).is_some());
        assert!(Date32Type::to_naive_date_opt(CHRONO_MAX_DAY + 1).is_none());
        assert!(timestamp_us_to_datetime(CHRONO_MIN_MICROS).is_some());
        assert!(timestamp_us_to_datetime(CHRONO_MIN_MICROS - 1).is_none());
        assert!(timestamp_us_to_datetime(CHRONO_MAX_MICROS).is_some());
        assert!(timestamp_us_to_datetime(CHRONO_MAX_MICROS + 1).is_none());
    }

    /// Comet's and iceberg-rust's calculators for a spec with one partition field per
    /// `(transform, column)`, and the batch of those columns. Each field gets a source column of
    /// its own, typed after the column's Arrow type: a spec takes at most one time transform per
    /// source column, which iceberg-java enforces too.
    fn calculators(
        fields: &[(Transform, ArrayRef)],
    ) -> (
        PartitionValueCalculator,
        iceberg::arrow::PartitionValueCalculator,
        RecordBatch,
    ) {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(fields.iter().enumerate().map(|(i, (_, column))| {
                    let source_type = arrow_type_to_type(column.data_type()).unwrap();
                    NestedField::optional(i as i32 + 1, format!("c{i}"), source_type).into()
                }))
                .build()
                .unwrap(),
        );
        let spec = fields
            .iter()
            .enumerate()
            .fold(
                PartitionSpec::builder(Arc::clone(&schema)),
                |builder, (i, (transform, _))| {
                    builder
                        .add_partition_field(format!("c{i}"), format!("p{i}"), *transform)
                        .unwrap()
                },
            )
            .build()
            .unwrap();
        let batch = RecordBatch::try_from_iter(
            fields
                .iter()
                .enumerate()
                .map(|(i, (_, column))| (format!("c{i}"), Arc::clone(column))),
        )
        .unwrap();
        (
            PartitionValueCalculator::try_new(&spec, &schema).unwrap(),
            iceberg::arrow::PartitionValueCalculator::try_new(&spec, &schema).unwrap(),
            batch,
        )
    }

    /// A calculator's output, one column per partition field.
    fn columns(partition_values: ArrayRef) -> Vec<ArrayRef> {
        partition_values.as_struct().columns().to_vec()
    }

    fn dates(values: &[Option<i32>]) -> ArrayRef {
        Arc::new(Date32Array::from(values.to_vec()))
    }

    fn micros(values: &[Option<i64>]) -> ArrayRef {
        Arc::new(TimestampMicrosecondArray::from(values.to_vec()))
    }

    /// The writer casts every batch to `schema_to_arrow_schema`, which tags `timestamptz` `+00:00`.
    fn micros_utc(values: &[Option<i64>]) -> ArrayRef {
        Arc::new(TimestampMicrosecondArray::from(values.to_vec()).with_timezone("+00:00"))
    }

    fn ints(values: &[i32]) -> Int32Array {
        values.iter().copied().map(Some).chain([None]).collect()
    }

    // Expectations from iceberg-java 1.11's `Transforms.year()` / `Transforms.month()` bound to
    // `date` and `timestamptz`, run on a JDK 17 JVM. They hold for every Iceberg version Comet
    // supports: `DateTimeUtil` has converted through `LocalDate` since before 1.5.
    #[test]
    fn years_and_months_past_chronos_calendar_match_iceberg_java() {
        // One step outside `chrono`'s calendar on either side, the `date_from_unix_date(100000000)`
        // of the issue, and both ends of Spark's date domain.
        let days = dates(&[
            Some(CHRONO_MIN_DAY - 1),
            Some(CHRONO_MAX_DAY + 1),
            Some(100_000_000),
            Some(i32::MAX),
            Some(i32::MIN),
            None,
        ]);
        let instants = micros(&INSTANTS_PAST_CHRONO);
        let instants_utc = micros_utc(&INSTANTS_PAST_CHRONO);
        let date_years = ints(&[-264_114, 260_173, 273_790, 5_879_610, -5_879_611]);
        let date_months = ints(&[-3_169_357, 3_122_076, 3_285_488, 70_555_326, -70_555_327]);
        let instant_years = ints(&[-264_114, 260_173, 285_198, 292_277, -292_278]);
        let instant_months = ints(&[-3_169_357, 3_122_076, 3_422_383, 3_507_324, -3_507_325]);
        let cases = [
            (Transform::Year, &days, &date_years),
            (Transform::Month, &days, &date_months),
            (Transform::Year, &instants_utc, &instant_years),
            (Transform::Month, &instants_utc, &instant_months),
            (Transform::Year, &instants, &instant_years),
            (Transform::Month, &instants, &instant_months),
        ];
        let (comet, iceberg_rust, batch) =
            calculators(&cases.map(|(transform, source, _)| (transform, Arc::clone(source))));

        for (column, (transform, source, expected)) in
            columns(comet.calculate(&batch).unwrap()).iter().zip(&cases)
        {
            assert_eq!(
                column.as_primitive(),
                *expected,
                "{transform} of {}",
                source.data_type()
            );
        }
        // The reason Comet computes these two for dates, and one reason for timestamps (see
        // `pre_epoch_timestamps_partition_like_iceberg_java` for the other): iceberg-rust's
        // transforms turn every value above into a NULL partition value. If this starts failing,
        // iceberg-rust has learned the whole domain and delegating dates becomes an option again.
        for (column, (transform, source, _)) in columns(iceberg_rust.calculate(&batch).unwrap())
            .iter()
            .zip(&cases)
        {
            assert_eq!(
                column.null_count(),
                column.len(),
                "{transform} of {}",
                source.data_type()
            );
        }
    }

    // iceberg-rust's `day` and `hour` already agreed with iceberg-java this far out, and the Comet
    // kernels that now compute them for timestamps still do (same JVM run as above). `day` of a
    // date is the date itself, so only timestamps are interesting.
    #[test]
    fn days_and_hours_past_chronos_calendar_match_iceberg_java() {
        let instants = micros_utc(&INSTANTS_PAST_CHRONO);
        let (comet, _, batch) = calculators(&[
            (Transform::Day, Arc::clone(&instants)),
            (Transform::Hour, instants),
        ]);
        let values = columns(comet.calculate(&batch).unwrap());
        assert_eq!(
            values[0].as_primitive::<Date32Type>(),
            &Date32Array::from(vec![
                Some(CHRONO_MIN_DAY - 1),
                Some(CHRONO_MAX_DAY + 1),
                Some(104_166_666),
                Some(106_751_991),
                Some(-106_751_992),
                None
            ])
        );
        // Java narrows the hour count with an `(int)` cast, which wraps this far out.
        assert_eq!(
            values[1].as_primitive(),
            &ints(&[
                1_979_800_287,
                -2_014_337_608,
                -1_794_967_296,
                -1_732_919_508,
                1_732_919_507
            ])
        );
    }

    // Expectations from iceberg-java 1.11's `DateTimeUtil` on a JDK 17 JVM; 1.5.2, 1.8.1, and
    // 1.10.0 agree.
    #[test]
    fn pre_epoch_timestamps_partition_like_iceberg_java() {
        let values = [
            // 1969-01-01T00:00:00.999999, which iceberg-java places by the second before it,
            // 1968-12-31T23:59:59 (apache/datafusion-comet#6426).
            Some(-31_535_999_000_001),
            // 1969-12-31T23:00:00.999999, where that moves only the hour.
            Some(-3_599_000_001),
            // 1969-12-30T23:59:59.5 and 1969-12-30T23:59:59.999998.
            Some(-86_400_500_000),
            Some(-86_400_000_002),
            None,
        ];
        let day = |days: [i32; 4]| -> ArrayRef {
            Arc::new(Date32Array::from_iter(
                days.into_iter().map(Some).chain([None]),
            ))
        };
        let int = |values: [i32; 4]| -> ArrayRef { Arc::new(ints(&values)) };
        // (transform, iceberg-java's partition values, iceberg-rust's)
        let cases = [
            (
                Transform::Year,
                int([-2, -1, -1, -1]),
                int([-1, -1, -1, -1]),
            ),
            (
                Transform::Month,
                int([-13, -1, -1, -1]),
                int([-12, -1, -1, -1]),
            ),
            (
                Transform::Day,
                day([-366, -1, -2, -2]),
                day([-365, -1, -1, -1]),
            ),
            (
                Transform::Hour,
                int([-8_761, -2, -25, -25]),
                int([-8_760, -1, -25, -25]),
            ),
        ];
        for source in [micros(&values), micros_utc(&values)] {
            let fields: Vec<_> = cases
                .iter()
                .map(|(transform, _, _)| (*transform, Arc::clone(&source)))
                .collect();
            let (comet, iceberg_rust, batch) = calculators(&fields);
            let comet = columns(comet.calculate(&batch).unwrap());
            let iceberg_rust = columns(iceberg_rust.calculate(&batch).unwrap());
            for (i, (transform, java, rust)) in cases.iter().enumerate() {
                let label = format!("{transform} of {}", source.data_type());
                assert_eq!(&comet[i], java, "{label}");
                // The reason Comet computes these: iceberg-rust floors the first two rows, and its
                // `day` moves the last two into 1969-12-31 (apache/iceberg-rust#3315). If this
                // starts failing, iceberg-rust's transforms have changed and delegating needs
                // another look.
                assert_eq!(&iceberg_rust[i], rust, "iceberg-rust's {label}");
            }
        }
    }

    /// Away from the values where iceberg-rust parts from iceberg-java (the NULLs past `chrono`'s
    /// calendar and the pre-epoch timestamps above), every value comes out as iceberg-rust
    /// computes it, up to both ends of `chrono`'s calendar, so what the native writer puts in a
    /// table now is consistent with what it put there before.
    #[test]
    fn agrees_with_iceberg_rust_wherever_chrono_can_represent_the_date() {
        let days = dates(&[
            Some(CHRONO_MIN_DAY),
            Some(-719_529), // -0001-12-31
            Some(-366),
            Some(-365),
            Some(-1),
            Some(0),
            Some(59),  // 1970-03-01
            Some(789), // 1972-02-29
            Some(17_486),
            Some(2_932_897), // +10000-01-01
            Some(CHRONO_MAX_DAY),
            None,
        ]);
        let instant_values = [
            Some(CHRONO_MIN_MICROS),
            Some(-719_529 * 86_400_000_000),
            Some(-86_400_000_001),
            Some(-3_600_000_001),
            Some(-1),
            Some(0),
            Some(1_510_871_468_000_000),
            Some(2_932_897 * 86_400_000_000 - 1),
            Some(CHRONO_MAX_MICROS),
            None,
            None,
            None,
        ];
        let instants = micros(&instant_values);
        let instants_utc = micros_utc(&instant_values);
        let rows = days.len() as i64;
        let longs: ArrayRef = Arc::new(Int64Array::from_iter(
            (0..rows).map(|i| Some(i * 1_000_003 - 5_000_000)),
        ));
        let strings: ArrayRef = Arc::new(StringArray::from_iter(
            (0..rows).map(|i| Some(format!("s{i}"))),
        ));
        let fields = [
            (Transform::Year, Arc::clone(&days)),
            (Transform::Month, Arc::clone(&days)),
            (Transform::Year, Arc::clone(&instants_utc)),
            (Transform::Month, Arc::clone(&instants)),
            (Transform::Day, Arc::clone(&days)),
            (Transform::Identity, days),
            (Transform::Day, Arc::clone(&instants_utc)),
            (Transform::Hour, instants),
            (Transform::Identity, instants_utc),
            (Transform::Bucket(16), Arc::clone(&longs)),
            (Transform::Truncate(10), Arc::clone(&longs)),
            (Transform::Truncate(2), strings),
            (Transform::Void, longs),
        ];
        let (comet, iceberg_rust, batch) = calculators(&fields);

        // The time transforms of dates and timestamps really do run through Comet's kernels, except
        // `day` of a date, and nothing else does.
        let on_comet = comet
            .transforms
            .iter()
            .map(|transform| matches!(transform, FieldTransform::Comet(_)))
            .collect::<Vec<_>>();
        assert_eq!(
            on_comet,
            [true, true, true, true, false, false, true, true, false, false, false, false, false]
        );

        for ((comet, iceberg_rust), (transform, source)) in
            columns(comet.calculate(&batch).unwrap())
                .iter()
                .zip(columns(iceberg_rust.calculate(&batch).unwrap()))
                .zip(&fields)
        {
            assert_eq!(
                comet,
                &iceberg_rust,
                "{transform} of {}",
                source.data_type()
            );
        }
    }

    /// Comet's kernels take dates and microsecond timestamps only, so the V3 nanosecond types keep
    /// iceberg-rust's transforms, whose `date_part` covers their whole range. The expected values
    /// are iceberg-java's again.
    #[test]
    fn nanosecond_timestamps_stay_on_iceberg_rust() {
        let nanos: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
            Some(i64::MIN),
            Some(-1),
            Some(0),
            Some(i64::MAX),
            None,
        ]));
        let (comet, _, batch) = calculators(&[
            (Transform::Year, Arc::clone(&nanos)),
            (Transform::Month, nanos),
        ]);
        assert!(comet
            .transforms
            .iter()
            .all(|transform| matches!(transform, FieldTransform::IcebergRust(_))));
        // 1677-09-21 and 2262-04-11: the calendar split is still real, not a NULL.
        let values = columns(comet.calculate(&batch).unwrap());
        assert_eq!(values[0].as_primitive(), &ints(&[-293, -1, 0, 292]));
        assert_eq!(values[1].as_primitive(), &ints(&[-3_508, -1, 0, 3_507]));
    }
}
