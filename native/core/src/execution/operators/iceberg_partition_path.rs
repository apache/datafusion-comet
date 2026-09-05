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

//! Data-file location generation for the native Iceberg writer.
//!
//! [`CometLocationGenerator`] stands in for iceberg-rust's `DefaultLocationGenerator`. It lays out
//! files identically (`{data_location}/{partition_path}/{file_name}`), but renders the partition
//! path itself so the directory names match iceberg-java's `PartitionSpec#partitionToPath` rather
//! than iceberg-rust's `PartitionSpec::partition_to_path`, which diverges from it -- and, for a
//! pre-1970 `timestamptz` value, panics.

use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use iceberg::spec::{
    Literal, PartitionKey, PartitionSpec, PrimitiveLiteral, PrimitiveType, SchemaRef, StructType,
    Transform, Type,
};
use iceberg::writer::file_writer::location_generator::LocationGenerator;
use itertools::Itertools;
use url::form_urlencoded;

const SECONDS_PER_DAY: i64 = 86_400;

/// `LocationGenerator` for Comet's native Iceberg writer.
///
/// `partition_type` is the write's partition spec resolved against its schema, computed once when
/// the writer stack is built. `LocationGenerator::generate_location` cannot fail, so resolving it up
/// front turns a spec that cannot be resolved into an error at task start instead of a panic on
/// every file (iceberg-rust's `PartitionSpec::partition_to_path` unwraps the same call inline).
#[derive(Clone, Debug)]
pub struct CometLocationGenerator {
    data_location: String,
    partition_type: Arc<StructType>,
}

impl CometLocationGenerator {
    /// `data_location` is used verbatim as the parent directory, matching
    /// `DefaultLocationGenerator::with_data_location`.
    pub fn try_new(
        data_location: String,
        partition_spec: &PartitionSpec,
        schema: &SchemaRef,
    ) -> Result<Self, iceberg::Error> {
        // An unpartitioned spec never contributes a partition directory (see
        // `generate_location`), so its partition type is never read. Skipping the resolution
        // matters: a V1 spec keeps a dropped partition field as a `void` transform, and that
        // field's source column may since have been dropped from the schema -- which
        // `PartitionSpec::partition_type` rejects even though nothing needs the answer.
        let partition_type = if partition_spec.is_unpartitioned() {
            StructType::new(vec![])
        } else {
            partition_spec.partition_type(schema)?
        };
        Ok(Self {
            data_location,
            partition_type: Arc::new(partition_type),
        })
    }

    /// Mirrors iceberg-java's `PartitionSpec#partitionToPath`: `name=value` pairs joined by `/`,
    /// with both halves form-urlencoded. `form_urlencoded` leaves exactly the byte set Java's
    /// `URLEncoder.encode(s, UTF_8)` leaves (`A-Za-z0-9`, `*`, `-`, `.`, `_`), maps space to `+`,
    /// and percent-encodes the rest with uppercase hex, so the two agree byte for byte.
    fn partition_to_path(&self, key: &PartitionKey) -> String {
        let fields = self.partition_type.fields();
        key.spec()
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                // Indexed rather than zipped so a spec/partition-type/value length disagreement
                // renders as "null" instead of panicking; the three are built from the same spec,
                // so they always line up in practice.
                let value = key.data().fields().get(index).and_then(Option::as_ref);
                let human = match fields.get(index) {
                    Some(nested) => human_string(&field.transform, &nested.field_type, value),
                    None => NULL.to_string(),
                };
                form_urlencoded::Serializer::new(String::new())
                    .append_pair(&field.name, &human)
                    .finish()
            })
            .join("/")
    }
}

impl LocationGenerator for CometLocationGenerator {
    fn generate_location(&self, partition_key: Option<&PartitionKey>, file_name: &str) -> String {
        // `is_effectively_none` is iceberg-rust's own predicate: no key, or a key whose spec is
        // unpartitioned (no fields, or every field a `void` transform). Matching it keeps the
        // layout decision -- partition directory or not -- identical to
        // `DefaultLocationGenerator`.
        if PartitionKey::is_effectively_none(partition_key) {
            format!("{}/{}", self.data_location, file_name)
        } else {
            format!(
                "{}/{}/{}",
                self.data_location,
                self.partition_to_path(partition_key.unwrap()),
                file_name
            )
        }
    }
}

const NULL: &str = "null";

/// The value half of one `name=value` partition-path pair, as iceberg-java's
/// `Transform#toHumanString(Type, T)` renders it.
///
/// Delegates to iceberg-rust's `Transform::to_human_string` and overrides only the arms where the
/// two disagree:
///
/// | Iceberg type       | iceberg-java                  | iceberg-rust                     |
/// |--------------------|-------------------------------|----------------------------------|
/// | `timestamp`        | `1969-12-31T23:59:58.5`       | `1969-12-31 23:59:58.500`        |
/// | `timestamptz`      | `1969-12-31T23:59:58.5+00:00` | panics for a negative value with a sub-second part; otherwise `1969-12-31 23:59:58.500 UTC` |
/// | `binary` / `fixed` | base64                        | uppercase hex                    |
///
/// The nanosecond timestamp types get the same treatment for the same reason. They are V3-only, so
/// `CometIcebergNativeWrite`'s format-version gate keeps them out of a native write today; the arms
/// exist so a future V3 write does not reintroduce the panic.
///
/// Known remaining divergence, deliberately left delegating: `float` and `double`. Java renders
/// them with `Float.toString`/`Double.toString` (always a fractional digit, `E` notation outside
/// `[1e-3, 1e7)`), Rust with its own shortest representation, so `1.0` becomes `1` and `1.0E20`
/// becomes `100000000000000000000`. Porting Java's algorithm is a much larger piece of work than a
/// partition directory name warrants -- Comet's `cast(float as string)` needs the same port -- and
/// unlike `timestamptz` it does not panic. Iceberg deprecated float/double partitioning in 1.3.
fn human_string(transform: &Transform, field_type: &Type, value: Option<&Literal>) -> String {
    // Java returns "null" for a null partition value regardless of transform or type, which also
    // covers every `void` field: `void` produces no value, so this is the only arm it reaches.
    let Some(primitive) = value.and_then(Literal::as_primitive_literal) else {
        return NULL.to_string();
    };

    // `year`/`month`/`day`/`hour` render the ordinal itself and never see a timestamp or binary
    // field type (their result types are `int` and `date`), so they cannot collide with the arms
    // below. iceberg-rust already mirrors `TransformUtil` for them.
    match (field_type.as_primitive_type(), &primitive) {
        (Some(PrimitiveType::Timestamp), PrimitiveLiteral::Long(micros)) => {
            iso_timestamp(*micros, 6, false)
        }
        (Some(PrimitiveType::Timestamptz), PrimitiveLiteral::Long(micros)) => {
            iso_timestamp(*micros, 6, true)
        }
        (Some(PrimitiveType::TimestampNs), PrimitiveLiteral::Long(nanos)) => {
            iso_timestamp(*nanos, 9, false)
        }
        (Some(PrimitiveType::TimestamptzNs), PrimitiveLiteral::Long(nanos)) => {
            iso_timestamp(*nanos, 9, true)
        }
        (
            Some(PrimitiveType::Binary | PrimitiveType::Fixed(_)),
            PrimitiveLiteral::Binary(bytes),
        ) => BASE64.encode(bytes),
        _ => transform.to_human_string(field_type, value),
    }
}

/// Renders a sub-second count since the Unix epoch the way iceberg-java's
/// `DateTimeUtil.microsToIsoTimestamp[tz]` / `nanosToIsoTimestamp[tz]` do:
/// `DateTimeFormatter.ISO_LOCAL_DATE_TIME` over the UTC `LocalDateTime`, optionally followed by the
/// fixed `+00:00` offset the timestamptz formatter appends (the offset is always UTC, so
/// `appendOffset("+HH:MM:ss", "+00:00")` always emits its no-offset text verbatim).
///
/// `subsecond_digits` is 6 for the microsecond types and 9 for the nanosecond ones.
///
/// `ISO_LOCAL_DATE_TIME` always prints the seconds field (it is available on a `LocalDateTime`, so
/// its optional section is never dropped) and prints a fraction only when the nanosecond field is
/// non-zero, with trailing zeros stripped -- `appendFraction(NANO_OF_SECOND, 0, 9, true)` output-
/// scales a `stripTrailingZeros`ed `BigDecimal`. Half a second is therefore `.5`, not `.500000`.
fn iso_timestamp(value: i64, subsecond_digits: usize, with_zone: bool) -> String {
    let per_second = 10i64.pow(subsecond_digits as u32);
    let seconds = value.div_euclid(per_second);
    let subsecond = value.rem_euclid(per_second);
    let (year, month, day) = civil_from_days(seconds.div_euclid(SECONDS_PER_DAY));
    let second_of_day = seconds.rem_euclid(SECONDS_PER_DAY);

    let mut out = iso_year(year);
    out.push_str(&format!(
        "-{month:02}-{day:02}T{:02}:{:02}:{:02}",
        second_of_day / 3_600,
        (second_of_day / 60) % 60,
        second_of_day % 60,
    ));
    if subsecond != 0 {
        out.push('.');
        let digits = format!("{subsecond:0subsecond_digits$}");
        out.push_str(digits.trim_end_matches('0'));
    }
    if with_zone {
        out.push_str("+00:00");
    }
    out
}

/// The year as `ISO_LOCAL_DATE` writes it: `appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD)`, so
/// four zero-padded digits inside `0..=9999`, a `+` sign above that, and `-` plus four zero-padded
/// digits below zero.
fn iso_year(year: i64) -> String {
    if (0..=9999).contains(&year) {
        format!("{year:04}")
    } else if year > 9999 {
        format!("+{year}")
    } else {
        format!("-{:04}", year.unsigned_abs())
    }
}

/// Splits a day count since 1970-01-01 into `(year, month, day)` in the proleptic Gregorian
/// calendar (Howard Hinnant's `civil_from_days`).
///
/// Hand-rolled rather than delegated to `chrono` because this must be total: every `i64` micros
/// value reaches a partition path, and `chrono`'s calendar stops at year 262143 while `i64` micros
/// reach year 292277. iceberg-java's `ChronoUnit.MICROS.addTo(EPOCH, micros)` has no such ceiling,
/// so a value past `chrono`'s range must still produce the same string, not an error we cannot
/// return from `generate_location` anyway.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    // Shift the epoch to 0000-03-01 so leap days land at the end of the 400-year era.
    let shifted = days + 719_468;
    let era = if shifted >= 0 {
        shifted
    } else {
        shifted - 146_096
    } / 146_097;
    let day_of_era = (shifted - era * 146_097) as u64; // [0, 146096]
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365; // [0, 399]
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100); // [0, 365]
    let march_month = (5 * day_of_year + 2) / 153; // [0, 11], 0 = March
    let day = (day_of_year - (153 * march_month + 2) / 5 + 1) as u32; // [1, 31]
    let month = if march_month < 10 {
        march_month + 3
    } else {
        march_month - 9
    } as u32; // [1, 12]
    let year = year_of_era as i64 + era * 400;
    (if month <= 2 { year + 1 } else { year }, month, day)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use iceberg::spec::{
        NestedField, PartitionSpec, Schema as IcebergSchema, Struct as IcebergStruct,
    };

    use super::*;

    const MICROS_PER_DAY: i64 = SECONDS_PER_DAY * 1_000_000;

    fn timestamptz(micros: i64) -> String {
        human_string(
            &Transform::Identity,
            &Type::Primitive(PrimitiveType::Timestamptz),
            Some(&Literal::Primitive(PrimitiveLiteral::Long(micros))),
        )
    }

    fn timestamp(micros: i64) -> String {
        human_string(
            &Transform::Identity,
            &Type::Primitive(PrimitiveType::Timestamp),
            Some(&Literal::Primitive(PrimitiveLiteral::Long(micros))),
        )
    }

    // The whole point of the timestamptz override: `microseconds_to_datetimetz` takes
    // `micros % 1_000_000` -- negative for a pre-epoch value -- casts it to `u32` and multiplies
    // by 1000, then unwraps the `None` that `DateTime::from_timestamp` returns for the resulting
    // out-of-range nanosecond count. Still present in iceberg-rust as of the pinned rev
    // (665c64e); `nanoseconds_to_datetimetz` has the same shape.
    #[test]
    fn renders_pre_epoch_timestamptz_that_upstream_panics_on() {
        assert_eq!(timestamptz(-1), "1969-12-31T23:59:59.999999+00:00");
        assert_eq!(timestamptz(-1_500_000), "1969-12-31T23:59:58.5+00:00");
        assert_eq!(
            timestamptz(-MICROS_PER_DAY - 1),
            "1969-12-30T23:59:59.999999+00:00"
        );
    }

    // Values from Iceberg's own `TestTransformUtil`/`DateTimeUtil` behaviour: seconds are always
    // printed, the fraction only when non-zero and with trailing zeros stripped.
    #[test]
    fn matches_java_iso_timestamp_formatting() {
        assert_eq!(timestamptz(0), "1970-01-01T00:00:00+00:00");
        assert_eq!(
            timestamptz(1_510_871_468_000_000),
            "2017-11-16T22:31:08+00:00"
        );
        assert_eq!(timestamptz(500_000), "1970-01-01T00:00:00.5+00:00");
        assert_eq!(timestamptz(100_000), "1970-01-01T00:00:00.1+00:00");
        assert_eq!(timestamptz(120_000), "1970-01-01T00:00:00.12+00:00");
        assert_eq!(timestamptz(123_456), "1970-01-01T00:00:00.123456+00:00");
        assert_eq!(timestamptz(1), "1970-01-01T00:00:00.000001+00:00");
        assert_eq!(timestamptz(10), "1970-01-01T00:00:00.00001+00:00");
        // Same rendering without the offset for the untagged type.
        assert_eq!(timestamp(0), "1970-01-01T00:00:00");
        assert_eq!(timestamp(-1_500_000), "1969-12-31T23:59:58.5");
        assert_eq!(timestamp(123_456), "1970-01-01T00:00:00.123456");
    }

    #[test]
    fn renders_nanosecond_timestamps() {
        let ntz = human_string(
            &Transform::Identity,
            &Type::Primitive(PrimitiveType::TimestampNs),
            Some(&Literal::Primitive(PrimitiveLiteral::Long(-1))),
        );
        assert_eq!(ntz, "1969-12-31T23:59:59.999999999");
        let tz = human_string(
            &Transform::Identity,
            &Type::Primitive(PrimitiveType::TimestamptzNs),
            Some(&Literal::Primitive(PrimitiveLiteral::Long(1_500_000_000))),
        );
        assert_eq!(tz, "1970-01-01T00:00:01.5+00:00");
    }

    // `appendValue(YEAR, 4, 10, EXCEEDS_PAD)`: zero-padded to four digits, `+` above 9999, `-`
    // plus four zero-padded digits below zero. Not `%04d`, which would render year -1 as "-001".
    #[test]
    fn renders_years_outside_the_four_digit_range() {
        assert_eq!(iso_year(0), "0000");
        assert_eq!(iso_year(1), "0001");
        assert_eq!(iso_year(9999), "9999");
        assert_eq!(iso_year(10_000), "+10000");
        assert_eq!(iso_year(-1), "-0001");
        assert_eq!(iso_year(-10_000), "-10000");
    }

    // `i64` micros reach year 292277, past `chrono`'s year-262143 ceiling; iceberg-java has no
    // such ceiling, so these must still render rather than fail.
    #[test]
    fn renders_timestamps_beyond_chronos_calendar() {
        assert_eq!(timestamptz(i64::MAX), "+294247-01-10T04:00:54.775807+00:00");
        assert_eq!(timestamptz(i64::MIN), "-290308-12-21T19:59:05.224192+00:00");
    }

    #[test]
    fn civil_from_days_matches_the_gregorian_calendar() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(-1), (1969, 12, 31));
        assert_eq!(civil_from_days(59), (1970, 3, 1));
        // 1972 was a leap year; 1900 was not.
        assert_eq!(civil_from_days(789), (1972, 2, 29));
        assert_eq!(civil_from_days(-25_567), (1900, 1, 1));
        assert_eq!(civil_from_days(-25_509), (1900, 2, 28));
        assert_eq!(civil_from_days(-25_508), (1900, 3, 1));
        assert_eq!(civil_from_days(-719_468), (0, 3, 1));
        assert_eq!(civil_from_days(-719_528), (0, 1, 1));
        assert_eq!(civil_from_days(-719_529), (-1, 12, 31));
    }

    // Java base64-encodes binary and fixed partition values; iceberg-rust hex-encodes them.
    #[test]
    fn base64_encodes_binary_partition_values() {
        for field_type in [PrimitiveType::Binary, PrimitiveType::Fixed(3)] {
            let encoded = human_string(
                &Transform::Identity,
                &Type::Primitive(field_type),
                Some(&Literal::Primitive(PrimitiveLiteral::Binary(vec![
                    0x00, 0x01, 0xff,
                ]))),
            );
            assert_eq!(encoded, "AAH/");
        }
    }

    // Everything not overridden stays on iceberg-rust's renderer, which already mirrors
    // `TransformUtil` for these.
    #[test]
    fn delegates_the_types_iceberg_rust_already_matches() {
        let cases: Vec<(PrimitiveType, PrimitiveLiteral, &str)> = vec![
            (
                PrimitiveType::Boolean,
                PrimitiveLiteral::Boolean(true),
                "true",
            ),
            (PrimitiveType::Int, PrimitiveLiteral::Int(-7), "-7"),
            (PrimitiveType::Long, PrimitiveLiteral::Long(-7), "-7"),
            (PrimitiveType::Date, PrimitiveLiteral::Int(-1), "1969-12-31"),
            (
                PrimitiveType::String,
                PrimitiveLiteral::String("a b".to_string()),
                "a b",
            ),
            (
                PrimitiveType::Decimal {
                    precision: 9,
                    scale: 2,
                },
                PrimitiveLiteral::Int128(-105),
                "-1.05",
            ),
        ];
        for (field_type, literal, expected) in cases {
            let rendered = human_string(
                &Transform::Identity,
                &Type::Primitive(field_type.clone()),
                Some(&Literal::Primitive(literal)),
            );
            assert_eq!(rendered, expected, "type={field_type:?}");
        }
    }

    #[test]
    fn renders_a_missing_value_as_null() {
        assert_eq!(
            human_string(
                &Transform::Identity,
                &Type::Primitive(PrimitiveType::Timestamptz),
                None
            ),
            "null"
        );
        // A `void` field never carries a value, so it takes the same arm.
        assert_eq!(
            human_string(
                &Transform::Void,
                &Type::Primitive(PrimitiveType::Long),
                None
            ),
            "null"
        );
    }

    fn schema() -> Arc<IcebergSchema> {
        Arc::new(
            IcebergSchema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "ts", Type::Primitive(PrimitiveType::Timestamptz))
                        .into(),
                    NestedField::optional(3, "name", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        )
    }

    fn generator(spec: &PartitionSpec, schema: &Arc<IcebergSchema>) -> CometLocationGenerator {
        CometLocationGenerator::try_new("file:/tmp/t/data".to_string(), spec, schema).unwrap()
    }

    #[test]
    fn unpartitioned_location_has_no_partition_directory() {
        let schema = schema();
        let spec = PartitionSpec::builder(Arc::clone(&schema)).build().unwrap();
        let generator = generator(&spec, &schema);
        assert_eq!(
            generator.generate_location(None, "f.parquet"),
            "file:/tmp/t/data/f.parquet"
        );
        // A key whose spec is unpartitioned is treated as no key at all, matching
        // `DefaultLocationGenerator`.
        let key = PartitionKey::new(spec, Arc::clone(&schema), IcebergStruct::empty());
        assert_eq!(
            generator.generate_location(Some(&key), "f.parquet"),
            "file:/tmp/t/data/f.parquet"
        );
    }

    #[test]
    fn partitioned_location_escapes_names_and_values() {
        let schema = schema();
        let spec = PartitionSpec::builder(Arc::clone(&schema))
            .with_spec_id(1)
            .add_partition_field("ts", "ts_id", Transform::Identity)
            .unwrap()
            .add_partition_field("name", "the name", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();
        let generator = generator(&spec, &schema);
        let key = PartitionKey::new(
            spec,
            Arc::clone(&schema),
            IcebergStruct::from_iter([
                Some(Literal::Primitive(PrimitiveLiteral::Long(-1_500_000))),
                Some(Literal::Primitive(PrimitiveLiteral::String(
                    "a/b c".to_string(),
                ))),
            ]),
        );
        assert_eq!(
            generator.generate_location(Some(&key), "f.parquet"),
            "file:/tmp/t/data/ts_id=1969-12-31T23%3A59%3A58.5%2B00%3A00/the+name=a%2Fb+c/f.parquet"
        );
    }

    // A `void` field alongside a real one keeps its slot in the path with the literal value
    // "null", as `Transform#toHumanString` renders it on the Java side.
    #[test]
    fn void_field_renders_as_null_in_the_path() {
        let schema = schema();
        let spec = PartitionSpec::builder(Arc::clone(&schema))
            .with_spec_id(2)
            .add_partition_field("ts", "ts_id", Transform::Void)
            .unwrap()
            .add_partition_field("id", "id_id", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();
        let generator = generator(&spec, &schema);
        let key = PartitionKey::new(
            spec,
            Arc::clone(&schema),
            IcebergStruct::from_iter([None, Some(Literal::Primitive(PrimitiveLiteral::Int(5)))]),
        );
        assert_eq!(
            generator.generate_location(Some(&key), "f.parquet"),
            "file:/tmp/t/data/ts_id=null/id_id=5/f.parquet"
        );
    }
}
