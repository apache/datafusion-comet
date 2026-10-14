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

//! Parquet field resolution for one group: the root schema or one struct.
//!
//! Spark's `ParquetReadSupport.clipParquetGroupFields` resolves every requested field of a group
//! on its own against the file fields of that group. A requested field that carries a Parquet
//! field id, when id matching is on, resolves by id only (`matchIdField`): a missing id is a
//! missing column, never a name match. Every other requested field resolves by name, folded with
//! `toLowerCase(Locale.ROOT)` when the read is case-insensitive. Comet resolves fields in three
//! places: the root [`schema_adapter`](super::schema_adapter), which renames file columns so
//! DataFusion's exact-name lookup finds them; the nested `Struct -> Struct` convert in
//! [`parquet_support`](super::parquet_support); and the plan-time conversion check, which
//! resolves nested fields the way that convert does. All of them select file fields through
//! [`FieldResolver`], so a change to the precedence rules reaches the root and nested fields
//! together.

use crate::parquet::parquet_support::{duplicate_parquet_field_error, field_id};
use arrow::datatypes::FieldRef;
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion_comet_common::SparkError;
use std::collections::HashMap;

/// The file fields that one requested field selects, and the rule that selected them. Each
/// `matches` lists file field indices in file order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Selection<'a> {
    /// Spark's `matchIdField`: id matching is on and the requested field carries `id`, so it
    /// selects the file fields that carry the same id and never compares names. No match means
    /// the column is missing.
    ById { id: i32, matches: &'a [usize] },
    /// Spark's `matchCaseSensitiveField` / `matchCaseInsensitiveField`: the file fields whose
    /// folded name equals the requested field's folded name.
    ByName { matches: &'a [usize] },
}

impl<'a> Selection<'a> {
    /// The selected file field indices, whichever rule selected them.
    pub(crate) fn matches(&self) -> &'a [usize] {
        match self {
            Selection::ById { matches, .. } | Selection::ByName { matches } => matches,
        }
    }
}

/// Resolves the requested fields of one group against the file fields of the same group, as
/// Spark's `clipParquetGroupFields` does. The caller folds both sides' names once (see
/// `name_fold`) and passes them in, parallel to the fields.
///
/// Selection does not raise on ambiguity, because the root reports an ambiguous name only for the
/// columns a read references; [`FieldResolver::resolve`] raises it for a nested field.
pub(crate) struct FieldResolver<'a> {
    requested: &'a [FieldRef],
    requested_folded: &'a [String],
    file: &'a [FieldRef],
    case_sensitive: bool,
    /// Spark's `shouldMatchById`: id matching is on and some requested field carries an id.
    match_by_id: bool,
    /// File field indices by Parquet field id, built only when `match_by_id`. Spark's
    /// `idToParquetFieldMap`.
    file_by_id: HashMap<i32, Indices>,
    /// File field indices by folded name, grouped so a collision is seen rather than overwritten.
    /// Spark's `caseInsensitiveParquetFieldMap`, and its case-sensitive map when the fold is the
    /// identity.
    file_by_name: HashMap<&'a str, Indices>,
}

impl<'a> FieldResolver<'a> {
    pub(crate) fn new(
        requested: &'a [FieldRef],
        requested_folded: &'a [String],
        file: &'a [FieldRef],
        file_folded: &'a [String],
        case_sensitive: bool,
        use_field_id: bool,
    ) -> Self {
        debug_assert_eq!(requested.len(), requested_folded.len());
        debug_assert_eq!(file.len(), file_folded.len());
        let match_by_id = use_field_id && requested.iter().any(|f| field_id(f).is_some());
        let mut file_by_id = HashMap::new();
        if match_by_id {
            file_by_id.reserve(file.len());
            for (index, field) in file.iter().enumerate() {
                if let Some(id) = field_id(field) {
                    Indices::insert(&mut file_by_id, id, index);
                }
            }
        }
        let mut file_by_name = HashMap::with_capacity(file_folded.len());
        for (index, folded) in file_folded.iter().enumerate() {
            Indices::insert(&mut file_by_name, folded.as_str(), index);
        }
        Self {
            requested,
            requested_folded,
            file,
            case_sensitive,
            match_by_id,
            file_by_id,
            file_by_name,
        }
    }

    /// The number of requested fields, which `select` and `resolve` index.
    pub(crate) fn requested_len(&self) -> usize {
        self.requested.len()
    }

    /// The number of file fields, which a [`Selection`] indexes.
    pub(crate) fn file_len(&self) -> usize {
        self.file.len()
    }

    /// The file fields requested field `requested` selects, by id when it carries one and id
    /// matching is on, otherwise by folded name.
    pub(crate) fn select(&self, requested: usize) -> Selection<'_> {
        let id = if self.match_by_id {
            field_id(&self.requested[requested])
        } else {
            None
        };
        match id {
            Some(id) => Selection::ById {
                id,
                matches: self.file_by_id.get(&id).map_or(&[], Indices::as_slice),
            },
            None => Selection::ByName {
                matches: self.name_matches(requested),
            },
        }
    }

    /// The file fields whose folded name equals requested field `requested`'s, even when it
    /// selects by id. The root hides these behind placeholder names so that a field `matchIdField`
    /// resolves cannot reach them by name.
    pub(crate) fn name_matches(&self, requested: usize) -> &[usize] {
        self.file_by_name
            .get(self.requested_folded[requested].as_str())
            .map_or(&[], Indices::as_slice)
    }

    /// The file field that requested field `requested` reads from, or `None` when the file holds
    /// no such field. An ambiguous selection is an error, raised before a decoder can multiply
    /// rows. A repeated id raises Spark's `_LEGACY_ERROR_TEMP_2094`, and a case-insensitive name
    /// collision raises its `foundDuplicateFieldInCaseInsensitiveModeError`. A repeated exact name
    /// raises Comet's duplicate field name error, where Spark would read the last field of that
    /// name.
    pub(crate) fn resolve(&self, requested: usize) -> DataFusionResult<Option<usize>> {
        match self.select(requested) {
            Selection::ById { id, matches } if matches.len() > 1 => {
                Err(self.duplicate_id_error(id, matches))
            }
            Selection::ByName { matches } if matches.len() > 1 => {
                Err(self.duplicate_name_error(requested, matches))
            }
            selection => Ok(selection.matches().first().copied()),
        }
    }

    /// Spark's `_LEGACY_ERROR_TEMP_2094` for requested `id`, which the file fields at `matches`
    /// all carry. The names are bracketed and comma-joined the way Spark's `matchIdField` renders
    /// them, so the message reads `id=1 matches [x, y]` on both sides.
    pub(crate) fn duplicate_id_error(&self, id: i32, matches: &[usize]) -> DataFusionError {
        SparkError::DuplicateFieldByFieldId {
            required_id: id,
            matched_fields: format!("[{}]", self.file_names(matches).join(", ")),
        }
        .into()
    }

    fn duplicate_name_error(&self, requested: usize, matches: &[usize]) -> DataFusionError {
        let name = self.requested[requested].name();
        if self.case_sensitive {
            return duplicate_parquet_field_error(name);
        }
        DataFusionError::External(Box::new(SparkError::duplicate_field_case_insensitive(
            name,
            &self.file_names(matches),
        )))
    }

    fn file_names(&self, indices: &[usize]) -> Vec<&str> {
        indices
            .iter()
            .map(|&index| self.file[index].name().as_str())
            .collect()
    }
}

/// The file field indices that share one key, in file order. Almost every key is unique, so a
/// single index is kept inline, and a group allocates only when two fields collide.
#[derive(Debug)]
enum Indices {
    One(usize),
    Many(Vec<usize>),
}

impl Indices {
    fn insert<K: std::hash::Hash + Eq>(map: &mut HashMap<K, Indices>, key: K, index: usize) {
        map.entry(key)
            .and_modify(|indices| match indices {
                Indices::One(first) => *indices = Indices::Many(vec![*first, index]),
                Indices::Many(all) => all.push(index),
            })
            .or_insert(Indices::One(index));
    }

    fn as_slice(&self) -> &[usize] {
        match self {
            Indices::One(index) => std::slice::from_ref(index),
            Indices::Many(all) => all,
        }
    }
}

#[cfg(test)]
mod test {
    use super::{FieldResolver, Selection};
    use arrow::datatypes::{DataType, Field, FieldRef};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn field(name: &str, id: Option<i32>) -> FieldRef {
        let field = Field::new(name, DataType::Int32, true);
        Arc::new(match id {
            Some(id) => field.with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                id.to_string(),
            )])),
            None => field,
        })
    }

    /// The test names are ASCII, where `to_ascii_lowercase` is the fold Spark applies.
    fn folded(fields: &[FieldRef], case_sensitive: bool) -> Vec<String> {
        fields
            .iter()
            .map(|f| {
                if case_sensitive {
                    f.name().clone()
                } else {
                    f.name().to_ascii_lowercase()
                }
            })
            .collect()
    }

    /// Run `check` against a resolver over `requested` and `file`.
    fn with_resolver<T>(
        requested: &[FieldRef],
        file: &[FieldRef],
        case_sensitive: bool,
        use_field_id: bool,
        check: impl FnOnce(&FieldResolver) -> T,
    ) -> T {
        let requested_folded = folded(requested, case_sensitive);
        let file_folded = folded(file, case_sensitive);
        check(&FieldResolver::new(
            requested,
            &requested_folded,
            file,
            &file_folded,
            case_sensitive,
            use_field_id,
        ))
    }

    /// `matchIdField` selects by id even when another file field has the requested name.
    #[test]
    fn id_bearing_field_selects_by_id_over_name() {
        let requested = [field("a", Some(1))];
        let file = [field("a", Some(2)), field("b", Some(1))];
        with_resolver(&requested, &file, false, true, |resolver| {
            assert_eq!(
                resolver.select(0),
                Selection::ById {
                    id: 1,
                    matches: &[1]
                }
            );
            assert_eq!(resolver.resolve(0).unwrap(), Some(1));
            assert_eq!(resolver.name_matches(0), &[0]);
        });
    }

    /// `matchIdField` treats an id the file lacks as a missing column (Spark gives it a fake
    /// name) instead of falling back to the file field with the requested name.
    #[test]
    fn missing_id_is_a_missing_column() {
        let requested = [field("a", Some(1))];
        let file = [field("A", Some(2))];
        with_resolver(&requested, &file, false, true, |resolver| {
            assert_eq!(
                resolver.select(0),
                Selection::ById {
                    id: 1,
                    matches: &[]
                }
            );
            assert_eq!(resolver.resolve(0).unwrap(), None);
        });
    }

    /// `shouldMatchById` only routes fields that carry an id to `matchIdField`. A field without one
    /// resolves by name even when its siblings resolve by id, and may select a file field that also
    /// carries an id.
    #[test]
    fn field_without_id_resolves_by_name_beside_id_bearing_siblings() {
        let requested = [field("a", Some(1)), field("b", None)];
        let file = [field("B", Some(5))];
        with_resolver(&requested, &file, false, true, |resolver| {
            assert_eq!(resolver.select(1), Selection::ByName { matches: &[0] });
            assert_eq!(resolver.resolve(1).unwrap(), Some(0));
        });
    }

    /// With id matching off, ids on either side are ignored.
    #[test]
    fn ids_are_ignored_when_id_matching_is_off() {
        let requested = [field("a", Some(1))];
        let file = [field("x", Some(1)), field("a", Some(2))];
        with_resolver(&requested, &file, false, false, |resolver| {
            assert_eq!(resolver.select(0), Selection::ByName { matches: &[1] });
        });
    }

    /// `matchCaseInsensitiveField` folds names, and fails when the fold is ambiguous.
    #[test]
    fn case_insensitive_name_collision_is_an_error() {
        let requested = [field("b", None), field("c", None)];
        let file = [field("A", None), field("B", None), field("b", None)];
        with_resolver(&requested, &file, false, false, |resolver| {
            assert_eq!(resolver.select(0), Selection::ByName { matches: &[1, 2] });
            let message = resolver.resolve(0).unwrap_err().to_string();
            assert!(
                message.contains("Found duplicate field(s)") && message.contains("[B, b]"),
                "{message}"
            );
            assert_eq!(resolver.resolve(1).unwrap(), None);
        });
    }

    /// Case-sensitive matching compares exact names, so a repeated exact name is the only
    /// ambiguity. Comet rejects it where Spark would read the last field of that name.
    #[test]
    fn case_sensitive_repeated_name_is_an_error() {
        let requested = [field("b", None)];
        let file = [field("B", None), field("b", None), field("b", None)];
        with_resolver(&requested, &file, true, false, |resolver| {
            assert_eq!(resolver.select(0), Selection::ByName { matches: &[1, 2] });
            let message = resolver.resolve(0).unwrap_err().to_string();
            assert!(
                message.contains("Found duplicate Parquet field name 'b'")
                    && !message.contains("case-insensitive"),
                "{message}"
            );
        });
    }

    /// A requested id the file repeats raises `_LEGACY_ERROR_TEMP_2094`, but a repeated id or
    /// name that no requested field selects stays harmless, as in Spark.
    #[test]
    fn only_a_requested_ambiguity_is_an_error() {
        let requested = [field("f", Some(1)), field("g", Some(2))];
        let file = [
            field("x", Some(1)),
            field("y", Some(1)),
            field("z", Some(2)),
            field("g", None),
            field("G", None),
        ];
        with_resolver(&requested, &file, false, true, |resolver| {
            let message = resolver.resolve(0).unwrap_err().to_string();
            assert!(
                message.contains("_LEGACY_ERROR_TEMP_2094")
                    && message.contains("id=1 matches [x, y]"),
                "{message}"
            );
            assert_eq!(resolver.resolve(1).unwrap(), Some(2));
            assert_eq!(resolver.name_matches(1), &[3, 4]);
        });
    }
}
