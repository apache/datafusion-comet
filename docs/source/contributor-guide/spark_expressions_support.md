<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Supported Spark Expressions

## How to Read This Document

- A function marked with `[x]` has a native implementation in Comet and does not fall back to Spark by default.
- A function marked with `[ ]` has no native Comet implementation and falls back to Spark.

> **Note:** Some functions listed as supported may still be incompatible with Spark in
> certain cases (data types, modes, edge values) and fall back to Spark at runtime. Full
> per-function details are documented in the
> [Compatibility Guide](https://datafusion.apache.org/comet/user-guide/latest/compatibility/expressions/index.html).

### agg_funcs

- [x] any
- [x] any_value
- [ ] approx_count_distinct
- [ ] approx_percentile
- [ ] approx_top_k
- [ ] approx_top_k_accumulate
- [ ] approx_top_k_combine
- [ ] array_agg
- [x] avg
- [x] bit_and
- [x] bit_or
- [x] bit_xor
- [x] bool_and
- [x] bool_or
- [ ] collect_list
- [x] collect_set
- [ ] corr
- [x] count
- [x] count_if
- [ ] count_min_sketch
- [x] covar_pop
- [x] covar_samp
- [x] every
- [x] first
- [x] first_value
- [ ] grouping
- [ ] grouping_id
- [ ] histogram_numeric
- [ ] hll_sketch_agg
- [ ] hll_union_agg
- [ ] kll_sketch_agg_bigint
- [ ] kll_sketch_agg_double
- [ ] kll_sketch_agg_float
- [ ] kll_sketch_get_n_bigint
- [ ] kll_sketch_get_n_double
- [ ] kll_sketch_get_n_float
- [ ] kll_sketch_get_quantile_bigint
- [ ] kll_sketch_get_quantile_double
- [ ] kll_sketch_get_quantile_float
- [ ] kll_sketch_get_rank_bigint
- [ ] kll_sketch_get_rank_double
- [ ] kll_sketch_get_rank_float
- [ ] kll_sketch_merge_bigint
- [ ] kll_sketch_merge_double
- [ ] kll_sketch_merge_float
- [ ] kll_sketch_to_string_bigint
- [ ] kll_sketch_to_string_double
- [ ] kll_sketch_to_string_float
- [ ] kurtosis
- [x] last
- [x] last_value
- [ ] listagg
- [x] max
- [ ] max_by
- [x] mean
- [ ] median
- [x] min
- [ ] min_by
- [ ] mode
- [ ] percentile
- [ ] percentile_approx
- [ ] percentile_cont
- [ ] percentile_disc
- [x] regr_avgx
- [x] regr_avgy
- [x] regr_count
- [ ] regr_intercept
- [ ] regr_r2
- [ ] regr_slope
- [ ] regr_sxx
- [ ] regr_sxy
- [ ] regr_syy
- [ ] skewness
- [x] some
- [x] std
- [x] stddev
- [x] stddev_pop
- [x] stddev_samp
- [ ] string_agg
- [x] sum
- [ ] theta_intersection_agg
- [ ] theta_sketch_agg
- [ ] theta_union_agg
- [ ] try_avg
- [ ] try_sum
- [x] var_pop
- [x] var_samp
- [x] variance

### array_funcs

- [x] array
- [x] array_append
- [x] array_compact
- [x] array_contains
- [x] array_distinct
- [x] array_except
- [x] array_insert
  - Spark 3.4.3 audited 2026-04-02
  - Spark 3.5.8 audited 2026-04-02
  - Spark 4.0.1 audited 2026-04-02 (pos=0 error message differs from Spark)
- [x] array_intersect
  - Spark 3.4.3 audited 2026-04-24 (result element order may differ from Spark when the right array is longer than the left; DataFusion probes the longer side)
  - Spark 3.5.8 audited 2026-04-24 (same ordering incompatibility as 3.4.3)
  - Spark 4.0.1 audited 2026-04-24 (ordering incompatibility as above; collated strings now fall back to Spark)
- [x] array_join
- [x] array_max
- [ ] array_min
- [x] array_position
- [ ] array_prepend
- [x] array_remove
- [x] array_repeat
- [x] array_union
- [x] arrays_overlap
- [x] arrays_zip
- [x] element_at
- [ ] flatten
- [x] get
- [ ] sequence
- [ ] shuffle
- [ ] slice
- [x] sort_array

### bitwise_funcs

- [x] `&`
- [ ] `<<`
- [ ] `>>`
- [ ] `>>>`
- [x] `^`
- [ ] bit_count
- [ ] bit_get
- [ ] getbit
- [x] shiftright
- [ ] shiftrightunsigned
- [x] `|`
- [x] `~`

### collection_funcs

- [ ] array_size
- [ ] cardinality
- [ ] concat
- [x] reverse
- [ ] size

### conditional_funcs

- [x] coalesce
- [x] if
- [x] ifnull
- [ ] nanvl
- [x] nullif
- [ ] nullifzero
- [x] nvl
- [x] nvl2
- [ ] when
- [ ] zeroifnull

### conversion_funcs

- [ ] bigint
- [ ] binary
- [ ] boolean
- [ ] cast
- [ ] date
- [ ] decimal
- [ ] double
- [ ] float
- [ ] int
- [ ] smallint
- [ ] string
- [ ] timestamp
- [ ] tinyint

### csv_funcs

- [ ] from_csv
- [ ] schema_of_csv
- [ ] to_csv

### datetime_funcs

- [ ] add_months
- [ ] convert_timezone
- [x] curdate
- [x] current_date
- [ ] current_time
- [ ] current_timestamp
- [x] current_timezone
- [ ] date_add
- [ ] date_diff
- [ ] date_format
- [x] date_from_unix_date
- [x] date_part
- [ ] date_sub
- [ ] date_trunc
- [ ] dateadd
- [ ] datediff
- [x] datepart
- [ ] day
- [ ] dayname
- [ ] dayofmonth
- [ ] dayofweek
- [ ] dayofyear
- [x] extract
- [x] from_unixtime
- [ ] from_utc_timestamp
- [ ] hour
- [ ] last_day
- [ ] localtimestamp
- [ ] make_date
- [ ] make_dt_interval
- [ ] make_interval
- [ ] make_time
- [ ] make_timestamp
- [ ] make_timestamp_ltz
- [ ] make_timestamp_ntz
- [ ] make_ym_interval
- [ ] minute
- [ ] month
- [ ] monthname
- [ ] months_between
- [ ] next_day
- [ ] now
- [ ] quarter
- [ ] second
- [ ] session_window
- [ ] time_diff
- [ ] time_trunc
- [ ] timestamp_micros
- [ ] timestamp_millis
- [x] timestamp_seconds
- [ ] to_date
- [ ] to_time
- [ ] to_timestamp
- [ ] to_timestamp_ltz
- [ ] to_timestamp_ntz
- [ ] to_unix_timestamp
- [ ] to_utc_timestamp
- [ ] trunc
- [ ] try_make_interval
- [ ] try_make_timestamp
- [ ] try_to_date
- [ ] try_to_time
- [ ] try_to_timestamp
- [ ] unix_date
- [ ] unix_micros
- [ ] unix_millis
- [ ] unix_seconds
- [x] unix_timestamp
- [ ] weekday
- [ ] weekofyear
- [ ] window
- [ ] window_time
- [ ] year

### generator_funcs

- [ ] explode
- [ ] explode_outer
- [ ] inline
- [ ] inline_outer
- [ ] posexplode
- [ ] posexplode_outer
- [ ] stack

### hash_funcs

- [x] crc32
- [ ] hash
- [x] md5
- [ ] sha
- [ ] sha1
- [ ] sha2
- [ ] xxhash64

### json_funcs

- [ ] from_json
- [x] get_json_object
- [ ] json_array_length
- [ ] json_object_keys
- [ ] json_tuple
- [ ] schema_of_json
- [ ] to_json

### lambda_funcs

- [ ] aggregate
- [ ] array_sort
- [ ] exists
- [ ] filter
- [ ] forall
- [ ] map_filter
- [ ] map_zip_with
- [ ] reduce
- [ ] transform
- [ ] transform_keys
- [ ] transform_values
- [ ] zip_with

### map_funcs

- [ ] element_at
- [ ] map
- [ ] map_concat
- [x] map_contains_key
- [ ] map_entries
- [ ] map_from_arrays
- [x] map_from_entries
- [x] map_keys
- [ ] map_values
- [ ] str_to_map
- [ ] try_element_at

### math_funcs

- [x] `%`
- [ ] `*`
- [ ] `+`
- [x] `-`
- [x] `/`
- [x] abs
- [x] acos
- [ ] acosh
- [x] asin
- [ ] asinh
- [x] atan
- [x] atan2
- [ ] atanh
- [x] bin
- [ ] bround
- [ ] cbrt
- [x] ceil
- [x] ceiling
- [ ] conv
- [x] cos
- [ ] cosh
- [ ] cot
- [ ] csc
- [ ] degrees
- [ ] div
- [ ] e
- [x] exp
- [ ] expm1
- [ ] factorial
- [x] floor
- [ ] greatest
- [ ] hex
- [ ] hypot
- [ ] least
- [x] ln
- [ ] log
- [x] log10
- [ ] log1p
- [x] log2
- [x] mod
- [x] negative
- [ ] pi
- [ ] pmod
- [x] positive
- [x] pow
- [x] power
- [ ] radians
- [ ] rand
- [ ] randn
- [ ] random
- [ ] randstr
- [ ] rint
- [x] round
- [ ] sec
- [x] shiftleft
- [x] sign
- [x] signum
- [x] sin
- [ ] sinh
- [x] sqrt
- [x] tan
- [ ] tanh
- [x] try_add
- [x] try_divide
- [ ] try_mod
- [x] try_multiply
- [x] try_subtract
- [x] unhex
- [ ] uniform
- [x] width_bucket

### misc_funcs

- [ ] aes_decrypt
- [ ] aes_encrypt
- [ ] approx_top_k_estimate
- [ ] assert_true
- [ ] bitmap_and_agg
- [ ] bitmap_bit_position
- [ ] bitmap_bucket_number
- [ ] bitmap_construct_agg
- [ ] bitmap_count
- [ ] bitmap_or_agg
- [x] current_catalog
- [x] current_database
- [x] current_schema
- [x] current_user
- [x] equal_null
- [ ] from_avro
- [ ] from_protobuf
- [ ] hll_sketch_estimate
- [ ] hll_union
- [ ] input_file_block_length
- [ ] input_file_block_start
- [ ] input_file_name
- [ ] is_variant_null
- [ ] java_method
- [x] monotonically_increasing_id
- [ ] parse_json
- [ ] raise_error
- [x] rand
- [x] randn
- [ ] reflect
- [ ] schema_of_avro
- [ ] schema_of_variant
- [ ] schema_of_variant_agg
- [ ] session_user
- [x] spark_partition_id
- [ ] st_asbinary
- [ ] st_geogfromwkb
- [ ] st_geomfromwkb
- [ ] st_setsrid
- [ ] st_srid
- [ ] theta_difference
- [ ] theta_intersection
- [ ] theta_sketch_estimate
- [ ] theta_union
- [ ] to_avro
- [ ] to_protobuf
- [ ] to_variant_object
- [ ] try_aes_decrypt
- [ ] try_parse_json
- [ ] try_reflect
- [ ] try_variant_get
- [ ] typeof
- [x] user
- [ ] uuid
- [ ] variant_get
- [ ] version

### predicate_funcs

- [x] `!`
- [x] `<`
- [x] `<=`
- [x] `<=>`
- [x] `=`
- [x] `==`
- [x] `>`
- [x] `>=`
- [x] and
- [ ] between
- [x] ilike
- [x] in
- [ ] isnan
- [x] isnotnull
- [x] isnull
- [x] like
- [x] not
- [x] or
- [ ] regexp
- [ ] regexp_like
- [ ] rlike

### string_funcs

- [x] ascii
- [ ] base64
- [x] bit_length
- [x] btrim
- [x] char
- [x] char_length
- [x] character_length
- [x] chr
- [ ] collate
- [ ] collation
- [x] concat_ws
- [x] contains
- [ ] decode
- [ ] elt
- [ ] encode
- [x] endswith
- [ ] find_in_set
- [ ] format_number
- [ ] format_string
- [x] initcap
- [x] instr
- [ ] is_valid_utf8
- [x] lcase
- [ ] left
- [x] len
- [x] length
- [ ] levenshtein
- [ ] locate
- [x] lower
- [x] lpad
- [x] ltrim
- [ ] luhn_check
- [ ] make_valid_utf8
- [ ] mask
- [x] octet_length
- [ ] overlay
- [ ] position
- [ ] printf
- [ ] quote
- [ ] regexp_count
- [ ] regexp_extract
- [ ] regexp_extract_all
- [ ] regexp_instr
- [ ] regexp_replace
- [ ] regexp_substr
- [x] repeat
- [x] replace
- [ ] right
- [x] rpad
- [x] rtrim
- [ ] sentences
- [ ] soundex
- [x] space
- [ ] split
- [ ] split_part
- [x] startswith
- [ ] substr
- [ ] substring
- [ ] substring_index
- [ ] to_binary
- [ ] to_char
- [ ] to_number
- [ ] to_varchar
- [x] translate
- [x] trim
- [ ] try_to_binary
- [ ] try_to_number
- [ ] try_validate_utf8
- [x] ucase
- [ ] unbase64
- [x] upper
- [ ] validate_utf8

### struct_funcs

- [ ] named_struct
- [ ] struct

### url_funcs

- [ ] parse_url
- [ ] try_parse_url
- [ ] try_url_decode
- [ ] url_decode
- [ ] url_encode

### window_funcs

- [ ] cume_dist
- [ ] dense_rank
- [ ] lag
- [ ] lead
- [ ] nth_value
- [ ] ntile
- [ ] percent_rank
- [ ] rank
- [ ] row_number

### xml_funcs

- [ ] from_xml
- [ ] schema_of_xml
- [ ] to_xml
- [ ] xpath
- [ ] xpath_boolean
- [ ] xpath_double
- [ ] xpath_float
- [ ] xpath_int
- [ ] xpath_long
- [ ] xpath_number
- [ ] xpath_short
- [ ] xpath_string
