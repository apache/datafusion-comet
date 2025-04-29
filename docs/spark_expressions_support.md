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

### agg_funcs
 - [x] any
 - [x] any_value
 - [ ] approx_count_distinct
 - [ ] approx_percentile
 - [ ] array_agg
 - [x] avg
 - [x] bit_and
 - [x] bit_or
 - [x] bit_xor
 - [ ] bitmap_construct_agg
 - [ ] bitmap_or_agg
 - [x] bool_and
 - [x] bool_or
 - [ ] collect_list
 - [ ] collect_set
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
 - [ ] kurtosis
 - [x] last
 - [x] last_value
 - [x] max
 - [ ] max_by
 - [x] mean
 - [ ] median
 - [x] min
 - [ ] min_by
 - [ ] mode
 - [ ] percentile
 - [ ] percentile_approx
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
 - [x] sum
 - [ ] try_avg
 - [ ] try_sum
 - [x] var_pop
 - [x] var_samp
 - [x] variance

### array_funcs
 - [x] array
 - [ ] array_append
 - [ ] array_compact
 - [ ] array_contains
 - [ ] array_distinct
 - [ ] array_except
 - [ ] array_insert
 - [ ] array_intersect
 - [ ] array_join
 - [ ] array_max
 - [ ] array_min
 - [ ] array_position
 - [ ] array_prepend
 - [ ] array_remove
 - [ ] array_repeat
 - [ ] array_union
 - [ ] arrays_overlap
 - [ ] arrays_zip
 - [ ] flatten
 - [ ] get
 - [ ] sequence
 - [ ] shuffle
 - [ ] slice
 - [ ] sort_array

### bitwise_funcs
 - [x] &
 - [x] ^
 - [ ] bit_count
 - [ ] bit_get
 - [ ] getbit
 - [x] shiftright
 - [ ] shiftrightunsigned
 - [x] |
 - [x] ~

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
 - [x] nvl
 - [x] nvl2
 - [ ] when

### conversion_funcs
 - [ ] bigint
 - [ ] binary
 - [ ] boolean
 - [x] cast
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
 - [ ] current_timestamp
 - [x] current_timezone
 - [x] date_add
 - [ ] date_diff
 - [ ] date_format
 - [ ] date_from_unix_date
 - [x] date_part
 - [x] date_sub
 - [ ] date_trunc
 - [x] dateadd
 - [ ] datediff
 - [x] datepart
 - [ ] day
 - [ ] dayofmonth
 - [ ] dayofweek
 - [ ] dayofyear
 - [x] extract
 - [ ] from_unixtime
 - [ ] from_utc_timestamp
 - [ ] hour
 - [ ] last_day
 - [ ] localtimestamp
 - [ ] make_date
 - [ ] make_dt_interval
 - [ ] make_interval
 - [ ] make_timestamp
 - [ ] make_timestamp_ltz
 - [ ] make_timestamp_ntz
 - [ ] make_ym_interval
 - [ ] minute
 - [ ] month
 - [ ] months_between
 - [ ] next_day
 - [ ] now
 - [ ] quarter
 - [ ] second
 - [ ] timestamp_micros
 - [ ] timestamp_millis
 - [ ] timestamp_seconds
 - [x] to_date
 - [ ] to_timestamp
 - [ ] to_timestamp_ltz
 - [ ] to_timestamp_ntz
 - [ ] to_unix_timestamp
 - [ ] to_utc_timestamp
 - [x] trunc
 - [ ] try_to_timestamp
 - [ ] unix_date
 - [ ] unix_micros
 - [ ] unix_millis
 - [ ] unix_seconds
 - [ ] unix_timestamp
 - [ ] weekday
 - [ ] weekofyear
 - [x] year

### generator_funcs
 - [ ] explode
 - [ ] explode_outer
 - [ ] inline
 - [ ] inline_outer
 - [ ] posexplode
 - [ ] posexplode_outer
 - [ ] stack

### hash_funcs
 - [ ] crc32
 - [ ] hash
 - [x] md5
 - [ ] sha
 - [ ] sha1
 - [ ] sha2
 - [ ] xxhash64

### json_funcs
 - [ ] from_json
 - [ ] get_json_object
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
 - [ ] map_contains_key
 - [ ] map_entries
 - [ ] map_from_arrays
 - [ ] map_from_entries
 - [ ] map_keys
 - [ ] map_values
 - [ ] str_to_map
 - [ ] try_element_at

### math_funcs
 - [x] %
 - [x] *
 - [x] +
 - [x] -
 - [x] /
 - [ ] abs
 - [x] acos
 - [ ] acosh
 - [x] asin
 - [ ] asinh
 - [x] atan
 - [x] atan2
 - [ ] atanh
 - [ ] bin
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
 - [x] div
 - [ ] e
 - [x] exp
 - [ ] expm1
 - [ ] factorial
 - [x] floor
 - [ ] greatest
 - [x] hex
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
 - [ ] rint
 - [x] round
 - [ ] sec
 - [x] shiftleft
 - [ ] sign
 - [ ] signum
 - [x] sin
 - [ ] sinh
 - [x] sqrt
 - [x] tan
 - [ ] tanh
 - [ ] try_add
 - [x] try_divide
 - [ ] try_multiply
 - [x] try_subtract
 - [x] unhex
 - [ ] width_bucket

### misc_funcs
 - [ ] aes_decrypt
 - [ ] aes_encrypt
 - [ ] assert_true
 - [ ] bitmap_bit_position
 - [ ] bitmap_bucket_number
 - [ ] bitmap_count
 - [x] equal_null
 - [ ] hll_sketch_estimate
 - [ ] hll_union
 - [ ] input_file_block_length
 - [ ] input_file_block_start
 - [ ] input_file_name
 - [ ] monotonically_increasing_id
 - [ ] raise_error
 - [ ] spark_partition_id
 - [ ] try_aes_decrypt
 - [ ] typeof
 - [x] user
 - [ ] uuid
 - [ ] version

### predicate_funcs
 - [x] !
 - [x] <
 - [x] <=
 - [x] <=>
 - [x] =
 - [x] ==
 - [x] >
 - [x] >=
 - [x] and
 - [ ] ilike
 - [x] in
 - [x] isnan
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
 - [x] concat_ws
 - [x] contains
 - [ ] decode
 - [ ] elt
 - [ ] encode
 - [x] endswith
 - [ ] find_in_set
 - [ ] format_number
 - [ ] format_string
 - [ ] initcap
 - [x] instr
 - [ ] lcase
 - [ ] left
 - [x] len
 - [x] length
 - [ ] levenshtein
 - [ ] locate
 - [ ] lower
 - [ ] lpad
 - [x] ltrim
 - [ ] luhn_check
 - [ ] mask
 - [x] octet_length
 - [ ] overlay
 - [ ] position
 - [ ] printf
 - [ ] regexp_count
 - [ ] regexp_extract
 - [ ] regexp_extract_all
 - [ ] regexp_instr
 - [ ] regexp_replace
 - [ ] regexp_substr
 - [x] repeat
 - [x] replace
 - [ ] right
 - [ ] rpad
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
 - [ ] ucase
 - [ ] unbase64
 - [ ] upper

### struct_funcs
 - [x] named_struct
 - [x] struct

### url_funcs
 - [ ] parse_url
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
 - [ ] xpath
 - [ ] xpath_boolean
 - [ ] xpath_double
 - [ ] xpath_float
 - [ ] xpath_int
 - [ ] xpath_long
 - [ ] xpath_number
 - [ ] xpath_short
 - [ ] xpath_string