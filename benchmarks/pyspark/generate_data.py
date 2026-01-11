#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Generate test data for shuffle size comparison benchmark.

This script generates a parquet dataset with a realistic schema (100 columns
including deeply nested structs, arrays, and maps) for benchmarking shuffle
operations across Spark, Comet JVM, and Comet Native shuffle modes.
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, LongType, DoubleType,
    StringType, BooleanType, DateType, TimestampType, ArrayType,
    MapType, DecimalType
)


def generate_data(output_path: str, num_rows: int, num_partitions: int):
    """Generate test data with realistic schema and write to parquet."""

    spark = SparkSession.builder \
        .appName("ShuffleBenchmark-DataGen") \
        .getOrCreate()

    print(f"Generating {num_rows:,} rows with {num_partitions} partitions")
    print(f"Output path: {output_path}")
    print("Schema: 100 columns including deeply nested structs, arrays, and maps")

    # Start with a range and build up the columns
    df = spark.range(0, num_rows, numPartitions=num_partitions)

    # Add columns using selectExpr for better performance
    df = df.selectExpr(
        # Key columns for grouping/partitioning (1-3)
        "cast(id % 1000 as int) as partition_key",
        "cast(id % 100 as int) as group_key",
        "id as row_id",

        # Integer columns (4-15)
        "cast(id % 10000 as int) as category_id",
        "cast(id % 500 as int) as region_id",
        "cast(id % 50 as int) as department_id",
        "cast((id * 7) % 1000000 as int) as customer_id",
        "cast((id * 13) % 100000 as int) as product_id",
        "cast(id % 12 + 1 as int) as month",
        "cast(id % 28 + 1 as int) as day",
        "cast(2020 + (id % 5) as int) as year",
        "cast((id * 17) % 256 as int) as priority",
        "cast((id * 19) % 1000 as int) as rank",
        "cast((id * 23) % 10000 as int) as score_int",
        "cast((id * 29) % 500 as int) as level",

        # Long columns (16-22)
        "id * 1000 as transaction_id",
        "(id * 17) % 10000000000 as account_number",
        "(id * 31) % 1000000000 as reference_id",
        "(id * 37) % 10000000000 as external_id",
        "(id * 41) % 1000000000 as correlation_id",
        "(id * 43) % 10000000000 as trace_id",
        "(id * 47) % 1000000000 as span_id",

        # Double columns (23-35)
        "cast(id % 10000 as double) / 100.0 as amount",
        "cast((id * 3) % 10000 as double) / 100.0 as price",
        "cast(id % 100 as double) / 100.0 as discount",
        "cast((id * 7) % 500 as double) / 10.0 as weight",
        "cast((id * 11) % 1000 as double) / 10.0 as height",
        "cast(id % 360 as double) as latitude",
        "cast((id * 2) % 360 as double) as longitude",
        "cast((id * 13) % 10000 as double) / 1000.0 as rate",
        "cast((id * 17) % 100 as double) / 100.0 as percentage",
        "cast((id * 19) % 1000 as double) as velocity",
        "cast((id * 23) % 500 as double) / 10.0 as acceleration",
        "cast((id * 29) % 10000 as double) / 100.0 as temperature",
        "cast((id * 31) % 1000 as double) / 10.0 as pressure",

        # String columns (36-50)
        "concat('user_', cast(id % 100000 as string)) as user_name",
        "concat('email_', cast(id % 50000 as string), '@example.com') as email",
        "concat('SKU-', lpad(cast(id % 10000 as string), 6, '0')) as sku",
        "concat('ORD-', cast(id as string)) as order_id",
        "array('pending', 'processing', 'shipped', 'delivered', 'cancelled')[cast(id % 5 as int)] as status",
        "array('USD', 'EUR', 'GBP', 'JPY', 'CAD')[cast(id % 5 as int)] as currency",
        "concat('Description for item ', cast(id % 1000 as string), ' with additional details') as description",
        "concat('REF-', lpad(cast(id % 100000 as string), 8, '0')) as reference_code",
        "concat('TXN-', cast(id as string), '-', cast(id % 1000 as string)) as transaction_code",
        "array('A', 'B', 'C', 'D', 'E')[cast(id % 5 as int)] as grade",
        "concat('Note: Record ', cast(id as string), ' processed successfully') as notes",
        "concat('Session-', lpad(cast(id % 10000 as string), 6, '0')) as session_id",
        "concat('Device-', cast(id % 1000 as string)) as device_id",
        "array('chrome', 'firefox', 'safari', 'edge')[cast(id % 4 as int)] as browser",
        "array('windows', 'macos', 'linux', 'ios', 'android')[cast(id % 5 as int)] as os",

        # Boolean columns (51-56)
        "id % 2 = 0 as is_active",
        "id % 3 = 0 as is_verified",
        "id % 7 = 0 as is_premium",
        "id % 5 = 0 as is_deleted",
        "id % 11 = 0 as is_featured",
        "id % 13 = 0 as is_archived",

        # Date and timestamp columns (57-60)
        "date_add(to_date('2020-01-01'), cast(id % 1500 as int)) as created_date",
        "date_add(to_date('2020-01-01'), cast((id + 30) % 1500 as int)) as updated_date",
        "date_add(to_date('2020-01-01'), cast((id + 60) % 1500 as int)) as expires_date",
        "to_timestamp(concat('2020-01-01 ', lpad(cast(id % 24 as string), 2, '0'), ':00:00')) as created_at",

        # Simple arrays (61-65)
        "array(cast(id % 100 as int), cast((id + 1) % 100 as int), cast((id + 2) % 100 as int), cast((id + 3) % 100 as int), cast((id + 4) % 100 as int)) as tag_ids",
        "array(cast(id % 1000 as double) / 10.0, cast((id * 2) % 1000 as double) / 10.0, cast((id * 3) % 1000 as double) / 10.0) as scores",
        "array(concat('tag_', cast(id % 20 as string)), concat('tag_', cast((id + 5) % 20 as string)), concat('tag_', cast((id + 10) % 20 as string))) as tags",
        "array(id % 2 = 0, id % 3 = 0, id % 5 = 0, id % 7 = 0) as flag_array",
        "array(id * 1000, id * 2000, id * 3000) as long_array",

        # Simple maps (66-68)
        "map('key1', cast(id % 100 as string), 'key2', cast((id * 2) % 100 as string), 'key3', cast((id * 3) % 100 as string)) as str_attributes",
        "map('score1', cast(id % 100 as double), 'score2', cast((id * 2) % 100 as double)) as double_attributes",
        "map(cast(id % 10 as int), concat('val_', cast(id % 100 as string)), cast((id + 1) % 10 as int), concat('val_', cast((id + 1) % 100 as string))) as int_key_map",

        # Level 2 nested struct: address with nested geo (69)
        "named_struct("
        "  'street', concat(cast(id % 9999 as string), ' Main St'),"
        "  'city', array('New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix')[cast(id % 5 as int)],"
        "  'state', array('NY', 'CA', 'IL', 'TX', 'AZ')[cast(id % 5 as int)],"
        "  'zip', lpad(cast(id % 99999 as string), 5, '0'),"
        "  'country', 'USA',"
        "  'geo', named_struct("
        "    'lat', cast(id % 180 as double) - 90.0,"
        "    'lng', cast(id % 360 as double) - 180.0,"
        "    'accuracy', cast(id % 100 as double)"
        "  )"
        ") as address",

        # Level 3 nested struct: organization hierarchy (70)
        "named_struct("
        "  'company', named_struct("
        "    'name', concat('Company_', cast(id % 1000 as string)),"
        "    'industry', array('tech', 'finance', 'healthcare', 'retail')[cast(id % 4 as int)],"
        "    'headquarters', named_struct("
        "      'city', array('NYC', 'SF', 'LA', 'CHI')[cast(id % 4 as int)],"
        "      'country', 'USA',"
        "      'timezone', array('EST', 'PST', 'PST', 'CST')[cast(id % 4 as int)]"
        "    )"
        "  ),"
        "  'department', named_struct("
        "    'name', array('Engineering', 'Sales', 'Marketing', 'HR')[cast(id % 4 as int)],"
        "    'code', concat('DEPT-', cast(id % 100 as string)),"
        "    'budget', cast(id % 1000000 as double)"
        "  )"
        ") as organization",

        # Level 4 nested struct: deep config (71)
        "named_struct("
        "  'level1', named_struct("
        "    'level2a', named_struct("
        "      'level3a', named_struct("
        "        'value_int', cast(id % 1000 as int),"
        "        'value_str', concat('deep_', cast(id % 100 as string)),"
        "        'value_bool', id % 2 = 0"
        "      ),"
        "      'level3b', named_struct("
        "        'metric1', cast(id % 100 as double),"
        "        'metric2', cast((id * 2) % 100 as double)"
        "      )"
        "    ),"
        "    'level2b', named_struct("
        "      'setting1', concat('setting_', cast(id % 50 as string)),"
        "      'setting2', id % 3 = 0,"
        "      'values', array(cast(id % 10 as int), cast((id + 1) % 10 as int), cast((id + 2) % 10 as int))"
        "    )"
        "  ),"
        "  'metadata', named_struct("
        "    'version', concat('v', cast(id % 10 as string)),"
        "    'timestamp', id * 1000"
        "  )"
        ") as deep_config",

        # Array of structs with nested structs (72)
        "array("
        "  named_struct("
        "    'item_id', cast(id % 1000 as int),"
        "    'details', named_struct("
        "      'name', concat('Item_', cast(id % 100 as string)),"
        "      'category', array('electronics', 'clothing', 'food', 'books')[cast(id % 4 as int)],"
        "      'pricing', named_struct("
        "        'base', cast(id % 100 as double) + 0.99,"
        "        'discount', cast(id % 20 as double) / 100.0,"
        "        'tax_rate', 0.08"
        "      )"
        "    ),"
        "    'quantity', cast(id % 10 + 1 as int)"
        "  ),"
        "  named_struct("
        "    'item_id', cast((id + 100) % 1000 as int),"
        "    'details', named_struct("
        "      'name', concat('Item_', cast((id + 100) % 100 as string)),"
        "      'category', array('electronics', 'clothing', 'food', 'books')[cast((id + 1) % 4 as int)],"
        "      'pricing', named_struct("
        "        'base', cast((id + 50) % 100 as double) + 0.99,"
        "        'discount', cast((id + 5) % 20 as double) / 100.0,"
        "        'tax_rate', 0.08"
        "      )"
        "    ),"
        "    'quantity', cast((id + 1) % 10 + 1 as int)"
        "  )"
        ") as line_items",

        # Map with struct values (73)
        "map("
        "  'primary', named_struct('name', concat('Primary_', cast(id % 100 as string)), 'score', cast(id % 100 as double), 'active', true),"
        "  'secondary', named_struct('name', concat('Secondary_', cast(id % 100 as string)), 'score', cast((id * 2) % 100 as double), 'active', id % 2 = 0)"
        ") as contact_map",

        # Struct with map containing arrays (74)
        "named_struct("
        "  'config_name', concat('Config_', cast(id % 100 as string)),"
        "  'settings', map("
        "    'integers', array(cast(id % 10 as int), cast((id + 1) % 10 as int), cast((id + 2) % 10 as int)),"
        "    'strings', array(concat('s1_', cast(id % 10 as string)), concat('s2_', cast(id % 10 as string)))"
        "  ),"
        "  'enabled', id % 2 = 0"
        ") as config_with_map",

        # Array of arrays (75)
        "array("
        "  array(cast(id % 10 as int), cast((id + 1) % 10 as int), cast((id + 2) % 10 as int)),"
        "  array(cast((id * 2) % 10 as int), cast((id * 2 + 1) % 10 as int)),"
        "  array(cast((id * 3) % 10 as int), cast((id * 3 + 1) % 10 as int), cast((id * 3 + 2) % 10 as int), cast((id * 3 + 3) % 10 as int))"
        ") as nested_int_arrays",

        # Array of maps (76)
        "array("
        "  map('a', cast(id % 100 as string), 'b', cast((id + 1) % 100 as string)),"
        "  map('x', cast((id * 2) % 100 as string), 'y', cast((id * 2 + 1) % 100 as string), 'z', cast((id * 2 + 2) % 100 as string))"
        ") as array_of_maps",

        # Map with array values (77)
        "map("
        "  'scores', array(cast(id % 100 as double), cast((id * 2) % 100 as double), cast((id * 3) % 100 as double)),"
        "  'ranks', array(cast(id % 10 as double), cast((id + 1) % 10 as double))"
        ") as map_with_arrays",

        # Complex event structure (78)
        "named_struct("
        "  'event_id', concat('EVT-', cast(id as string)),"
        "  'event_type', array('click', 'view', 'purchase', 'signup')[cast(id % 4 as int)],"
        "  'timestamp', id * 1000,"
        "  'properties', map("
        "    'source', array('web', 'mobile', 'api')[cast(id % 3 as int)],"
        "    'campaign', concat('camp_', cast(id % 50 as string))"
        "  ),"
        "  'user', named_struct("
        "    'id', cast(id % 100000 as int),"
        "    'segment', array('new', 'returning', 'premium')[cast(id % 3 as int)],"
        "    'attributes', named_struct("
        "      'age_group', array('18-24', '25-34', '35-44', '45+')[cast(id % 4 as int)],"
        "      'interests', array(concat('int_', cast(id % 10 as string)), concat('int_', cast((id + 1) % 10 as string)))"
        "    )"
        "  )"
        ") as event_data",

        # Financial transaction with deep nesting (79)
        "named_struct("
        "  'txn_id', concat('TXN-', cast(id as string)),"
        "  'amount', named_struct("
        "    'value', cast(id % 10000 as double) / 100.0,"
        "    'currency', array('USD', 'EUR', 'GBP')[cast(id % 3 as int)],"
        "    'exchange', named_struct("
        "      'rate', 1.0 + cast(id % 100 as double) / 1000.0,"
        "      'source', 'market',"
        "      'timestamp', id * 1000"
        "    )"
        "  ),"
        "  'parties', named_struct("
        "    'sender', named_struct("
        "      'account', concat('ACC-', lpad(cast(id % 100000 as string), 8, '0')),"
        "      'bank', named_struct("
        "        'code', concat('BNK-', cast(id % 100 as string)),"
        "        'country', array('US', 'UK', 'DE', 'JP')[cast(id % 4 as int)]"
        "      )"
        "    ),"
        "    'receiver', named_struct("
        "      'account', concat('ACC-', lpad(cast((id + 50000) % 100000 as string), 8, '0')),"
        "      'bank', named_struct("
        "        'code', concat('BNK-', cast((id + 50) % 100 as string)),"
        "        'country', array('US', 'UK', 'DE', 'JP')[cast((id + 1) % 4 as int)]"
        "      )"
        "    )"
        "  )"
        ") as financial_txn",

        # Product catalog entry (80)
        "named_struct("
        "  'product_id', concat('PROD-', lpad(cast(id % 10000 as string), 6, '0')),"
        "  'variants', array("
        "    named_struct("
        "      'sku', concat('VAR-', cast(id % 1000 as string), '-A'),"
        "      'attributes', map('color', 'red', 'size', 'S'),"
        "      'inventory', named_struct('quantity', cast(id % 100 as int), 'warehouse', 'WH-1')"
        "    ),"
        "    named_struct("
        "      'sku', concat('VAR-', cast(id % 1000 as string), '-B'),"
        "      'attributes', map('color', 'blue', 'size', 'M'),"
        "      'inventory', named_struct('quantity', cast((id + 10) % 100 as int), 'warehouse', 'WH-2')"
        "    )"
        "  ),"
        "  'pricing', named_struct("
        "    'list_price', cast(id % 1000 as double) + 0.99,"
        "    'tiers', array("
        "      named_struct('min_qty', 1, 'price', cast(id % 1000 as double) + 0.99),"
        "      named_struct('min_qty', 10, 'price', cast(id % 1000 as double) * 0.9 + 0.99),"
        "      named_struct('min_qty', 100, 'price', cast(id % 1000 as double) * 0.8 + 0.99)"
        "    )"
        "  )"
        ") as product_catalog",

        # Additional scalar columns (81-90)
        "cast((id * 53) % 10000 as int) as metric_1",
        "cast((id * 59) % 10000 as int) as metric_2",
        "cast((id * 61) % 10000 as int) as metric_3",
        "cast((id * 67) % 1000000 as long) as counter_1",
        "cast((id * 71) % 1000000 as long) as counter_2",
        "cast((id * 73) % 10000 as double) / 100.0 as measure_1",
        "cast((id * 79) % 10000 as double) / 100.0 as measure_2",
        "concat('label_', cast(id % 500 as string)) as label_1",
        "concat('category_', cast(id % 200 as string)) as label_2",
        "id % 17 = 0 as flag_1",

        # Additional complex columns (91-95)
        "array("
        "  named_struct('ts', id * 1000, 'value', cast(id % 100 as double)),"
        "  named_struct('ts', id * 1000 + 1000, 'value', cast((id + 1) % 100 as double)),"
        "  named_struct('ts', id * 1000 + 2000, 'value', cast((id + 2) % 100 as double))"
        ") as time_series",

        "map("
        "  'en', concat('English text ', cast(id % 100 as string)),"
        "  'es', concat('Spanish texto ', cast(id % 100 as string)),"
        "  'fr', concat('French texte ', cast(id % 100 as string))"
        ") as translations",

        "named_struct("
        "  'rules', array("
        "    named_struct('id', cast(id % 100 as int), 'condition', concat('cond_', cast(id % 10 as string)), 'action', concat('act_', cast(id % 5 as string))),"
        "    named_struct('id', cast((id + 1) % 100 as int), 'condition', concat('cond_', cast((id + 1) % 10 as string)), 'action', concat('act_', cast((id + 1) % 5 as string)))"
        "  ),"
        "  'default_action', 'none',"
        "  'priority', cast(id % 10 as int)"
        ") as rule_engine",

        "array("
        "  map('metric', 'cpu', 'value', cast(id % 100 as double), 'unit', 'percent'),"
        "  map('metric', 'memory', 'value', cast((id * 2) % 100 as double), 'unit', 'percent'),"
        "  map('metric', 'disk', 'value', cast((id * 3) % 100 as double), 'unit', 'percent')"
        ") as system_metrics",

        "named_struct("
        "  'permissions', map("
        "    'read', array('user', 'admin'),"
        "    'write', array('admin'),"
        "    'delete', array('admin')"
        "  ),"
        "  'roles', array("
        "    named_struct('name', 'viewer', 'level', 1),"
        "    named_struct('name', 'editor', 'level', 2),"
        "    named_struct('name', 'admin', 'level', 3)"
        "  )"
        ") as access_control",

        # Final columns (96-100)
        "cast((id * 83) % 1000 as int) as final_metric_1",
        "cast((id * 89) % 10000 as double) / 100.0 as final_measure_1",
        "concat('final_', cast(id % 1000 as string)) as final_label",
        "array(cast(id % 5 as int), cast((id + 1) % 5 as int), cast((id + 2) % 5 as int), cast((id + 3) % 5 as int), cast((id + 4) % 5 as int)) as final_array",
        "named_struct("
        "  'summary', concat('Summary for record ', cast(id as string)),"
        "  'checksum', concat(cast(id % 256 as string), '-', cast((id * 7) % 256 as string), '-', cast((id * 13) % 256 as string)),"
        "  'version', cast(id % 100 as int)"
        ") as final_metadata"
    )

    print(f"Generated schema with {len(df.columns)} columns")
    df.printSchema()

    # Write as parquet
    df.write.mode("overwrite").parquet(output_path)

    # Verify the data
    written_df = spark.read.parquet(output_path)
    actual_count = written_df.count()
    print(f"Wrote {actual_count:,} rows to {output_path}")

    spark.stop()


def main():
    parser = argparse.ArgumentParser(
        description="Generate test data for shuffle benchmark"
    )
    parser.add_argument(
        "--output", "-o",
        required=True,
        help="Output path for parquet data (local path or hdfs://...)"
    )
    parser.add_argument(
        "--rows", "-r",
        type=int,
        default=10_000_000,
        help="Number of rows to generate (default: 10000000 for ~1GB with wide schema)"
    )
    parser.add_argument(
        "--partitions", "-p",
        type=int,
        default=None,
        help="Number of output partitions (default: auto based on cluster)"
    )

    args = parser.parse_args()

    # Default partitions to a reasonable number if not specified
    num_partitions = args.partitions if args.partitions else 200

    generate_data(args.output, args.rows, num_partitions)


if __name__ == "__main__":
    main()
