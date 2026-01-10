#!/usr/bin/env python3
"""
Generate test data for shuffle size comparison benchmark.

This script generates a parquet dataset with a realistic schema (50 columns
including nested structs and arrays) for benchmarking shuffle operations
across Spark, Comet JVM, and Comet Native shuffle modes.
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
    print("Schema: 50 columns including nested structs and arrays")

    # Start with a range and build up the columns
    df = spark.range(0, num_rows, numPartitions=num_partitions)

    # Add columns using selectExpr for better performance
    df = df.selectExpr(
        # Key columns for grouping/partitioning
        "cast(id % 1000 as int) as partition_key",          # 1
        "cast(id % 100 as int) as group_key",               # 2
        "id as row_id",                                      # 3

        # Integer columns
        "cast(id % 10000 as int) as category_id",           # 4
        "cast(id % 500 as int) as region_id",               # 5
        "cast(id % 50 as int) as department_id",            # 6
        "cast((id * 7) % 1000000 as int) as customer_id",   # 7
        "cast((id * 13) % 100000 as int) as product_id",    # 8
        "cast(id % 12 + 1 as int) as month",                # 9
        "cast(id % 28 + 1 as int) as day",                  # 10
        "cast(2020 + (id % 5) as int) as year",             # 11

        # Long columns
        "id * 1000 as transaction_id",                       # 12
        "(id * 17) % 10000000000 as account_number",        # 13
        "(id * 31) % 1000000000 as reference_id",           # 14

        # Double columns
        "cast(id % 10000 as double) / 100.0 as amount",     # 15
        "cast((id * 3) % 10000 as double) / 100.0 as price", # 16
        "cast(id % 100 as double) / 100.0 as discount",     # 17
        "cast((id * 7) % 500 as double) / 10.0 as weight",  # 18
        "cast((id * 11) % 1000 as double) / 10.0 as height", # 19
        "cast(id % 360 as double) as latitude",             # 20
        "cast((id * 2) % 360 as double) as longitude",      # 21

        # String columns (various lengths)
        "concat('user_', cast(id % 100000 as string)) as user_name",  # 22
        "concat('email_', cast(id % 50000 as string), '@example.com') as email",  # 23
        "concat('SKU-', lpad(cast(id % 10000 as string), 6, '0')) as sku",  # 24
        "concat('ORD-', cast(id as string)) as order_id",   # 25
        "array('pending', 'processing', 'shipped', 'delivered', 'cancelled')[cast(id % 5 as int)] as status",  # 26
        "array('USD', 'EUR', 'GBP', 'JPY', 'CAD')[cast(id % 5 as int)] as currency",  # 27
        "concat('Description for item ', cast(id % 1000 as string), ' with additional details and specifications') as description",  # 28

        # Boolean columns
        "id % 2 = 0 as is_active",                          # 29
        "id % 3 = 0 as is_verified",                        # 30
        "id % 7 = 0 as is_premium",                         # 31

        # Date and timestamp columns
        "date_add(to_date('2020-01-01'), cast(id % 1500 as int)) as created_date",  # 32
        "date_add(to_date('2020-01-01'), cast((id + 30) % 1500 as int)) as updated_date",  # 33
        "to_timestamp(concat('2020-01-01 ', lpad(cast(id % 24 as string), 2, '0'), ':00:00')) as created_at",  # 34

        # Nested struct: address
        "named_struct("
        "  'street', concat(cast(id % 9999 as string), ' Main St'),"
        "  'city', array('New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix')[cast(id % 5 as int)],"
        "  'state', array('NY', 'CA', 'IL', 'TX', 'AZ')[cast(id % 5 as int)],"
        "  'zip', concat(lpad(cast(id % 99999 as string), 5, '0')),"
        "  'country', 'USA'"
        ") as address",                                      # 35

        # Nested struct: metadata
        "named_struct("
        "  'source', array('web', 'mobile', 'api', 'batch')[cast(id % 4 as int)],"
        "  'version', concat('v', cast(1 + id % 10 as string), '.', cast(id % 100 as string)),"
        "  'flags', named_struct("
        "    'flag_a', id % 2 = 0,"
        "    'flag_b', id % 3 = 0,"
        "    'flag_c', id % 5 = 0"
        "  )"
        ") as metadata",                                     # 36

        # Nested struct: dimensions
        "named_struct("
        "  'length', cast(id % 100 as double) + 0.5,"
        "  'width', cast((id * 3) % 100 as double) + 0.5,"
        "  'height', cast((id * 7) % 100 as double) + 0.5,"
        "  'unit', 'cm'"
        ") as dimensions",                                   # 37

        # Array columns
        "array(cast(id % 100 as int), cast((id + 1) % 100 as int), cast((id + 2) % 100 as int)) as tag_ids",  # 38
        "array("
        "  cast(id % 1000 as double) / 10.0,"
        "  cast((id * 2) % 1000 as double) / 10.0,"
        "  cast((id * 3) % 1000 as double) / 10.0,"
        "  cast((id * 4) % 1000 as double) / 10.0"
        ") as scores",                                       # 39
        "array("
        "  concat('tag_', cast(id % 20 as string)),"
        "  concat('tag_', cast((id + 5) % 20 as string)),"
        "  concat('tag_', cast((id + 10) % 20 as string))"
        ") as tags",                                         # 40

        # Array of structs
        "array("
        "  named_struct('item_id', cast(id % 1000 as int), 'quantity', cast(id % 10 + 1 as int), 'unit_price', cast(id % 100 as double) + 0.99),"
        "  named_struct('item_id', cast((id + 100) % 1000 as int), 'quantity', cast((id + 1) % 10 + 1 as int), 'unit_price', cast((id + 50) % 100 as double) + 0.99)"
        ") as line_items",                                   # 41

        # More scalar columns to reach 50
        "cast(id % 255 as int) as priority",                # 42
        "cast((id * 19) % 1000000 as long) as sequence_num", # 43
        "cast(id % 10000 as double) * 0.0001 as ratio",     # 44
        "concat('CODE-', lpad(cast(id % 100000 as string), 8, '0')) as reference_code",  # 45
        "id % 4 = 0 as needs_review",                       # 46
        "array('A', 'B', 'C', 'D', 'E')[cast(id % 5 as int)] as grade",  # 47
        "cast((id * 23) % 100 as double) / 100.0 as confidence_score",  # 48

        # Map column
        "map("
        "  'key1', cast(id % 100 as string),"
        "  'key2', cast((id * 2) % 100 as string),"
        "  'key3', cast((id * 3) % 100 as string)"
        ") as attributes",                                   # 49

        # Final nested struct with arrays
        "named_struct("
        "  'primary_contact', named_struct("
        "    'name', concat('Contact ', cast(id % 1000 as string)),"
        "    'phone', concat('+1-555-', lpad(cast(id % 10000 as string), 4, '0')),"
        "    'emails', array("
        "      concat('primary_', cast(id % 1000 as string), '@example.com'),"
        "      concat('secondary_', cast(id % 1000 as string), '@example.com')"
        "    )"
        "  ),"
        "  'notes', array("
        "    concat('Note 1 for record ', cast(id as string)),"
        "    concat('Note 2 for record ', cast(id as string))"
        "  )"
        ") as contact_info"                                  # 50
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
