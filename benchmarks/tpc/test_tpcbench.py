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

"""Run with spark-submit test_tpcbench.py; no additional test dependencies."""

import io
from datetime import date
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
from unittest.mock import patch

from pyspark.sql import SparkSession
from pyspark.sql.types import CharType, StringType, VarcharType, _parse_datatype_string

import tpcbench
from tpcds_schema import TABLE_SCHEMAS


def check_types(spark):
    assert len(TABLE_SCHEMAS) == 24
    for table, ddl in TABLE_SCHEMAS.items():
        expected = _parse_datatype_string(ddl)
        actual = spark.table(table).schema
        assert actual.names == expected.names, table
        for field, declared in zip(actual, expected):
            if isinstance(declared.dataType, (CharType, VarcharType)):
                assert field.dataType == StringType(), (table, field)
                assert field.metadata["__CHAR_VARCHAR_TYPE_STRING"] == declared.dataType.simpleString()
            else:
                assert field.dataType == declared.dataType, (table, field)
    assert spark.table("item").schema["i_category"].metadata == {
        "__CHAR_VARCHAR_TYPE_STRING": "char(50)"
    }


def check_values(spark):
    predicates = {
        "i_category = 'Men'": [1, 2],
        "i_category = 'Men   '": [1, 2],
        "i_category = ' Men'": [4],
        "i_category = i_item_id": [1, 2],
        "i_category IS NULL": [3],
        "i_category = NULL": [],
        "i_category IN ('other', NULL)": [],
        "i_category <=> NULL": [3],
        "i_item_desc = 'Men'": [1, 4],
        "i_item_desc = 'Men '": [2],
    }
    for predicate, expected in predicates.items():
        actual = [r[0] for r in spark.sql(
            f"SELECT i_item_sk FROM item WHERE {predicate} ORDER BY i_item_sk"
        ).collect()]
        assert actual == expected, (predicate, actual, expected)
    rows = spark.sql("SELECT * FROM item ORDER BY i_item_sk").collect()
    assert all(tuple(r) == (None, None) for r in spark.sql(
        "SELECT i_category = NULL, i_category IN (NULL) FROM item"
    ).collect())
    assert rows[0].i_category == rows[1].i_category == "Men".ljust(50)
    assert rows[0].i_rec_start_date == date(2020, 1, 2)
    assert rows[0].i_current_price == Decimal("12.34")
    assert rows[2].i_category is None and rows[2].i_item_desc is None
    assert rows[4].i_category == "M" * 51  # External values are not truncated.
    assert rows[4].i_item_desc == "x" * 201
    joined = spark.sql("""SELECT a.i_item_sk, b.i_item_sk FROM item a JOIN item b
        ON a.i_item_id = b.i_category WHERE a.i_item_sk <= 2 ORDER BY 1, 2""").collect()
    assert [tuple(r) for r in joined] == [(1, 1), (1, 2), (2, 1), (2, 2)]


def manifest(directory):
    return {str(p.relative_to(directory)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in directory.rglob("*") if p.is_file()}


def main():
    spark = SparkSession.builder.getOrCreate()
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        data = root / "data'quoted"
        # Raw Parquet has STRING, not CHAR. Include both padded and unpadded values.
        spark.createDataFrame(
            [(1, "Men", "shirts", "Men", "Men", date(2020, 1, 2), Decimal("12.34")),
             (2, "Men".ljust(50), "shirts".ljust(50), "Men".ljust(16), "Men ", None, None),
             (3, None, None, None, None, None, None),
             (4, " Men", "shirts", "Men", "Men", None, None),
             (5, "M" * 51, "shirts", "M" * 17, "x" * 201, None, None)],
            "i_item_sk INT, i_category STRING, i_class STRING, i_item_id STRING, "
            "i_item_desc STRING, i_rec_start_date DATE, i_current_price DECIMAL(7,2)",
        ).write.parquet(str(data / "item"))
        tables = (
            "call_center catalog_page catalog_returns catalog_sales customer "
            "customer_address customer_demographics date_dim time_dim "
            "household_demographics income_band inventory promotion reason "
            "ship_mode store store_returns store_sales warehouse web_page "
            "web_returns web_sales web_site"
        ).split()
        for table in tables:
            shutil.copytree(data / "item", data / table)
        before = manifest(data)
        databases = spark.sql("SHOW DATABASES IN spark_catalog").collect()

        query = "SELECT i_item_sk FROM item WHERE i_category = 'Men' AND i_class = 'shirts'"
        original_open = open
        query_file = str(Path(tpcbench.__file__).parent / "queries/tpcds/q1.sql")

        def open_query(path, *args, **kwargs):
            if str(path) == query_file:
                return io.StringIO(query)
            return original_open(path, *args, **kwargs)

        # Substitute only the query fixture; execute actual main registration and collection.
        with patch("builtins.open", side_effect=open_query), patch.object(SparkSession, "stop"):
            tpcbench.main("tpcds", str(data), None, None, 1, str(root),
                          "registration", "parquet", query_num=1)
        result = json.loads(next(root.glob("registration-*.json")).read_text())
        assert result["1"]["row_count"] == 2, result["1"]
        assert result["1"]["result_hash"] == tpcbench.result_hash(
            spark.sql("SELECT 1 AS i_item_sk UNION ALL SELECT 2").collect()
        )
        check_types(spark)
        check_values(spark)
        tpcbench.register_tables(spark, "tpcds", ["item"], str(data), None, None,
                                "PARQUET", {})
        assert spark.table("item").schema["i_category"].metadata == {
            "__CHAR_VARCHAR_TYPE_STRING": "char(50)"
        }
        # Repeated registration uses fresh isolated metadata; both directory layouts work.
        (data / "item").rename(data / "item.parquet")
        renamed = manifest(data)
        for _ in range(2):
            tpcbench.register_tables(spark, "tpcds", ["item"], str(data), None, None,
                                    "parquet", {})
            check_values(spark)
            assert spark.sql("SHOW DATABASES IN spark_catalog").collect() == databases
            assert manifest(data) == renamed

        # Reader options must reach the external table, not only the inferred path.
        try:
            tpcbench.register_tables(spark, "tpcds", ["item"], str(data), None, None,
                                    "parquet", {"mergeSchema": True})
        except Exception as error:
            raise AssertionError("Boolean reader option mergeSchema=True must succeed") from error
        assert spark.table("item").count() == 5
        tpcbench.register_tables(spark, "tpcds", ["item"], str(data), None, None,
                                "parquet", {"pathGlobFilter": "no-match-*.parquet"})
        assert spark.table("item").count() == 0
        assert spark.sql("SHOW DATABASES IN spark_catalog").collect() == databases

        # Real catalog creation failure after a successful table must release metadata.
        bad_layout = root / "bad-layout"
        shutil.copytree(data / "item.parquet", bad_layout / "item")
        for partition in ("a=1", "b=1"):
            shutil.copytree(data / "item.parquet", bad_layout / "customer" / partition)
        bad_before = manifest(bad_layout)
        try:
            tpcbench.register_tables(spark, "tpcds", ["item", "customer"], str(bad_layout),
                                    None, None, "parquet", {})
        except Exception as error:
            assert "Conflicting partition column names" in str(error), str(error)
        else:
            raise AssertionError("Conflicting partition columns should fail registration")
        assert spark.table("item").count() == 5
        assert spark.sql("SHOW DATABASES IN spark_catalog").collect() == databases
        assert manifest(bad_layout) == bad_before
        assert manifest(data) == renamed

        # TPC-H files retain STRING comparisons even if columns resemble TPC-DS.
        tpcbench.register_tables(spark, "tpch", ["item"], str(data), None, None,
                                "parquet", {})
        assert [r[0] for r in spark.sql(
            "SELECT i_item_sk FROM item WHERE i_category = 'Men'"
        ).collect()] == [1]
        assert spark.table("item").schema["i_category"].metadata == {}
        # Non-Parquet TPC-DS and catalog inputs likewise retain source types.
        csv = root / "csv"
        spark.read.parquet(str(data / "item.parquet")).write.option("header", True).csv(str(csv / "item"))
        tpcbench.register_tables(spark, "tpcds", ["item"], str(csv), None, None,
                                "csv", {"header": "true"})
        assert spark.table("item").schema["i_category"].metadata == {}
        assert spark.table("item").count() == 5
        json_data = root / "json"
        spark.read.parquet(str(data / "item.parquet")).write.json(str(json_data / "item"))
        tpcbench.register_tables(spark, "tpcds", ["item"], str(json_data), None, None,
                                "json", {})
        assert spark.table("item").schema["i_category"].metadata == {}
        assert spark.sql("SELECT * FROM item WHERE i_category = 'Men'").count() == 1
        spark.sql("CREATE DATABASE catalog_input")
        try:
            spark.catalog.createTable("spark_catalog.catalog_input.item", source="parquet",
                                      path=str(data / "item.parquet"))
            tpcbench.register_tables(spark, "tpcds", ["item"], None, "spark_catalog",
                                    "catalog_input", "parquet", {})
            assert spark.table("item").schema["i_category"].metadata == {}
            assert spark.sql("SELECT * FROM item WHERE i_category = 'Men'").count() == 1
        finally:
            spark.sql("DROP DATABASE catalog_input CASCADE")
        (data / "item.parquet").rename(data / "item")
        assert manifest(data) == before
    spark.stop()
    print("TPC-DS registration checks passed")


if __name__ == "__main__":
    main()
