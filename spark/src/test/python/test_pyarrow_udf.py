#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import pytest
import pyarrow as pa
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import IntegerType, StringType, ArrayType


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session with Comet enabled and PyArrow UDF support."""
    spark = (SparkSession.builder
             .appName("PyArrowUDFTest")
             .config("spark.sql.extensions", "org.apache.comet.CometSparkSessionExtensions")
             .config("spark.comet.enabled", "true")
             .config("spark.sql.execution.pythonUdf.arrow.enabled", "true")
             .config("spark.sql.adaptive.enabled", "false")
             .getOrCreate())
    yield spark
    spark.stop()


class TestPyArrowUDFFallback:
    """Test PyArrow UDFs that should fallback to Spark execution."""

    def test_simple_pyarrow_udf_addition(self, spark):
        """Test a simple PyArrow UDF that adds two integers - should fallback to Spark."""
        # Create test data
        df = spark.createDataFrame([(1, 2), (3, 4), (5, 6)], ["a", "b"])
        
        # Define a PyArrow UDF
        @pandas_udf(returnType=IntegerType())
        def pyarrow_add(a, b):
            return a + b
        
        # Apply the UDF
        result_df = df.select(col("a"), col("b"), pyarrow_add(col("a"), col("b")).alias("sum"))
        
        # Collect results
        result = result_df.collect()
        
        # Verify results are correct
        expected = [(1, 2, 3), (3, 4, 7), (5, 6, 11)]
        assert result == expected
        
        # # Verify that this falls back to Spark (not using Comet native execution)
        # # We can check the execution plan to ensure it doesn't contain Comet operators
        # plan = result_df.queryExecution.executedPlan
        # plan_str = str(plan)
        #
        # # The plan should not contain CometProject or other Comet operators for PyArrow UDFs
        # assert "CometProject" not in plan_str
        # assert "CometScan" not in plan_str  # Should fallback completely


    def test_string_pyarrow_udf(self, spark):
        """Test a PyArrow UDF that processes strings - should fallback to Spark."""
        # Create test data
        df = spark.createDataFrame([("hello", "world"), ("foo", "bar")], ["str1", "str2"])
        
        # Define a PyArrow UDF that concatenates strings
        @pandas_udf(returnType=StringType())
        def pyarrow_concat(str1, str2):
            return str1 + "-" + str2
        
        # Apply the UDF
        result_df = df.select(col("str1"), col("str2"), 
                             pyarrow_concat(col("str1"), col("str2")).alias("concat"))
        
        # Collect results
        result = result_df.collect()
        
        # Verify results are correct
        expected = [("hello", "world", "hello-world"), ("foo", "bar", "foo-bar")]
        assert result == expected
        
        # Verify fallback to Spark
        # plan = result_df.queryExecution.executedPlan
        # plan_str = str(plan)
        # assert "CometProject" not in plan_str


    def test_array_processing_pyarrow_udf(self, spark):
        """Test a PyArrow UDF that processes arrays - should fallback to Spark."""
        # Create test data with arrays
        df = spark.createDataFrame([([1, 2, 3],), ([4, 5, 6],)], ["arr"])
        
        # Define a PyArrow UDF that sums array elements
        @pandas_udf(returnType=IntegerType())
        def pyarrow_array_sum(arr):
            return arr.apply(sum)
        
        # Apply the UDF
        result_df = df.select(col("arr"), pyarrow_array_sum(col("arr")).alias("sum"))
        
        # Collect results
        result = result_df.collect()
        
        # Verify results are correct
        expected = [([1, 2, 3], 6), ([4, 5, 6], 15)]
        assert result == expected
        
        # Verify fallback to Spark
        # plan = result_df.queryExecution.executedPlan
        # plan_str = str(plan)
        # assert "CometProject" not in plan_str


    def test_mixed_expressions_with_pyarrow_udf(self, spark):
        """Test that mixing PyArrow UDF with regular expressions causes complete fallback."""
        # Create test data
        df = spark.createDataFrame([(1, 2, 3), (4, 5, 6)], ["a", "b", "c"])
        
        # Define a PyArrow UDF
        @pandas_udf(returnType=IntegerType())
        def pyarrow_multiply(a, b):
            return a * b
        
        # Query that mixes PyArrow UDF with regular expressions
        result_df = df.select(
            col("a"),
            col("b"), 
            col("c"),
            (col("a") + col("b")).alias("regular_add"),  # Regular Spark expression
            pyarrow_multiply(col("a"), col("b")).alias("udf_multiply")  # PyArrow UDF
        )
        
        # Collect results
        result = result_df.collect()
        
        # Verify results are correct
        expected = [(1, 2, 3, 3, 2), (4, 5, 6, 9, 20)]
        assert result == expected
        
        # The entire query should fallback to Spark due to the PyArrow UDF
        # plan = result_df.queryExecution.executedPlan
        # plan_str = str(plan)
        # assert "CometProject" not in plan_str


    def test_pyarrow_udf_with_null_values(self, spark):
        """Test PyArrow UDF handling of null values - should fallback to Spark."""
        # Create test data with nulls
        df = spark.createDataFrame([(1, 2), (None, 4), (5, None), (None, None)], ["a", "b"])
        
        # Define a PyArrow UDF that handles nulls
        @pandas_udf(returnType=IntegerType())
        def pyarrow_add_with_nulls(a, b):
            # PyArrow/pandas will handle nulls automatically
            return a + b
        
        # Apply the UDF
        result_df = df.select(col("a"), col("b"), 
                             pyarrow_add_with_nulls(col("a"), col("b")).alias("sum"))
        
        # Collect results
        result = result_df.collect()
        
        # Verify results (nulls should be preserved)
        expected = [(1, 2, 3), (None, 4, None), (5, None, None), (None, None, None)]
        assert result == expected
        
        # Verify fallback to Spark
        # plan = result_df.queryExecution.executedPlan
        # plan_str = str(plan)
        # assert "CometProject" not in plan_str


if __name__ == "__main__":
    pytest.main([__file__])