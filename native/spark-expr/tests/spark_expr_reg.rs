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

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use datafusion::error::Result;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::execution::FunctionRegistry;
    use datafusion::prelude::SessionContext;
    use datafusion_comet_spark_expr::create_comet_physical_fun;
    use datafusion_comet_spark_expr::register_all_comet_functions;

    #[tokio::test]
    async fn test_udf_registration() -> Result<()> {
        // 1. Setup session with UDF registration of existing Spark-compatible expression
        let mut session_state = SessionStateBuilder::new().build();
        let _ = session_state.register_udf(create_comet_physical_fun(
            "xxhash64",
            DataType::Int64,
            &session_state,
            None,
        )?);
        let _ = session_state.register_udf(create_comet_physical_fun(
            "aes_decrypt",
            DataType::Binary,
            &session_state,
            None,
        )?);
        let ctx = SessionContext::new_with_state(session_state);

        // 2. Execute SQL with literal values
        let df = ctx
            .sql("SELECT xxhash64('valid',64) AS hash_value")
            .await
            .unwrap();
        let results = df.collect().await?;

        // 3. Ensure results are returned, i.e UDF is registered. no need to validate the actual value
        assert!(!results.is_empty(), "Results should not be empty");

        Ok(())
    }

    #[tokio::test]
    async fn test_make_date_returns_null_for_invalid_input() -> Result<()> {
        // Setup session with all Comet functions registered
        let mut ctx = SessionContext::new();
        register_all_comet_functions(&mut ctx)?;

        // Test that make_date returns NULL for invalid month (0)
        // DataFusion's built-in make_date would throw an error
        let df = ctx.sql("SELECT make_date(2023, 0, 15)").await?;
        let results = df.collect().await?;

        // Should return one row with NULL
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 1);

        // The result should be NULL for invalid input
        let column = results[0].column(0);
        assert!(column.is_null(0), "Expected NULL for invalid month");

        Ok(())
    }

    #[tokio::test]
    async fn test_make_date_valid_input() -> Result<()> {
        // Setup session with all Comet functions registered
        let mut ctx = SessionContext::new();
        register_all_comet_functions(&mut ctx)?;

        // Test that make_date works for valid input
        let df = ctx.sql("SELECT make_date(1970, 1, 1)").await?;
        let results = df.collect().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 1);

        // Should return epoch date (1970-01-01 = day 0)
        let column = results[0].column(0);
        assert!(!column.is_null(0), "Expected valid date for epoch");

        Ok(())
    }
}
