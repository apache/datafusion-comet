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

    #[tokio::test]
    async fn test_udf_registration() -> Result<()> {
        // 1. Setup session with UDF registration of existing Spark-compatible expression
        let mut session_state = SessionStateBuilder::new().build();
        let _ = session_state.register_udf(create_comet_physical_fun(
            "xxhash64",
            DataType::Int64,
            &session_state,
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
}
