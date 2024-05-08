package org.apache.comet.shims

import org.apache.spark.sql.{SQLQueryTestHelper, SparkSession}

trait ShimCometTPCHQuerySuite extends SQLQueryTestHelper {
  protected def getNormalizedQueryExecutionResult(session: SparkSession, sql: String): (String, Seq[String]) = {
    getNormalizedResult(session, sql)
  }
}
