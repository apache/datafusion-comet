package org.apache.spark.sql.comet.shims

trait ShimCometTPCDSQuerySuite {
  // This is private in `TPCDSBase`.
  val excludedTpcdsQueries: Set[String] = Set()
}
