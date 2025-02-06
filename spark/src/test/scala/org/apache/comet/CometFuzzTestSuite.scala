package org.apache.comet

import org.apache.comet.testing.{DataGenOptions, ParquetGenerator}
import org.apache.hadoop.fs.Path
import org.apache.parquet.format.IntType
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.types.FloatType

import scala.util.Random

class CometFuzzTestSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  test("aggregates") {
    withTempDir { dir =>
      val path = new Path(dir.toURI.toString, "test.parquet")
      val filename = path.toString
      val random = new Random(42)
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        ParquetGenerator.makeParquetFile(random, spark, filename, 10000, DataGenOptions())
      }
      val table = spark.read.parquet(filename).coalesce(1)
      table.createOrReplaceTempView("t1")

      val numericAggs = Seq("min", "max", "sum", "avg", "count", "median",
        "stddev", "stddev_pop", "stddev_samp", "var", "var_pop", "var_samp",
        "corr", "covar_pop", "covar_samp",
      "skewness", "kurtosis")

      /*
      ![62,1.2417965031048598E38]
       [62,1.2417965031048596E38]
       */

      // p[ercentile_approx

      val numericFields = table.schema.fields.filter { field =>
        field.dataType match {
          case _: IntType | _: FloatType => true
          case _ => false
        }
      }

      for (agg <- numericAggs) {
        for (field <- numericFields) {
          val sql = s"SELECT c1, $agg(${field.name}) FROM t1 GROUP BY c1 ORDER BY c1"
          println(sql)
          checkSparkAnswerWithTol(sql)
        }
      }
    }
  }
}


