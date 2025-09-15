package org.apache.comet

import org.apache.comet.testing.{DataGenOptions, ParquetGenerator}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.comet.{CometNativeScanExec, CometScanExec}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.scalactic.source.Position
import org.scalatest.Tag

import java.io.File
import java.text.SimpleDateFormat
import scala.util.Random

class CometFuzzTestBase extends CometTestBase with AdaptiveSparkPlanHelper {

  var filename: String = null

  /**
   * We use Asia/Kathmandu because it has a non-zero number of minutes as the offset, so is an
   * interesting edge case. Also, this timezone tends to be different from the default system
   * timezone.
   *
   * Represents UTC+5:45
   */
  val defaultTimezone = "Asia/Kathmandu"

  override def beforeAll(): Unit = {
    super.beforeAll()
    val tempDir = System.getProperty("java.io.tmpdir")
    filename = s"$tempDir/CometFuzzTestSuite_${System.currentTimeMillis()}.parquet"
    val random = new Random(42)
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "false",
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> defaultTimezone) {
      val options =
        DataGenOptions(
          generateArray = true,
          generateStruct = true,
          generateNegativeZero = false,
          // override base date due to known issues with experimental scans
          baseDate =
            new SimpleDateFormat("YYYY-MM-DD hh:mm:ss").parse("2024-05-25 12:34:56").getTime)
      ParquetGenerator.makeParquetFile(random, spark, filename, 1000, options)
    }
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    FileUtils.deleteDirectory(new File(filename))
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
                                                                                 pos: Position): Unit = {
    Seq("native", "jvm").foreach { shuffleMode =>
      Seq(
        CometConf.SCAN_NATIVE_COMET,
        CometConf.SCAN_NATIVE_DATAFUSION,
        CometConf.SCAN_NATIVE_ICEBERG_COMPAT).foreach { scanImpl =>
        super.test(testName + s" ($scanImpl, $shuffleMode shuffle)", testTags: _*) {
          withSQLConf(
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> scanImpl,
            CometConf.COMET_SCAN_ALLOW_INCOMPATIBLE.key -> "true",
            CometConf.COMET_SHUFFLE_MODE.key -> shuffleMode) {
            testFun
          }
        }
      }
    }
  }

  def collectNativeScans(plan: SparkPlan): Seq[SparkPlan] = {
    collect(plan) {
      case scan: CometScanExec => scan
      case scan: CometNativeScanExec => scan
    }
  }

  def collectCometShuffleExchanges(plan: SparkPlan): Seq[SparkPlan] = {
    collect(plan) { case exchange: CometShuffleExchangeExec =>
      exchange
    }
  }

}
