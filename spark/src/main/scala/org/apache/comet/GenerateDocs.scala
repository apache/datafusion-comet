package org.apache.comet

import org.apache.comet.expressions.{CometCast, Compatible, Incompatible, Unsupported}
import org.apache.spark.sql.catalyst.expressions.Cast

import java.io.{BufferedOutputStream, FileOutputStream}
import scala.io.Source

/**
 * Utility for generating markdown documentation from the configs.
 *
 * This is invoked when running `mvn clean package -DskipTests`.
 */
object GenerateDocs {

  def main(args: Array[String]): Unit = {
    generateConfigReference()
    generateCompatReference()
  }

  def generateConfigReference(): Unit = {
    val templateFilename = "docs/source/user-guide/configs-template.md"
    val outputFilename = "docs/source/user-guide/configs.md"
    val w = new BufferedOutputStream(new FileOutputStream(outputFilename))
    for (line <- Source.fromFile(templateFilename).getLines()) {
      if (line.trim == "<!--CONFIG_TABLE-->") {
        val publicConfigs = CometConf.allConfs.filter(_.isPublic)
        val confs = publicConfigs.sortBy(_.key)
        w.write("| Config | Description | Default Value |\n".getBytes)
        w.write("|--------|-------------|---------------|\n".getBytes)
        for (conf <- confs) {
          w.write(s"| ${conf.key} | ${conf.doc.trim} | ${conf.defaultValueString} |\n".getBytes)
        }
      } else {
        w.write(s"${line.trim}\n".getBytes)
      }
    }
    w.close()
  }

  def generateCompatReference(): Unit = {
    val templateFilename = "docs/source/user-guide/compatibility-template.md"
    val outputFilename = "docs/source/user-guide/compatibility.md"
    val w = new BufferedOutputStream(new FileOutputStream(outputFilename))
    for (line <- Source.fromFile(templateFilename).getLines()) {
      if (line.trim == "<!--CAST_TABLE-->") {
        w.write("| From Type | To Type | Compatible? | Notes |\n".getBytes)
        w.write("|-|-|-|-|\n".getBytes)
        for (fromType <- CometCast.supportedTypes) {
          for (toType <- CometCast.supportedTypes) {
            if (Cast.canCast(fromType, toType) && fromType != toType) {
              val fromTypeName = fromType.typeName.replace("(10,2)", "")
              val toTypeName = toType.typeName.replace("(10,2)", "")
              CometCast.isSupported(fromType, toType, None, "LEGACY") match {
                case Compatible =>
                  w.write(s"| $fromTypeName | $toTypeName | Compatible | |".getBytes)
                case Incompatible(Some(reason)) =>
                  w.write(s"| $fromTypeName | $toTypeName | Incompatible | $reason |".getBytes)
                case Incompatible(None) =>
                  w.write(s"| $fromTypeName | $toTypeName | Incompatible | |.getBytes")
                case Unsupported =>
                  w.write(s"| $fromTypeName | $toTypeName | Unsupported | |".getBytes)
              }
            }
          }
        }
      } else {
        w.write(s"${line.trim}\n".getBytes)
      }
    }
    w.close()
  }
}
