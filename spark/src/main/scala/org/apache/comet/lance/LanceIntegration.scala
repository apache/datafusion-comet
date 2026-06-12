/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.comet.lance

import java.lang.reflect.InvocationTargetException
import java.util.{Optional => JOptional}

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason

/**
 * Reflection-only bridge for optional Lance Spark integration.
 *
 * Default Comet builds must not depend on Lance classes. This object treats both Lance Spark and
 * the Comet contrib-lance scaffold as optional runtime classes and falls back cleanly when either
 * side is absent.
 */
object LanceIntegration extends Logging {

  private val LanceScanClassName = "org.lance.spark.read.LanceScan"
  private val NativeScanPlanMethod = "nativeScanPlan"
  private val ContribSupportModule = "org.apache.comet.lance.CometLanceSupport$"

  def isLanceScan(scan: Any): Boolean = {
    scan != null && {
      scan.getClass.getName == LanceScanClassName ||
      loadClass(LanceScanClassName).exists(_.isInstance(scan))
    }
  }

  def nativeScanPlan(scan: Any): Option[Any] =
    if (isLanceScan(scan)) {
      invokeNativeScanPlan(scan)
    } else {
      None
    }

  def tryCreateNativeScan(scanExec: BatchScanExec): Option[SparkPlan] = {
    if (!CometConf.COMET_LANCE_NATIVE_ENABLED.get(scanExec.conf)) {
      withFallbackReason(
        scanExec,
        s"Native Lance scan disabled because ${CometConf.COMET_LANCE_NATIVE_ENABLED.key} " +
          "is not enabled")
      return None
    }

    if (!CometConf.COMET_EXEC_ENABLED.get(scanExec.conf)) {
      withFallbackReason(
        scanExec,
        s"Native Lance scan disabled because ${CometConf.COMET_EXEC_ENABLED.key} is not enabled")
      return None
    }

    val nativePlan = nativeScanPlan(scanExec.scan) match {
      case Some(plan) => plan
      case None =>
        withFallbackReason(
          scanExec,
          s"Native Lance scan disabled because $LanceScanClassName.$NativeScanPlanMethod() " +
            "is not available")
        return None
    }

    val support = loadContribSupport match {
      case Some(module) => module
      case None =>
        withFallbackReason(
          scanExec,
          "Native Lance scan disabled because the contrib-lance build profile is not present")
        return None
    }

    try {
      val method =
        support.getClass.getMethod("tryTransform", classOf[BatchScanExec], classOf[Object])
      method.invoke(support, scanExec, nativePlan.asInstanceOf[AnyRef]) match {
        case plan: Option[_] => plan.asInstanceOf[Option[SparkPlan]]
        case other =>
          logWarning(
            "Native Lance scan disabled because contrib-lance returned unexpected " +
              s"result: ${Option(other).map(_.getClass.getName).getOrElse("null")}")
          None
      }
    } catch {
      case e: InvocationTargetException =>
        val cause = Option(e.getCause).getOrElse(e)
        logWarning(
          "Native Lance scan disabled because contrib-lance threw during reflection: " +
            s"${cause.getClass.getName}: ${cause.getMessage}",
          cause)
        None
      case NonFatal(e) =>
        logWarning(s"Native Lance scan disabled by contrib-lance reflection failure: $e")
        None
    }
  }

  private[comet] def invokeNativeScanPlan(scan: Any): Option[Any] = {
    try {
      findNoArgMethod(scan.getClass, NativeScanPlanMethod)
        .flatMap { method =>
          optionalResult(method.invoke(scan))
        }
    } catch {
      case e: InvocationTargetException =>
        logWarning(
          s"Native Lance scan disabled because $NativeScanPlanMethod() threw: " +
            s"${Option(e.getCause).map(_.getMessage).getOrElse(e.getMessage)}")
        None
      case NonFatal(e) =>
        logWarning(s"Native Lance scan disabled by reflection failure: $e")
        None
    }
  }

  private def optionalResult(value: Any): Option[Any] = value match {
    case null => None
    case option: Option[_] => option
    case option: JOptional[_] if option.isPresent => Some(option.get)
    case _: JOptional[_] => None
    case other => Some(other)
  }

  private def findNoArgMethod(
      clazz: Class[_],
      methodName: String): Option[java.lang.reflect.Method] = {
    var current = clazz
    while (current != null) {
      try {
        val method = current.getDeclaredMethod(methodName)
        method.setAccessible(true)
        return Some(method)
      } catch {
        case _: NoSuchMethodException =>
          current = current.getSuperclass
        case NonFatal(_) =>
          return None
      }
    }
    None
  }

  private def loadContribSupport: Option[AnyRef] =
    loadClass(ContribSupportModule).flatMap { clazz =>
      try {
        Some(clazz.getField("MODULE$").get(null).asInstanceOf[AnyRef])
      } catch {
        case NonFatal(_) => None
      }
    }

  private def loadClass(className: String): Option[Class[_]] = {
    try {
      val classLoader = Thread.currentThread().getContextClassLoader
      // scalastyle:off classforname
      val clazz =
        if (classLoader != null) {
          Class.forName(className, false, classLoader)
        } else {
          Class.forName(className)
        }
      // scalastyle:on classforname
      Some(clazz)
    } catch {
      case _: ClassNotFoundException | _: NoClassDefFoundError => None
      case NonFatal(e) =>
        logDebug(s"Unable to load optional class $className", e)
        None
    }
  }
}
