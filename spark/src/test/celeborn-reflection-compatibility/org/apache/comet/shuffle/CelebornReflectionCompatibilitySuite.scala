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

package org.apache.comet.shuffle

import java.lang.reflect.{Field, Method, Modifier}
import java.util.{List => JList, Map => JMap, Optional}
import java.util.concurrent.{ConcurrentHashMap, ExecutorService}
import java.util.concurrent.atomic.{AtomicReference, LongAdder}

import org.scalatest.funsuite.AnyFunSuite

class CelebornReflectionCompatibilitySuite extends AnyFunSuite {

  private val classLoader = getClass.getClassLoader

  private lazy val shuffleClient = load("org.apache.celeborn.client.ShuffleClientImpl")
  private lazy val pushState = load("org.apache.celeborn.common.write.PushState")
  private lazy val inFlightRequestTracker =
    load("org.apache.celeborn.common.write.InFlightRequestTracker")
  private lazy val transportClientFactory =
    load("org.apache.celeborn.common.network.client.TransportClientFactory")
  private lazy val transportClient =
    load("org.apache.celeborn.common.network.client.TransportClient")
  private lazy val transportResponseHandler =
    load("org.apache.celeborn.common.network.client.TransportResponseHandler")
  private lazy val pushRequestInfo = load("org.apache.celeborn.common.write.PushRequestInfo")

  private lazy val celebornVersion = Option(shuffleClient.getPackage)
    .flatMap(pkg => Option(pkg.getImplementationVersion))
    .getOrElse(fail("Celeborn client JAR does not declare its implementation version"))

  private lazy val celebornReleaseLine = celebornVersion.split("\\.").take(2).mkString(".")

  test("load a released Celeborn 0.6 or 0.7 client") {
    assert(shuffleClient.getPackage.getImplementationTitle == "celeborn-client-spark-3-shaded")
    assert(
      Set("0.6", "0.7").contains(celebornReleaseLine),
      s"unsupported Celeborn compatibility-test version $celebornVersion")
  }

  test("resolve the Celeborn shuffle client members used by the partition pusher") {
    instanceMethod(
      shuffleClient,
      "pushOrMergeData",
      java.lang.Integer.TYPE,
      java.lang.Integer.TYPE,
      java.lang.Integer.TYPE,
      java.lang.Integer.TYPE,
      java.lang.Integer.TYPE,
      classOf[Array[Byte]],
      java.lang.Integer.TYPE,
      java.lang.Integer.TYPE,
      java.lang.Integer.TYPE,
      java.lang.Integer.TYPE,
      java.lang.Boolean.TYPE,
      java.lang.Boolean.TYPE)
    instanceMethod(
      shuffleClient,
      "cleanup",
      java.lang.Void.TYPE,
      java.lang.Integer.TYPE,
      java.lang.Integer.TYPE,
      java.lang.Integer.TYPE)
    instanceMethod(shuffleClient, "getPushState", pushState, classOf[String])

    instanceField(shuffleClient, "pushStates", classOf[JMap[_, _]])
    instanceField(shuffleClient, "pushDataRetryPool", classOf[ExecutorService])

    celebornReleaseLine match {
      case "0.6" =>
        instanceMethod(
          shuffleClient,
          "mapperEnd",
          java.lang.Void.TYPE,
          java.lang.Integer.TYPE,
          java.lang.Integer.TYPE,
          java.lang.Integer.TYPE,
          java.lang.Integer.TYPE)
        assert(!shuffleClient.getMethods.exists(_.getName == "computeBatchCRC"))
        intercept[NoSuchFieldException](declaredField(shuffleClient, "cryptoHandler"))

      case "0.7" =>
        instanceMethod(
          shuffleClient,
          "mapperEnd",
          java.lang.Void.TYPE,
          java.lang.Integer.TYPE,
          java.lang.Integer.TYPE,
          java.lang.Integer.TYPE,
          java.lang.Integer.TYPE,
          java.lang.Integer.TYPE)
        instanceMethod(
          shuffleClient,
          "computeBatchCRC",
          java.lang.Void.TYPE,
          java.lang.Integer.TYPE,
          java.lang.Integer.TYPE,
          java.lang.Integer.TYPE,
          java.lang.Integer.TYPE,
          classOf[Array[Byte]],
          java.lang.Integer.TYPE,
          java.lang.Integer.TYPE)
        instanceField(shuffleClient, "cryptoHandler", classOf[Optional[_]])

      case other => fail(s"unsupported Celeborn release line $other")
    }
  }

  test("resolve the Celeborn push-state members used for admission") {
    exactInstanceField(pushState, "exception", classOf[AtomicReference[_]])
    instanceField(pushState, "inFlightRequestTracker", inFlightRequestTracker)
    exactInstanceField(inFlightRequestTracker, "totalInflightReqs", classOf[LongAdder])
  }

  test("resolve the Celeborn transport members used to track callback ownership") {
    instanceMethod(shuffleClient, "getDataClientFactory", transportClientFactory)

    val transportClientBootstrap =
      load("org.apache.celeborn.common.network.client.TransportClientBootstrap")
    assert(transportClientBootstrap.isInterface)
    instanceMethod(transportClientBootstrap, "doBootstrap", java.lang.Void.TYPE, transportClient)

    val bootstraps = instanceField(transportClientFactory, "clientBootstraps", classOf[JList[_]])
    assert(bootstraps.getGenericType.getTypeName.contains(transportClientBootstrap.getName))
    instanceField(transportClientFactory, "connectionPool", classOf[JMap[_, _]])

    val clientPool =
      load("org.apache.celeborn.common.network.client.TransportClientFactory$ClientPool")
    val clients = instanceField(clientPool, "clients", classOf[Array[_]])
    assert(clients.getType.getComponentType == transportClient)
    val locks = instanceField(clientPool, "locks", classOf[Array[_]])
    assert(locks.getType.getComponentType == classOf[Object])

    val channel = declaredField(transportClient, "channel")
    assert(!Modifier.isStatic(channel.getModifiers))
    assert(channel.getType.isInterface)
    instanceMethod(transportClient, "getHandler", transportResponseHandler)
    instanceField(transportResponseHandler, "outstandingPushes", classOf[ConcurrentHashMap[_, _]])

    val callback = declaredField(pushRequestInfo, "callback")
    assert(!Modifier.isStatic(callback.getModifiers))
    assert(!Modifier.isFinal(callback.getModifiers))
    assert(callback.getType.isInterface)

    val writeFuture = channel.getType.getMethod("writeAndFlush", classOf[Object]).getReturnType
    assert(writeFuture.isInterface)
    instanceMethod(writeFuture, "isDone", java.lang.Boolean.TYPE)
    assert(writeFuture.getMethods.exists { method =>
      method.getName == "addListener" &&
      method.getParameterCount == 1 &&
      method.getParameterTypes.head.isInterface
    })
  }

  test("reject native shuffle for released clients with final completion-tracking fields") {
    val completionFields = Seq(
      declaredField(transportClientFactory, "clientBootstraps"),
      declaredField(transportClient, "channel"),
      declaredField(transportResponseHandler, "outstandingPushes"),
      declaredField(shuffleClient, "pushDataRetryPool"))
    completionFields.foreach { field =>
      assert(Modifier.isFinal(field.getModifiers), s"$field must remain final in Celeborn")
      assert(!Modifier.isVolatile(field.getModifiers), s"$field must not be volatile in Celeborn")
    }

    val reason =
      CelebornShufflePartitionPusher.nativePushCompletionUnavailableReason(shuffleClient)
    assert(reason != null, s"Celeborn $celebornVersion must not use native push admission")
    assert(reason.contains("completion"), reason)
    assert(reason.contains("volatile"), reason)
  }

  private def load(name: String): Class[_] = Class.forName(name, false, classLoader)

  private def instanceField(owner: Class[_], name: String, expectedType: Class[_]): Field = {
    val result = declaredField(owner, name)
    assert(
      !Modifier.isStatic(result.getModifiers),
      s"${owner.getName}.$name must be an instance field")
    assert(
      expectedType.isAssignableFrom(result.getType),
      s"${owner.getName}.$name has type ${result.getType.getName}, expected ${expectedType.getName}")
    result
  }

  private def exactInstanceField(owner: Class[_], name: String, expectedType: Class[_]): Field = {
    val result = instanceField(owner, name, expectedType)
    assert(
      result.getType == expectedType,
      s"${owner.getName}.$name has type ${result.getType.getName}, expected ${expectedType.getName}")
    result
  }

  private def declaredField(owner: Class[_], name: String): Field = {
    var current = owner
    while (current != null) {
      try {
        val result = current.getDeclaredField(name)
        result.setAccessible(true)
        return result
      } catch {
        case _: NoSuchFieldException => current = current.getSuperclass
      }
    }
    throw new NoSuchFieldException(s"${owner.getName}.$name")
  }

  private def instanceMethod(
      owner: Class[_],
      name: String,
      returnType: Class[_],
      parameterTypes: Class[_]*): Method = {
    val result = owner.getMethod(name, parameterTypes: _*)
    assert(
      !Modifier.isStatic(result.getModifiers),
      s"${owner.getName}.$name must be an instance method")
    assert(
      result.getReturnType == returnType,
      s"${owner.getName}.$name returns ${result.getReturnType.getName}, expected ${returnType.getName}")
    result
  }
}
