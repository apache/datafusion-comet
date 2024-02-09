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

package org.apache.spark.sql.comet;

import java.util.HashMap;

import org.apache.spark.sql.execution.ScalarSubquery;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

import org.apache.comet.CometRuntimeException;

/** A helper class to execute scalar subqueries and retrieve subquery results from native code. */
public class CometScalarSubquery {
  /**
   * A map from (planId, subqueryId) to the corresponding ScalarSubquery. We cannot simply use
   * `subqueryId` because same query plan may be executed multiple times in same executor (i.e., JVM
   * instance). For such cases, if we delete the ScalarSubquery from the map after the first
   * execution, the second execution will fail to find the ScalarSubquery if the native code is
   * still running.
   */
  private static final HashMap<Long, HashMap<Long, ScalarSubquery>> subqueryMap = new HashMap<>();

  public static synchronized void setSubquery(long planId, ScalarSubquery subquery) {
    if (!subqueryMap.containsKey(planId)) {
      subqueryMap.put(planId, new HashMap<>());
    }

    subqueryMap.get(planId).put(subquery.exprId().id(), subquery);
  }

  public static synchronized void removeSubquery(long planId, ScalarSubquery subquery) {
    subqueryMap.get(planId).remove(subquery.exprId().id());

    if (subqueryMap.get(planId).isEmpty()) {
      subqueryMap.remove(planId);
    }
  }

  /** Retrieve the result of subquery. */
  private static Object getSubquery(Long planId, Long id) {
    if (!subqueryMap.containsKey(planId)) {
      throw new CometRuntimeException("Subquery " + id + " not found for plan " + planId + ".");
    }

    return subqueryMap.get(planId).get(id).eval(null);
  }

  /** Check if the result of a subquery is null. Called from native code. */
  public static boolean isNull(long planId, long id) {
    return getSubquery(planId, id) == null;
  }

  /** Get the result of a subquery as a boolean. Called from native code. */
  public static boolean getBoolean(long planId, long id) {
    return (boolean) getSubquery(planId, id);
  }

  /** Get the result of a subquery as a byte. Called from native code. */
  public static byte getByte(long planId, long id) {
    return (byte) getSubquery(planId, id);
  }

  /** Get the result of a subquery as a short. Called from native code. */
  public static short getShort(long planId, long id) {
    return (short) getSubquery(planId, id);
  }

  /** Get the result of a subquery as an integer. Called from native code. */
  public static int getInt(long planId, long id) {
    return (int) getSubquery(planId, id);
  }

  /** Get the result of a subquery as a long. Called from native code. */
  public static long getLong(long planId, long id) {
    return (long) getSubquery(planId, id);
  }

  /** Get the result of a subquery as a float. Called from native code. */
  public static float getFloat(long planId, long id) {
    return (float) getSubquery(planId, id);
  }

  /** Get the result of a subquery as a double. Called from native code. */
  public static double getDouble(long planId, long id) {
    return (double) getSubquery(planId, id);
  }

  /** Get the result of a subquery as a decimal represented as bytes. Called from native code. */
  public static byte[] getDecimal(long planId, long id) {
    return ((Decimal) getSubquery(planId, id)).toJavaBigDecimal().unscaledValue().toByteArray();
  }

  /** Get the result of a subquery as a string. Called from native code. */
  public static String getString(long planId, long id) {
    return ((UTF8String) getSubquery(planId, id)).toString();
  }

  /** Get the result of a subquery as a binary. Called from native code. */
  public static byte[] getBinary(long planId, long id) {
    return (byte[]) getSubquery(planId, id);
  }
}
