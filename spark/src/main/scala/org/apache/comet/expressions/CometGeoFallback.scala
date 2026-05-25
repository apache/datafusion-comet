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

package org.apache.comet.expressions

/**
 * JVM fallback implementations for Comet geo UDFs. Called only when Comet native execution is
 * disabled. When Comet is active the expression is serded to ScalarFunc and executed via the Rust
 * geo crate in DataFusion.
 *
 * Requires Apache Sedona on the classpath to function. Without Sedona and without Comet enabled
 * an UnsupportedOperationException is thrown at runtime.
 */
object CometGeoFallback {

  private def notSupported(fn: String): Nothing =
    throw new UnsupportedOperationException(
      s"$fn requires either Comet native execution (spark.comet.exec.enabled=true) " +
        s"or Apache Sedona on the classpath for JVM fallback.")

  def contains(g1: String, g2: String): Boolean = notSupported("st_contains")
  def intersects(g1: String, g2: String): Boolean = notSupported("st_intersects")
  def within(g1: String, g2: String): Boolean = notSupported("st_within")
  def distance(g1: String, g2: String): Double = notSupported("st_distance")
  def area(g: String): Double = notSupported("st_area")
  def centroid(g: String): String = notSupported("st_centroid")
  def length(g: String): Double = notSupported("st_length")
  def isEmpty(g: String): Boolean = notSupported("st_isempty")
  def geometryType(g: String): String = notSupported("st_geometrytype")
  def numPoints(g: String): Long = notSupported("st_numpoints")
  def stX(g: String): Double = notSupported("st_x")
  def stY(g: String): Double = notSupported("st_y")
  def envelope(g: String): String = notSupported("st_envelope")
  def convexHull(g: String): String = notSupported("st_convexhull")
  def simplify(g: String, tolerance: Double): String = notSupported("st_simplify")
  def buffer(g: String, distance: Double): String = notSupported("st_buffer")
  def union(g1: String, g2: String): String = notSupported("st_union")
  def intersection(g1: String, g2: String): String = notSupported("st_intersection")
}
