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

  // Constructors
  def geomFromWkt(g: String): String = notSupported("st_geomfromwkt")
  def geomFromGeoJson(g: String): String = notSupported("st_geomfromgeojson")
  def makeEnvelope(xmin: Double, ymin: Double, xmax: Double, ymax: Double): String =
    notSupported("st_makeenvelope")
  def makeLine(g1: String, g2: String): String = notSupported("st_makeline")
  // Serializers
  def asText(g: String): String = notSupported("st_astext")
  def asGeoJson(g: String): String = notSupported("st_asgeojson")
  // Predicates
  def contains(g1: String, g2: String): Boolean = notSupported("st_contains")
  def intersects(g1: String, g2: String): Boolean = notSupported("st_intersects")
  def within(g1: String, g2: String): Boolean = notSupported("st_within")
  def covers(g1: String, g2: String): Boolean = notSupported("st_covers")
  def coveredBy(g1: String, g2: String): Boolean = notSupported("st_coveredby")
  def equals(g1: String, g2: String): Boolean = notSupported("st_equals")
  def touches(g1: String, g2: String): Boolean = notSupported("st_touches")
  def crosses(g1: String, g2: String): Boolean = notSupported("st_crosses")
  def disjoint(g1: String, g2: String): Boolean = notSupported("st_disjoint")
  def overlaps(g1: String, g2: String): Boolean = notSupported("st_overlaps")
  // Measurements
  def distance(g1: String, g2: String): Double = notSupported("st_distance")
  def distanceSphere(g1: String, g2: String): Double = notSupported("st_distancesphere")
  def area(g: String): Double = notSupported("st_area")
  def length(g: String): Double = notSupported("st_length")
  def perimeter(g: String): Double = notSupported("st_perimeter")
  def hausdorffDistance(g1: String, g2: String): Double = notSupported("st_hausdorffdistance")
  // Transformations
  def centroid(g: String): String = notSupported("st_centroid")
  def envelope(g: String): String = notSupported("st_envelope")
  def convexHull(g: String): String = notSupported("st_convexhull")
  def simplify(g: String, tolerance: Double): String = notSupported("st_simplify")
  def simplifyPreserveTopology(g: String, tolerance: Double): String =
    notSupported("st_simplifypreservetopology")
  def flipCoordinates(g: String): String = notSupported("st_flipcoordinates")
  def boundary(g: String): String = notSupported("st_boundary")
  def buffer(g: String, distance: Double): String = notSupported("st_buffer")
  // Set operations
  def union(g1: String, g2: String): String = notSupported("st_union")
  def intersection(g1: String, g2: String): String = notSupported("st_intersection")
  def difference(g1: String, g2: String): String = notSupported("st_difference")
  def symDifference(g1: String, g2: String): String = notSupported("st_symdifference")
  // Accessors
  def isEmpty(g: String): Boolean = notSupported("st_isempty")
  def geometryType(g: String): String = notSupported("st_geometrytype")
  def numPoints(g: String): Long = notSupported("st_numpoints")
  def stX(g: String): Double = notSupported("st_x")
  def stY(g: String): Double = notSupported("st_y")
}
