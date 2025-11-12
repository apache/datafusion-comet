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

package org.apache.comet.serde

sealed trait SupportLevel

/**
 * Comet either supports this feature with full compatibility with Spark, or may have known
 * differences in some specific edge cases that are unlikely to be an issue for most users.
 *
 * Any compatibility differences are noted in the
 * [[https://datafusion.apache.org/comet/user-guide/compatibility.html Comet Compatibility Guide]].
 */
case class Compatible(notes: Option[String] = None) extends SupportLevel

/**
 * Comet supports this feature but results can be different from Spark.
 *
 * Any compatibility differences are noted in the
 * [[https://datafusion.apache.org/comet/user-guide/compatibility.html Comet Compatibility Guide]].
 */
case class Incompatible(notes: Option[String] = None) extends SupportLevel

/** Comet does not support this feature */
case class Unsupported(notes: Option[String] = None) extends SupportLevel
