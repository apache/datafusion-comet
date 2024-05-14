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

package org.apache.comet

import scala.util.Random

class DataGenerator(r: Random) {

  /** Generate a random string using the specified characters */
  def generateString(chars: String, maxLen: Int): String = {
    val len = r.nextInt(maxLen)
    Range(0, len).map(_ => chars.charAt(r.nextInt(chars.length))).mkString
  }

  /** Generate random strings */
  def generateStrings(n: Int, maxLen: Int): Seq[String] = {
    Range(0, n).map(_ => r.nextString(maxLen))
  }

  /** Generate random strings using the specified characters */
  def generateStrings(n: Int, chars: String, maxLen: Int): Seq[String] = {
    Range(0, n).map(_ => generateString(chars, maxLen))
  }

}
