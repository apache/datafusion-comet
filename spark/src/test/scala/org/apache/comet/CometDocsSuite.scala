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

import org.scalatest.funsuite.AnyFunSuite

class CometDocsSuite extends AnyFunSuite {

  test("a release links to the guide for its major and minor version") {
    assert(CometDocs.userGuideVersion("1.2.0") == "1.2")
    assert(CometDocs.userGuideVersion("1.2.3") == "1.2")
    assert(CometDocs.userGuideVersion("10.14.0") == "10.14")
  }

  test("a snapshot or unknown version links to the development guide") {
    assert(CometDocs.userGuideVersion("1.2.0-SNAPSHOT") == "dev")
    assert(CometDocs.userGuideVersion("1.2.0-rc1") == "dev")
    assert(CometDocs.userGuideVersion("<unknown>") == "dev")
    assert(CometDocs.userGuideVersion("") == "dev")
  }

  test("the guide URL and the compatibility message follow the build version") {
    val version = CometDocs.userGuideVersion(COMET_VERSION)
    assert(
      CometDocs.userGuideUrl("tuning.html") ==
        s"https://datafusion.apache.org/comet/user-guide/$version/tuning.html")
    assert(CometConf.COMPAT_GUIDE.contains(s"/user-guide/$version/compatibility/index.html"))
    assert(!CometConf.COMPAT_GUIDE.contains("/latest/"))
  }
}
