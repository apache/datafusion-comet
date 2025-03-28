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

import org.apache.spark.sql.CometTestBase

class AppleDefaultConfigSuite extends CometTestBase {

  test("Comet configuration defaults are as expected") {
    // check that we haven't accidentally changed any defaults when we merge changes from OSS Comet

    // Comet should be disabled by default, but enabled when this test is running in CI.
    // Note that the Call Home listener has a set of hard-coded config settings for Comet,
    // including `spark.comet.enabled=true`. See `SparkCallHomeListenerV2#defaultNativeEngineConf`
    // for details.
    val COMET_ENABLED_DEFAULT = sys.env.getOrElse("ENABLE_COMET", "false")
    assert(CometConf.COMET_ENABLED.defaultValue.get == COMET_ENABLED_DEFAULT.toBoolean)
    assert(CometConf.COMET_ENABLED.get())

    // scans and execs should be enabled by default
    assert(CometConf.COMET_NATIVE_SCAN_ENABLED.defaultValue.get)
    assert(CometConf.COMET_EXEC_ENABLED.defaultValue.get)

    // individual execs all default to enabled
    assert(CometConf.COMET_EXEC_AGGREGATE_ENABLED.defaultValue.get)
    assert(CometConf.COMET_EXEC_BROADCAST_EXCHANGE_ENABLED.defaultValue.get)
    assert(CometConf.COMET_EXEC_BROADCAST_HASH_JOIN_ENABLED.defaultValue.get)
    assert(CometConf.COMET_EXEC_COALESCE_ENABLED.defaultValue.get)
    assert(CometConf.COMET_EXEC_COLLECT_LIMIT_ENABLED.defaultValue.get)
    assert(CometConf.COMET_EXEC_EXPAND_ENABLED.defaultValue.get)
    assert(CometConf.COMET_EXEC_FILTER_ENABLED.defaultValue.get)
    assert(CometConf.COMET_EXEC_GLOBAL_LIMIT_ENABLED.defaultValue.get)
    assert(CometConf.COMET_EXEC_HASH_JOIN_ENABLED.defaultValue.get)
    assert(CometConf.COMET_EXEC_LOCAL_LIMIT_ENABLED.defaultValue.get)
    assert(CometConf.COMET_EXEC_PROJECT_ENABLED.defaultValue.get)
    assert(CometConf.COMET_EXEC_SORT_ENABLED.defaultValue.get)
    assert(CometConf.COMET_EXEC_SORT_MERGE_JOIN_ENABLED.defaultValue.get)
    assert(CometConf.COMET_EXEC_TAKE_ORDERED_AND_PROJECT_ENABLED.defaultValue.get)
    assert(CometConf.COMET_EXEC_UNION_ENABLED.defaultValue.get)
    assert(CometConf.COMET_EXEC_WINDOW_ENABLED.defaultValue.get)

    // SMJ + join condition has known issues
    assert(!CometConf.COMET_EXEC_SORT_MERGE_JOIN_WITH_JOIN_FILTER_ENABLED.defaultValue.get)

    // shuffle is disabled by default due to memory issues
    assert(!CometConf.COMET_EXEC_SHUFFLE_ENABLED.defaultValue.get)
    assert("auto" == CometConf.COMET_SHUFFLE_MODE.defaultValue.get)
  }

}
