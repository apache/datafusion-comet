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

package org.apache.comet.contrib.delta

import org.apache.comet.{CometConfigProvider, ConfigEntry}

/**
 * Exposes this contrib's config entries to `GenerateDocs`. Note: with the current module layout
 * (`contrib/delta-spark` depends on `comet-spark`) the doc build cannot see this provider; it
 * exists to satisfy the contrib-conf contract and becomes active if the module is ever folded
 * into the spark build like `contrib/delta` is.
 */
class DeltaSparkConfigProvider extends CometConfigProvider {
  override def configs: Seq[ConfigEntry[_]] = DeltaScanConf.all
  override def docPage: String = "delta.md"
  override def docCategory: String = "delta"
}
