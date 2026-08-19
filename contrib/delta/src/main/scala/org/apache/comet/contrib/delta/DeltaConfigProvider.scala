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
 * Makes [[DeltaConf]]'s entries visible to `GenerateDocs`.
 *
 * Without this, the entries are missing from the generated docs entirely: they register
 * themselves with `CometConf.allConfs` when constructed, and `object DeltaConf` is only
 * constructed on first use -- which never happens on the doc-generation path. Registered through
 * `META-INF/services`, packaged only under the `contrib-delta` Maven profile, so a default build
 * discovers nothing and its docs are byte-identical.
 *
 * Public no-arg class (not an `object`) so `ServiceLoader` can instantiate it.
 */
class DeltaConfigProvider extends CometConfigProvider {

  override def configs: Seq[ConfigEntry[_]] = DeltaConf.all

  override def docPage: String = "delta.md"

  override def docCategory: String = DeltaConf.CATEGORY
}
