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

import org.apache.spark.internal.Logging

/**
 * Makes an optional, out-of-tree contrib's config entries visible to [[GenerateDocs]].
 *
 * A `ConfigEntry` registers itself with `CometConf.allConfs` as a side effect of being
 * constructed, and a Scala `object` is constructed lazily on first use. A contrib's config object
 * (e.g. the Delta contrib's `DeltaConf`) is therefore never initialised during doc generation --
 * nothing on that path refers to it -- so its entries are absent from `CometConf.allConfs` and
 * silently missing from the generated tables. Note this is about lazy initialisation, not about
 * where the object lives: a config object inside core that nothing referenced would be just as
 * invisible.
 *
 * Implementations are discovered through the JDK [[ServiceLoader]], the same mechanism as
 * [[org.apache.comet.rules.CometScanContrib]] and
 * [[org.apache.spark.sql.comet.PlanDataInjector]], so core names no contrib and a default build
 * (which ships no service file) sees an empty registry and unchanged docs.
 *
 * A contrib ships:
 *   - an implementation of this trait, and
 *   - a `META-INF/services/org.apache.comet.CometConfigProvider` resource naming it.
 */
trait CometConfigProvider {

  /**
   * The contrib's config entries. Returning them forces the enclosing config object to
   * initialise, which is what registers them; the returned values are what gets documented.
   */
  def configs: Seq[ConfigEntry[_]]

  /**
   * User-guide page these configs are documented on, relative to the user-guide directory (e.g.
   * `"delta.md"`). Contrib configs are written to the contrib's own page rather than into core's
   * `configs.md`, so core's generated docs describe exactly what a default build ships and do not
   * change depending on which contrib profiles were enabled at build time.
   */
  def docPage: String

  /**
   * `CONFIG_TABLE` category this provider fills on its page. Must not collide with a core
   * category, or the contrib's entries would also be emitted into core's tables.
   */
  def docCategory: String
}

object CometConfigProvider extends Logging {

  /**
   * Discovered contrib config providers. Mirrors the other contrib registries: a misbuilt contrib
   * jar surfaces as a warning rather than failing doc generation. Empty on a default build.
   */
  lazy val providers: Seq[CometConfigProvider] =
    ContribServices.load(classOf[CometConfigProvider], getClass.getClassLoader)

  /** Discovery against an explicit ClassLoader, so tests need not force the cached registry. */
  private[comet] def loadProviders(loader: ClassLoader): Seq[CometConfigProvider] =
    ContribServices.loadFrom(classOf[CometConfigProvider], loader)
}
