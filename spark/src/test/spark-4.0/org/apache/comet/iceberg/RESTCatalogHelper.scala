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

package org.apache.comet.iceberg

import java.io.File
import java.nio.file.Files

/** Helper trait for setting up REST catalog with Jetty 11 (jakarta.servlet) for Spark 4.0 */
trait RESTCatalogHelper {

  /** Helper to set up REST catalog with embedded Jetty server (Spark 4.0 / Jetty 11) */
  def withRESTCatalog(f: (String, org.eclipse.jetty.server.Server, File) => Unit): Unit = {
    import org.apache.iceberg.inmemory.InMemoryCatalog
    import org.apache.iceberg.CatalogProperties
    import org.apache.iceberg.rest.{RESTCatalogAdapter, RESTCatalogServlet}
    import org.eclipse.jetty.server.Server
    import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
    import org.eclipse.jetty.server.handler.gzip.GzipHandler

    val warehouseDir = Files.createTempDirectory("comet-rest-catalog-test").toFile
    val backendCatalog = new InMemoryCatalog()
    backendCatalog.initialize(
      "in-memory",
      java.util.Map.of(CatalogProperties.WAREHOUSE_LOCATION, warehouseDir.getAbsolutePath))

    val adapter = new RESTCatalogAdapter(backendCatalog)
    val servlet = new RESTCatalogServlet(adapter)

    val servletContext = new ServletContextHandler(ServletContextHandler.NO_SESSIONS)
    servletContext.setContextPath("/")
    val servletHolder = new ServletHolder(servlet.asInstanceOf[jakarta.servlet.Servlet])
    servletHolder.setInitParameter("jakarta.ws.rs.Application", "ServiceListPublic")
    servletContext.addServlet(servletHolder, "/*")
    servletContext.setVirtualHosts(null)
    servletContext.insertHandler(new GzipHandler())

    val httpServer = new Server(0) // random port
    httpServer.setHandler(servletContext)

    try {
      httpServer.start()
      val restUri = httpServer.getURI.toString.stripSuffix("/")
      f(restUri, httpServer, warehouseDir)
    } finally {
      try {
        httpServer.stop()
        httpServer.join()
      } catch {
        case _: Exception => // ignore cleanup errors
      }
      try {
        backendCatalog.close()
      } catch {
        case _: Exception => // ignore cleanup errors
      }
      def deleteRecursively(file: File): Unit = {
        if (file.isDirectory) {
          file.listFiles().foreach(deleteRecursively)
        }
        file.delete()
      }
      deleteRecursively(warehouseDir)
    }
  }
}
