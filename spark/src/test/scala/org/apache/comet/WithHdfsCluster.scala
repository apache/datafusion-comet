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

import java.io.{File, FileWriter}
import java.net.InetAddress
import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils

/**
 * Trait for starting and stopping a MiniDFSCluster for testing.
 *
 * Most copy from:
 * https://github.com/apache/kyuubi/blob/master/kyuubi-server/src/test/scala/org/apache/kyuubi/server/MiniDFSService.scala
 */
trait WithHdfsCluster extends Logging {

  private var hadoopConfDir: File = _
  private var hdfsCluster: MiniDFSCluster = _
  private var hdfsConf: Configuration = _
  private var tmpRootDir: Path = _
  private var fileSystem: FileSystem = _

  def startHdfsCluster(): Unit = {
    hdfsConf = new Configuration()
    // before HADOOP-18206 (3.4.0), HDFS MetricsLogger strongly depends on
    // commons-logging, we should disable it explicitly, otherwise, it throws
    // ClassNotFound: org.apache.commons.logging.impl.Log4JLogger
    hdfsConf.set("dfs.namenode.metrics.logger.period.seconds", "0")
    hdfsConf.set("dfs.datanode.metrics.logger.period.seconds", "0")
    // Set bind host to localhost to avoid java.net.BindException
    hdfsConf.setIfUnset("dfs.namenode.rpc-bind-host", "localhost")

    hdfsCluster = new MiniDFSCluster.Builder(hdfsConf)
      .checkDataNodeAddrConfig(true)
      .checkDataNodeHostConfig(true)
      .build()
    logInfo(
      "NameNode address in configuration is " +
        s"${hdfsConf.get(HdfsClientConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY)}")
    hadoopConfDir =
      JavaUtils.createDirectory(System.getProperty("java.io.tmpdir"), "comet_hdfs_conf")
    saveHadoopConf(hadoopConfDir)

    fileSystem = hdfsCluster.getFileSystem
    tmpRootDir = new Path("/tmp")
    fileSystem.mkdirs(tmpRootDir)
  }

  def stopHdfsCluster(): Unit = {
    if (hdfsCluster != null) hdfsCluster.shutdown(true)
    if (hadoopConfDir != null) FileUtils.deleteDirectory(hadoopConfDir)
  }

  private def saveHadoopConf(hadoopConfDir: File): Unit = {
    val configToWrite = new Configuration(false)
    val hostName = InetAddress.getLocalHost.getHostName
    hdfsConf.iterator().asScala.foreach { kv =>
      val key = kv.getKey
      val value = kv.getValue.replaceAll(hostName, "localhost")
      configToWrite.set(key, value)
    }
    val file = new File(hadoopConfDir, "core-site.xml")
    val writer = new FileWriter(file)
    configToWrite.writeXml(writer)
    writer.close()
  }

  def getHadoopConf: Configuration = hdfsConf
  def getDFSPort: Int = hdfsCluster.getNameNodePort
  def getHadoopConfDir: String = hadoopConfDir.getAbsolutePath
  def getHadoopConfFile: Path = new Path(hadoopConfDir.toURI.toURL.toString, "core-site.xml")
  def getTmpRootDir: Path = tmpRootDir
  def getFileSystem: FileSystem = fileSystem

  def withTmpHdfsDir(tmpDir: Path => Unit): Unit = {
    val tempPath = new Path(tmpRootDir, UUID.randomUUID().toString)
    fileSystem.mkdirs(tempPath)
    try tmpDir(tempPath)
    finally fileSystem.delete(tempPath, true)
  }

}
