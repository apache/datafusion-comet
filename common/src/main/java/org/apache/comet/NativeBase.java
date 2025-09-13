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

package org.apache.comet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.sql.comet.util.Utils;

import static org.apache.comet.Constants.LOG_CONF_NAME;
import static org.apache.comet.Constants.LOG_CONF_PATH;
import static org.apache.comet.Constants.LOG_LEVEL;

/** Base class for JNI bindings. MUST be inherited by all classes that introduce JNI APIs. */
public abstract class NativeBase {
  static final String ARROW_UNSAFE_MEMORY_ACCESS = "arrow.enable_unsafe_memory_access";
  static final String ARROW_NULL_CHECK_FOR_GET = "arrow.enable_null_check_for_get";

  private static final Logger LOG = LoggerFactory.getLogger(NativeBase.class);
  private static final String NATIVE_LIB_NAME = "comet";

  private static final String libraryToLoad = System.mapLibraryName(NATIVE_LIB_NAME);
  private static boolean loaded = false;
  private static volatile Throwable loadErr = null;
  private static final String searchPattern = "libcomet-";

  static {
    try {
      load();
    } catch (Throwable th) {
      LOG.warn("Failed to load comet library", th);
      // logging may not be initialized yet, so also write to stderr
      System.err.println("Failed to load comet library: " + th.getMessage());
      loadErr = th;
    }
  }

  public static synchronized boolean isLoaded() throws Throwable {
    if (loadErr != null) {
      throw loadErr;
    }
    return loaded;
  }

  // Only for testing
  static synchronized void setLoaded(boolean b) {
    loaded = b;
  }

  static synchronized void load() {
    if (loaded) {
      return;
    }

    cleanupOldTempLibs();

    // Check if the arch used by JDK is the same as arch on the host machine, in particular,
    // whether x86_64 JDK is used in arm64 Mac
    if (!checkArch()) {
      LOG.warn(
          "Comet is disabled. JDK compiled for x86_64 is used in a Mac based on Apple Silicon. "
              + "In order to use Comet, Please install a JDK version for ARM64 architecture");
      return;
    }

    // Try to load Comet library from the java.library.path.
    try {
      System.loadLibrary(NATIVE_LIB_NAME);
      loaded = true;
    } catch (UnsatisfiedLinkError ex) {
      // Doesn't exist, so proceed to loading bundled library.
      bundleLoadLibrary();
    }

    initWithLogConf();
    // Only set the Arrow properties when debugging mode is off
    if (!(boolean) CometConf.COMET_DEBUG_ENABLED().get()) {
      setArrowProperties();
    }
  }

  /**
   * Use the bundled native libraries. Functionally equivalent to <code>System.loadLibrary</code>.
   */
  private static void bundleLoadLibrary() {
    String resourceName = resourceName();
    InputStream is = NativeBase.class.getResourceAsStream(resourceName);
    if (is == null) {
      throw new UnsupportedOperationException(
          "Unsupported OS/arch, cannot find "
              + resourceName
              + ". Please try building from source.");
    }

    File tempLib = null;
    File tempLibLock = null;
    try {
      // Create the .lck file first to avoid a race condition
      // with other concurrently running Java processes using Comet.
      tempLibLock = File.createTempFile(searchPattern, "." + os().libExtension + ".lck");
      tempLib = new File(tempLibLock.getAbsolutePath().replaceFirst(".lck$", ""));
      // copy to tempLib
      Files.copy(is, tempLib.toPath(), StandardCopyOption.REPLACE_EXISTING);
      System.load(tempLib.getAbsolutePath());
      loaded = true;
    } catch (IOException e) {
      throw new IllegalStateException("Cannot unpack libcomet: " + e);
    } finally {
      if (!loaded) {
        if (tempLib != null && tempLib.exists()) {
          if (!tempLib.delete()) {
            LOG.error(
                "Cannot unpack libcomet / cannot delete a temporary native library " + tempLib);
          }
        }
        if (tempLibLock != null && tempLibLock.exists()) {
          if (!tempLibLock.delete()) {
            LOG.error(
                "Cannot unpack libcomet / cannot delete a temporary lock file " + tempLibLock);
          }
        }
      } else {
        tempLib.deleteOnExit();
        tempLibLock.deleteOnExit();
      }
    }
  }

  private static void initWithLogConf() {
    String logConfPath = System.getProperty(LOG_CONF_PATH(), Utils.getConfPath(LOG_CONF_NAME()));
    String logLevel = System.getProperty(LOG_LEVEL());

    // If both the system property and the environmental variable failed to find a log
    // configuration, then fall back to using the deployed default
    if (logConfPath == null) {
      LOG.info(
          "Couldn't locate log file from either COMET_CONF_DIR or comet.log.file.path. "
              + "Using default log configuration with {} log level which emits to stdout",
          logLevel == null ? "INFO" : logLevel);
      logConfPath = "";
    } else {
      // Ignore log level if a log configuration file is specified
      if (logLevel != null) {
        LOG.warn("Ignoring log level {} because a log configuration file is specified", logLevel);
      }

      LOG.info("Using {} for native library logging", logConfPath);
    }
    init(logConfPath, logLevel);
  }

  private static void cleanupOldTempLibs() {
    String tempFolder = new File(System.getProperty("java.io.tmpdir")).getAbsolutePath();
    File dir = new File(tempFolder);

    File[] tempLibFiles =
        dir.listFiles(
            new FilenameFilter() {
              public boolean accept(File dir, String name) {
                return name.startsWith(searchPattern) && !name.endsWith(".lck");
              }
            });

    if (tempLibFiles != null) {
      for (File tempLibFile : tempLibFiles) {
        File lckFile = new File(tempLibFile.getAbsolutePath() + ".lck");
        if (!lckFile.exists()) {
          try {
            tempLibFile.delete();
          } catch (SecurityException e) {
            LOG.error("Failed to delete old temp lib", e);
          }
        }
      }
    }
  }

  // Set Arrow related properties upon initializing native, such as enabling unsafe memory access
  // as well as disabling null check for get, for performance reasons.
  private static void setArrowProperties() {
    setPropertyIfNull(ARROW_UNSAFE_MEMORY_ACCESS, "true");
    setPropertyIfNull(ARROW_NULL_CHECK_FOR_GET, "false");
  }

  private static void setPropertyIfNull(String key, String value) {
    if (System.getProperty(key) == null) {
      LOG.info("Setting system property {} to {}", key, value);
      System.setProperty(key, value);
    } else {
      LOG.info(
          "Skip setting system property {} to {}, because it is already set to {}",
          key,
          value,
          System.getProperty(key));
    }
  }

  private enum OS {
    // Even on Windows, the default compiler from cpptasks (gcc) uses .so as a shared lib extension
    WINDOWS("win32", "so"),
    LINUX("linux", "so"),
    MAC("darwin", "dylib"),
    SOLARIS("solaris", "so");
    public final String name, libExtension;

    OS(String name, String libExtension) {
      this.name = name;
      this.libExtension = libExtension;
    }
  }

  private static String arch() {
    return System.getProperty("os.arch");
  }

  private static OS os() {
    String osName = System.getProperty("os.name");
    if (osName.contains("Linux")) {
      return OS.LINUX;
    } else if (osName.contains("Mac")) {
      return OS.MAC;
    } else if (osName.contains("Windows")) {
      return OS.WINDOWS;
    } else if (osName.contains("Solaris") || osName.contains("SunOS")) {
      return OS.SOLARIS;
    } else {
      throw new UnsupportedOperationException("Unsupported operating system: " + osName);
    }
  }

  // For some reason users will get JVM crash when running Comet that is compiled for `aarch64`
  // using a JVM that is compiled against `amd64`. Here we check if that is the case and fallback
  // to Spark accordingly.
  private static boolean checkArch() {
    if (os() == OS.MAC) {
      try {
        String javaArch = arch();
        Process process = Runtime.getRuntime().exec("uname -a");
        if (process.waitFor() == 0) {
          BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()));
          String line;
          while ((line = in.readLine()) != null) {
            if (javaArch.equals("x86_64") && line.contains("ARM64")) {
              return false;
            }
          }
        }
      } catch (IOException | InterruptedException e) {
        LOG.warn("Error parsing host architecture", e);
      }
    }

    return true;
  }

  private static String resourceName() {
    OS os = os();
    String packagePrefix = NativeBase.class.getPackage().getName().replace('.', '/');

    return "/" + packagePrefix + "/" + os.name + "/" + arch() + "/" + libraryToLoad;
  }

  /**
   * Initialize the native library through JNI.
   *
   * @param logConfPath location to the native log configuration file
   */
  static native void init(String logConfPath, String logLevel);
}
