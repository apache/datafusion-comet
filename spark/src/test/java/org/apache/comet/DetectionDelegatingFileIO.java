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

import java.util.Map;
import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.SerializableSupplier;

/**
 * A FileIO that works normally (delegating to HadoopFileIO) but whose class is not on Comet's
 * recognized-FileIO allowlist. Composition rather than inheritance is the point: a HadoopFileIO
 * SUBCLASS passes the hierarchy check by design, while this class must be declined. Instantiated
 * reflectively by Iceberg's `CatalogUtil.loadFileIO`, hence public with a no-arg constructor.
 *
 * <p>This is Java because {@code getConf} is abstract on some supported Iceberg versions (1.8.1)
 * and a default method on others (1.12). Scala has no spelling that compiles against both, since it
 * requires {@code override} for a concrete member and rejects it for an abstract one.
 */
public class DetectionDelegatingFileIO implements FileIO, HadoopConfigurable {
  private final HadoopFileIO delegate = new HadoopFileIO();

  @Override
  public InputFile newInputFile(String path) {
    return delegate.newInputFile(path);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return delegate.newOutputFile(path);
  }

  @Override
  public void deleteFile(String path) {
    delegate.deleteFile(path);
  }

  @Override
  public void initialize(Map<String, String> properties) {
    delegate.initialize(properties);
  }

  @Override
  public void setConf(Configuration conf) {
    delegate.setConf(conf);
  }

  public Configuration getConf() {
    return delegate.getConf();
  }

  @Override
  public void serializeConfWith(
      Function<Configuration, SerializableSupplier<Configuration>> confSerializer) {
    delegate.serializeConfWith(confSerializer);
  }
}
