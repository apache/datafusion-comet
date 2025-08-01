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

package org.apache.comet.parquet;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

/**
 * A Parquet {@link InputFile} implementation that's similar to {@link
 * org.apache.parquet.hadoop.util.HadoopInputFile}, but with optimizations introduced in Hadoop 3.x,
 * for S3 specifically.
 */
public class WrappedInputFile implements InputFile {
  Object wrapped;

  public WrappedInputFile(Object inputFile) {
    this.wrapped = inputFile;
  }

  @Override
  public long getLength() throws IOException {
    try {
      Method targetMethod = wrapped.getClass().getDeclaredMethod("getLength"); //
      targetMethod.setAccessible(true);
      return (long) targetMethod.invoke(wrapped);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public SeekableInputStream newStream() throws IOException {
    try {
      Method targetMethod = wrapped.getClass().getDeclaredMethod("newStream"); //
      targetMethod.setAccessible(true);
      InputStream stream = (InputStream) targetMethod.invoke(wrapped);
      return new WrappedSeekableInputStream(stream);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public String toString() {
    return wrapped.toString();
  }
}
