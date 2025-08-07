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
import java.util.Objects;

import org.apache.parquet.io.DelegatingSeekableInputStream;

/**
 * Wraps an InputStream that possibly implements the methods of a Parquet SeekableInputStream (but
 * is not a Parquet SeekableInputStream). Such an InputStream exists, for instance, in Iceberg's
 * SeekableInputStream
 */
public class WrappedSeekableInputStream extends DelegatingSeekableInputStream {

  private final InputStream wrappedInputStream; // The InputStream we are wrapping

  public WrappedSeekableInputStream(InputStream inputStream) {
    super(inputStream);
    this.wrappedInputStream = Objects.requireNonNull(inputStream, "InputStream cannot be null");
  }

  @Override
  public long getPos() throws IOException {
    try {
      Method targetMethod = wrappedInputStream.getClass().getDeclaredMethod("getPos"); //
      targetMethod.setAccessible(true);
      return (long) targetMethod.invoke(wrappedInputStream);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void seek(long newPos) throws IOException {
    try {
      Method targetMethod = wrappedInputStream.getClass().getDeclaredMethod("seek", long.class);
      targetMethod.setAccessible(true);
      targetMethod.invoke(wrappedInputStream, newPos);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
