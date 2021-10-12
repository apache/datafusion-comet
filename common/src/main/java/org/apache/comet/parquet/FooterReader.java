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
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.spark.sql.execution.datasources.PartitionedFile;

/**
 * Copied from Spark's `ParquetFooterReader` in order to avoid shading issue around Parquet.
 *
 * <p>`FooterReader` is a util class which encapsulates the helper methods of reading parquet file
 * footer.
 */
public class FooterReader {
  public static ParquetMetadata readFooter(Configuration configuration, PartitionedFile file)
      throws IOException, URISyntaxException {
    long start = file.start();
    long length = file.length();
    Path filePath = new Path(new URI(file.filePath().toString()));
    CometInputFile inputFile = CometInputFile.fromPath(filePath, configuration);
    ParquetReadOptions readOptions =
        HadoopReadOptions.builder(inputFile.getConf(), inputFile.getPath())
            .withRange(start, start + length)
            .build();
    ReadOptions cometReadOptions = ReadOptions.builder(configuration).build();
    // Use try-with-resources to ensure fd is closed.
    try (FileReader fileReader = new FileReader(inputFile, readOptions, cometReadOptions)) {
      return fileReader.getFooter();
    }
  }
}
