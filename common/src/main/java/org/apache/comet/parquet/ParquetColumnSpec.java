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

import java.util.Map;

public class ParquetColumnSpec {

  private final int fieldId;
  private final String[] path;
  private final String physicalType;
  private final int typeLength;
  private final boolean isRepeated;
  private final int maxDefinitionLevel;
  private final int maxRepetitionLevel;

  // Logical type info
  private String logicalTypeName;
  private Map<String, String> logicalTypeParams;

  public ParquetColumnSpec(
      int fieldId,
      String[] path,
      String physicalType,
      int typeLength,
      boolean isRepeated,
      int maxDefinitionLevel,
      int maxRepetitionLevel,
      String logicalTypeName,
      Map<String, String> logicalTypeParams) {
    this.fieldId = fieldId;
    this.path = path;
    this.physicalType = physicalType;
    this.typeLength = typeLength;
    this.isRepeated = isRepeated;
    this.maxDefinitionLevel = maxDefinitionLevel;
    this.maxRepetitionLevel = maxRepetitionLevel;
    this.logicalTypeName = logicalTypeName;
    this.logicalTypeParams = logicalTypeParams;
  }

  public int getFieldId() {
    return fieldId;
  }

  public String[] getPath() {
    return path;
  }

  public String getPhysicalType() {
    return physicalType;
  }

  public int getTypeLength() {
    return typeLength;
  }

  public boolean isRepeated() {
    return isRepeated;
  }

  public int getMaxRepetitionLevel() {
    return maxRepetitionLevel;
  }

  public int getMaxDefinitionLevel() {
    return maxDefinitionLevel;
  }

  public String getLogicalTypeName() {
    return logicalTypeName;
  }

  public Map<String, String> getLogicalTypeParams() {
    return logicalTypeParams;
  }
}
