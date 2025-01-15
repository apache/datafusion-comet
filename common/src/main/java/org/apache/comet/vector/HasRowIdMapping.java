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

package org.apache.comet.vector;

/**
 * An interface could be implemented by vectors that have row id mapping.
 *
 * <p>For example, Iceberg's DeleteFile has a row id mapping to map row id to position. This
 * interface is used to set and get the row id mapping. The row id mapping is an array of integers,
 * where the index is the row id and the value is the position. Here is an example:
 * [0,1,2,3,4,5,6,7] -- Original status of the row id mapping array Position delete 2, 6
 * [0,1,3,4,5,7,-,-] -- After applying position deletes [Set Num records to 6]
 */
public interface HasRowIdMapping {
  default void setRowIdMapping(int[] rowIdMapping) {
    throw new UnsupportedOperationException("setRowIdMapping is not supported");
  }

  default int[] getRowIdMapping() {
    throw new UnsupportedOperationException("getRowIdMapping is not supported");
  }
}
