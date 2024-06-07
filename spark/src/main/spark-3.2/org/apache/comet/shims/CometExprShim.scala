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
package org.apache.comet.shims

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.DataType

/**
 * `CometExprShim` acts as a shim for for parsing expressions from different Spark versions.
 */
trait CometExprShim {
    /**
     * Returns a tuple of expressions for the `unhex` function.
     */
    def unhexSerde(unhex: Unhex): (Expression, Expression) = {
        (unhex.child, Literal(false))
    }

    protected def isTimestampNTZType(dt: DataType): Boolean = dt match {
        // `TimestampNTZType` is private in Spark 3.2.
        case dt if dt.typeName == "timestamp_ntz" => true
        case _ => false
    }
}
