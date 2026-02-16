# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Thin wrapper around SparkSession.builder."""

from typing import Dict

from pyspark.sql import SparkSession


def create_session(app_name: str, spark_conf: Dict[str, str]) -> SparkSession:
    """Create (or retrieve) a SparkSession with the given config.

    When launched via spark-submit the configs are already set; this just
    picks up the existing session.
    """
    builder = SparkSession.builder.appName(app_name)
    for key, value in spark_conf.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
