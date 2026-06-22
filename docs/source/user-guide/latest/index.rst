.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

.. image:: /_static/images/DataFusionComet-Logo-Light.png
  :alt: DataFusion Comet Logo

================================
Comet $COMET_VERSION User Guide
================================

This guide covers Comet $COMET_VERSION: how to install it, build it from source, configure it for
your Spark deployment, and get the best results from it. It also documents the data sources, data
types, operators, and expressions that Comet supports, along with a compatibility guide describing
known differences from Apache Spark.

Operational topics include reading and understanding Comet query plans, tuning, available metrics,
and integration guides for Apache Iceberg and Kubernetes. Select a topic from the navigation menu
to read more.

.. _toc.user-guide-links-$COMET_VERSION:
.. toctree::
   :maxdepth: 1
   :caption: Getting Started
   :hidden:

   Installing Comet <installation>
   Configuration Settings <configs>

.. toctree::
   :maxdepth: 1
   :caption: What Comet Supports
   :hidden:

   Supported Data Sources <datasources>
   Supported Data Types <datatypes>
   Supported Operators <operators>
   Supported Expressions <expressions>
   ScalaUDF and Java UDF Support <scala_java_udfs>

.. toctree::
   :maxdepth: 1
   :caption: Compatibility
   :hidden:

   Overview <compatibility/index>
   compatibility/scans
   compatibility/floating-point
   compatibility/regex
   compatibility/operators
   compatibility/expressions/index
   compatibility/json
   compatibility/spark-versions

.. toctree::
   :maxdepth: 1
   :caption: Operating Comet
   :hidden:

   Understanding Comet Plans <understanding-comet-plans>
   Tuning Guide <tuning>
   Metrics Guide <metrics>

.. toctree::
   :maxdepth: 1
   :caption: Integrations
   :hidden:

   Iceberg Guide <iceberg>
   Delta Lake Guide <delta>
   S3 Credential Providers <s3-credential-providers>
   Kubernetes Guide <kubernetes>

.. toctree::
   :maxdepth: 1
   :caption: Advanced
   :hidden:

   Building From Source <source>
