#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Thin release image: drops a pre-built multi-arch Comet uber-jar into an
# Apache Spark base image.  Expected to be built by
# dev/release/build-docker-images.sh, which supplies SPARK_IMAGE and
# COMET_JAR build args.
#
# The jar already contains native libs for linux/amd64 and linux/aarch64, so
# the same jar works for both platforms under `docker buildx build
# --platform linux/amd64,linux/arm64`.

ARG SPARK_IMAGE
FROM ${SPARK_IMAGE}

ARG COMET_JAR
USER root

COPY ${COMET_JAR} $SPARK_HOME/jars/

# Restore the non-root user set by the apache/spark base image.
ARG spark_uid=185
USER ${spark_uid}
