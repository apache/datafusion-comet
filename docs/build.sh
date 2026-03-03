#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set -e
rm -rf build 2> /dev/null
rm -rf temp 2> /dev/null
mkdir temp
cp -rf source/* temp/

# Add user guide from published releases
rm -rf comet-0.10
rm -rf comet-0.11
rm -rf comet-0.12
python3 generate-versions.py

# Generate dynamic content (configs, compatibility matrices) for latest docs
# This runs GenerateDocs against the temp copy, not source files
echo "Generating dynamic documentation content..."
cd ..
./mvnw -q package -Pgenerate-docs -DskipTests -Dmaven.test.skip=true \
  -Dexec.arguments="$(pwd)/docs/temp/user-guide/latest/"
cd docs

make SOURCEDIR=`pwd`/temp html
