#!/usr/bin/python
##############################################################################
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
##############################################################################

# This script clones release branches such as branch-0.9 and then copies the user guide markdown
# for inclusion in the published documentation so that we publish user guides for released versions
# of Comet

import os

for version in ["0.8", "0.9"]:
    os.system(f"git clone --depth 1 https://github.com/apache/datafusion-comet.git -b branch-{version} comet-{version}")
    os.system(f"mkdir temp/user-guide/{version}")
    os.system(f"cp -rf comet-{version}/docs/source/user-guide/* temp/user-guide/{version}")