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

import argparse
import re
import sys

def main(file, task_filter):
    # keep track of running total allocation per consumer
    alloc = {}

    # open file
    with open(file) as f:
        # iterate over lines in file
        print("name,size,label")
        for line in f:
            # print(line, file=sys.stderr)

            # example line: [Task 486] MemoryPool[HashJoinInput[6]].shrink(1000)
            # parse consumer name
            re_match = re.search('\[Task (.*)\] MemoryPool\[(.*)\]\.(try_grow|grow|shrink)\(([0-9]*)\)', line, re.IGNORECASE)
            if re_match:
                try:
                    task = int(re_match.group(1))
                    if task != task_filter:
                        continue

                    consumer = re_match.group(2)
                    method = re_match.group(3)
                    size = int(re_match.group(4))

                    if alloc.get(consumer) is None:
                        alloc[consumer] = size
                    else:
                        if method == "grow" or method == "try_grow":
                            if "Err" in line:
                                # do not update allocation if try_grow failed
                                # annotate this entry so it can be shown in the chart
                                print(consumer, ",", alloc[consumer], ",ERR")
                            else:
                                alloc[consumer] = alloc[consumer] + size
                        elif method == "shrink":
                            alloc[consumer] = alloc[consumer] - size

                    print(consumer, ",", alloc[consumer])

                except Exception as e:
                    print("error parsing", line, e, file=sys.stderr)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Generate CSV From memory debug output")
    ap.add_argument("--task", default=None, help="Task ID.")
    ap.add_argument("--file", default=None, help="Spark log containing memory debug output")
    args = ap.parse_args()
    main(args.file, int(args.task))
