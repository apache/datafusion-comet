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

"""
Config loader for the unified benchmark runner.

Reads key=value .conf files, merges them with precedence
(profile < engine < CLI overrides), and splits into spark vs runner configs.

The ``runner.*`` namespace controls the shell wrapper (JAR paths, env vars,
result name) without colliding with Spark config keys.  Examples:
  runner.jars=${COMET_JAR}
  runner.env.TZ=UTC
  runner.name=comet
"""

import os
import re
from typing import Dict, List, Tuple


def load_conf_file(path: str) -> Dict[str, str]:
    """Read a key=value .conf file.

    - Blank lines and lines starting with ``#`` are skipped.
    - ``${VAR}`` references are expanded from the environment.
    - Values may optionally be quoted with single or double quotes.
    """
    conf: Dict[str, str] = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            if not key or not _:
                continue
            # Strip optional quotes
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                value = value[1:-1]
            # Expand ${VAR} references from environment
            value = re.sub(
                r"\$\{(\w+)\}",
                lambda m: os.environ.get(m.group(1), m.group(0)),
                value,
            )
            conf[key] = value
    return conf


def merge_configs(
    profile_path: str = None,
    engine_path: str = None,
    cli_overrides: List[str] = None,
) -> Dict[str, str]:
    """Merge configs with precedence: profile < engine < CLI overrides."""
    merged: Dict[str, str] = {}
    if profile_path:
        merged.update(load_conf_file(profile_path))
    if engine_path:
        merged.update(load_conf_file(engine_path))
    for override in cli_overrides or []:
        key, _, value = override.partition("=")
        key = key.strip()
        value = value.strip()
        if key and _:
            # Expand ${VAR} in CLI overrides too
            value = re.sub(
                r"\$\{(\w+)\}",
                lambda m: os.environ.get(m.group(1), m.group(0)),
                value,
            )
            merged[key] = value
    return merged


def split_config(merged: Dict[str, str]) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Separate ``runner.*`` keys from ``spark.*`` (and other) keys.

    Returns (spark_conf, runner_conf) where runner_conf has the
    ``runner.`` prefix stripped.
    """
    spark_conf: Dict[str, str] = {}
    runner_conf: Dict[str, str] = {}
    for key, value in merged.items():
        if key.startswith("runner."):
            runner_conf[key[len("runner."):]] = value
        else:
            spark_conf[key] = value
    return spark_conf, runner_conf
