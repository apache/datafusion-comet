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

"""Conservative Python value/import analysis without importing upstream code.

Bindings are deliberately file-wide: any shadow, reassignment, wildcard import
or dynamic namespace access disables the affected proof (sometimes more than a
scope-aware analyzer would). Only unconditional module string constants and
string addition are resolved. Function arguments/defaults are not propagated.
"""

import ast
from collections import Counter
import re
import string


FUNCTIONS = "pyspark.sql.functions"
RLIKE_APIS = {"rlike", "regexp", "regexp_like"}
OTHER_APIS = {"regexp_extract", "regexp_extract_all", "regexp_replace", "regexp_count",
              "regexp_substr", "regexp_instr"}


def dotted(node):
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = dotted(node.value)
        return parent + "." + node.attr if parent else None
    return None


def arguments(call, parameters):
    """Bind a small known signature, rejecting expansion, duplicates and extra args."""
    if len(call.args) > len(parameters) or any(isinstance(a, ast.Starred) for a in call.args):
        return None
    values = dict(zip(parameters, call.args))
    for keyword in call.keywords:
        if keyword.arg not in parameters or keyword.arg in values:
            return None
        values[keyword.arg] = keyword.value
    return values if set(values) == set(parameters) else None


class PythonValues:
    def __init__(self, text, rule_constructors=()):
        self.text = text
        self.tree = ast.parse(text)
        self.writes = Counter()
        self.imports = {}
        self.constants = {}
        self.dynamic_namespace = False
        rule_modules = {name.rsplit(".", 1)[0] for name in rule_constructors}
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Store, ast.Del)):
                self.writes[node.id] += 1
            elif isinstance(node, (ast.Attribute, ast.Subscript)) and isinstance(
                    node.ctx, (ast.Store, ast.Del)):
                target = node.value
                while isinstance(target, (ast.Attribute, ast.Subscript)):
                    target = target.value
                if isinstance(target, ast.Name):
                    self.writes[target.id] += 1
            elif isinstance(node, ast.arg):
                self.writes[node.arg] += 1
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                self.writes[node.name] += 1
            elif isinstance(node, (ast.Global, ast.Nonlocal)):
                for name in node.names:
                    self.writes[name] += 1
            elif isinstance(node, ast.ExceptHandler) and node.name:
                self.writes[node.name] += 1
            elif isinstance(node, (ast.MatchAs, ast.MatchStar)) and node.name:
                self.writes[node.name] += 1
            elif isinstance(node, ast.MatchMapping) and node.rest:
                self.writes[node.rest] += 1
            elif isinstance(node, (ast.Import, ast.ImportFrom)):
                for alias in node.names:
                    name = alias.asname or alias.name.split(".")[0]
                    self.writes[name] += 1
                    if alias.name == "*":
                        self.dynamic_namespace = True
            elif isinstance(node, ast.Call) and dotted(node.func) in {
                    "exec", "eval", "globals", "locals", "vars", "setattr", "delattr"}:
                self.dynamic_namespace = True
        for node in self.tree.body:
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == FUNCTIONS:
                        name = alias.asname or "pyspark"
                        canonical = alias.name if alias.asname else "pyspark"
                        self.imports[name] = (canonical, node.lineno)
            elif isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    if node.level == 0 and (
                            node.module in rule_modules or node.module == FUNCTIONS or
                            node.module == "pyspark.sql" and alias.name == "functions"):
                        self.imports[alias.asname or alias.name] = (
                            node.module + "." + alias.name, node.lineno)
            elif isinstance(node, (ast.Assign, ast.AnnAssign)):
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                if node.value is None:
                    continue
                value = self.resolve(node.value)
                for target in targets:
                    if isinstance(target, ast.Name) and self.writes[target.id] == 1 and value:
                        self.constants[target.id] = {
                            "value": value[0], "line": node.lineno,
                            "evidence": value[2] + [{"name": target.id, "line": node.lineno,
                                                     "expression": self.source(node.value)}],
                        }

    def source(self, node):
        return ast.get_source_segment(self.text, node)

    def symbol(self, node):
        name = dotted(node)
        if not name or self.dynamic_namespace:
            return None
        head, _, tail = name.partition(".")
        imported = self.imports.get(head)
        if not imported or self.writes[head] != 1 or imported[1] >= node.lineno:
            return None
        return imported[0] + ("." + tail if tail else "")

    def resolve(self, node, depth=0):
        if depth > 32:
            return None
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value, "DIRECT_LITERAL", []
        if isinstance(node, ast.Name) and not self.dynamic_namespace:
            constant = self.constants.get(node.id)
            if constant and self.writes[node.id] == 1 and constant["line"] < node.lineno:
                return constant["value"], "RESOLVED_CONSTANT", constant["evidence"]
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            left, right = self.resolve(node.left, depth + 1), self.resolve(node.right, depth + 1)
            if left and right:
                return left[0] + right[0], "RESOLVED_CONSTANT", left[2] + right[2]
        return None

    def coordinates(self, node, name=None):
        if name and isinstance(node, ast.Attribute):
            line = node.end_lineno
            byte_column = node.end_col_offset - len(name)
        else:
            line, byte_column = node.lineno, node.col_offset
        prefix = self.text.splitlines()[line - 1].encode("utf-8")[:byte_column]
        return line, len(prefix.decode("utf-8")) + 1


def template_carriers(values):
    """Audit dynamic SQL text without evaluating holes, defaults, or .format()."""
    carriers = []
    for node in ast.walk(values.tree):
        parts, holes = [], []
        if isinstance(node, ast.JoinedStr):
            for item in node.values:
                if isinstance(item, ast.Constant) and isinstance(item.value, str):
                    parts.append(item.value)
                elif isinstance(item, ast.FormattedValue):
                    marker = f"__REGEX_DYNAMIC_{len(holes)}__"
                    holes.append({"marker": marker, "expression": values.source(item.value)})
                    parts.append(marker)
            kind = "python-fstring-template"
        elif (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and
              node.func.attr == "format" and isinstance(node.func.value, ast.Constant) and
              isinstance(node.func.value.value, str)):
            try:
                for literal, field, spec, conversion in string.Formatter().parse(node.func.value.value):
                    parts.append(literal)
                    if field is not None:
                        marker = f"__REGEX_DYNAMIC_{len(holes)}__"
                        holes.append({"marker": marker, "expression": field})
                        parts.append(marker)
            except ValueError:
                continue
            kind = "python-format-template"
        else:
            continue
        template = "".join(parts)
        if holes and re.search(r"\b(?:rlike|regexp_like|regexp)\b", template, re.IGNORECASE):
            line, column = values.coordinates(node)
            carriers.append({"line": line, "column": column, "form": kind,
                             "template_text": template, "template_holes": holes,
                             "host_expression": values.source(node)})
    return carriers


def inventory(text, audit_templates=None, non_rlike_methods=(), rule_constructors=()):
    values = PythonValues(text, rule_constructors)
    rows = []
    for call in ast.walk(values.tree):
        if not isinstance(call, ast.Call):
            continue
        canonical = values.symbol(call.func)
        if canonical in rule_constructors:
            keywords = {k.arg: k.value for k in call.keywords}
            kind = keywords.get("check_type")
            if not (isinstance(kind, ast.Constant) and kind.value == "has_pattern"):
                continue
            line, column = values.coordinates(call.func)
            row = {"line": line, "column": column, "operator": "rlike",
                   "form": "reviewed-python-rule", "status": "non_direct_literal_or_unparsed",
                   "classification": "UNKNOWN", "api_binding": canonical,
                   "api_binding_verified": False,
                   "extraction_reason": "manifest_reviewed_rule_consumer"}
            argument = keywords.get("value")
            resolved = values.resolve(argument) if argument is not None else None
            if not call.args and None not in keywords and resolved:
                row.update(classification="RESOLVED_CONSTANT", status="resolved_constant_candidate",
                           pattern_value=resolved[0], literal_python=values.source(argument),
                           constant_resolution=resolved[2], constant_resolution_verified=True,
                           resolution_kind="reviewed_rule_constructor")
            rows.append(row)
            continue
        operator = canonical.rsplit(".", 1)[-1] if canonical else None
        bound_function = bool(operator in RLIKE_APIS | OTHER_APIS and
                              canonical == FUNCTIONS + "." + operator)
        method = isinstance(call.func, ast.Attribute) and call.func.attr == "rlike"
        if isinstance(call.func, ast.Attribute) and call.func.attr in non_rlike_methods:
            line, column = values.coordinates(call.func, call.func.attr)
            rows.append({"line": line, "column": column, "operator": call.func.attr,
                         "form": "reviewed-framework-method", "classification": "NON_RLIKE_REGEX",
                         "status": "non_rlike_regex", "api_binding_verified": False,
                         "extraction_reason": "source_manifest_backend_contract"})
            continue
        raw_head = (dotted(call.func) or "").split(".")[0]
        imported = values.imports.get(raw_head)
        possible_rlike_alias = bool(imported and imported[0].rsplit(".", 1)[-1] in RLIKE_APIS)
        if not bound_function and not method and not possible_rlike_alias:
            continue
        name = operator if bound_function else "rlike"
        line, column = values.coordinates(call.func, name if isinstance(call.func, ast.Attribute) else None)
        row = {"line": line, "column": column, "operator": name,
               "form": "python-function" if bound_function else "python-method",
               "status": "non_direct_literal_or_unparsed", "classification": "UNKNOWN",
               "api_binding_verified": bound_function}
        if not bound_function and (possible_rlike_alias or imported):
            row.update(form="python-function-unresolved",
                       extraction_reason="unproved_or_shadowed_function_binding")
            rows.append(row)
            continue
        if bound_function:
            row["api_binding"] = canonical
            if name in OTHER_APIS:
                row.update(classification="NON_RLIKE_REGEX", status="non_rlike_regex")
                rows.append(row)
                continue
            bound = arguments(call, ("str", "regexp"))
            if bound is None:
                row["extraction_reason"] = "unsupported_function_call_shape"
                rows.append(row)
                continue
            argument = bound["regexp"]
            row["pattern_expression"] = values.source(argument)
            # In functions.rlike a string is a COLUMN NAME, not a regex literal.
            if not (isinstance(argument, ast.Call) and
                    values.symbol(argument.func) == FUNCTIONS + ".lit"):
                row.update(classification="DYNAMIC_PATTERN", extraction_reason="pattern_column_expression")
                rows.append(row)
                continue
            literal = arguments(argument, ("col",))
            if literal is None:
                row["extraction_reason"] = "unsupported_lit_call_shape"
                rows.append(row)
                continue
            row["literal_wrapper"] = values.source(argument)
            argument = literal["col"]
        else:
            row["receiver"] = values.source(call.func.value)
            bound = arguments(call, ("other",))
            if bound is None:
                row["extraction_reason"] = "unsupported_method_call_shape"
                rows.append(row)
                continue
            argument = bound["other"]
        row["pattern_expression"] = values.source(argument)
        resolved = values.resolve(argument)
        if resolved:
            value, classification, evidence = resolved
            row.update(classification=classification, pattern_value=value,
                       literal_python=values.source(argument), constant_resolution=evidence,
                       constant_resolution_verified=True,
                       status="direct_literal_candidate" if classification == "DIRECT_LITERAL"
                       else "resolved_constant_candidate")
        else:
            non_string = isinstance(argument, ast.Constant) and not isinstance(argument.value, str)
            row.update(classification="UNKNOWN" if non_string else "DYNAMIC_PATTERN",
                       extraction_reason="non_string_literal" if non_string else "runtime_or_unresolved_value")
        rows.append(row)
    if audit_templates is not None:
        audit_templates.extend(template_carriers(values))
    return sorted(rows, key=lambda r: (r["line"], r["column"]))
