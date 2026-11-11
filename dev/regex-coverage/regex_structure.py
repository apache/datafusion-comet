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

"""Conservative lexical structure inventory; never decides Comet admission.

Unknown regex shapes produce partial results. Measurements and blocker witnesses
must come from the real Scala analyzer. Offsets are decoded Unicode code points.
"""

import re


META_ESCAPES = set(".*+?()[]{}|^$\\")
SHORTHANDS = {c: "SHORTHAND_DIGIT" for c in "dD"}
SHORTHANDS.update({c: "SHORTHAND_WORD" for c in "wW"})
SHORTHANDS.update({c: "SHORTHAND_SPACE" for c in "sS"})
COUNTED = re.compile(r"\{([0-9]+)(?:,([0-9]*))?\}")
EXPERIMENTS = {
    "leading_anchor": {"ANCHOR_START"},
    "both_anchor_types": {"ANCHOR_START", "ANCHOR_END"},
    "dot_wildcard": {"DOT_WILDCARD"},
    "digit_shorthand": {"SHORTHAND_DIGIT"},
    "all_observed_blockers": {
        "ANCHOR_START", "ANCHOR_END", "DOT_WILDCARD", "SHORTHAND_DIGIT",
        "SHORTHAND_WORD", "SHORTHAND_SPACE", "CONTROL_CHARACTER", "NON_ASCII",
        "CONTROL_ESCAPE",
    },
}


class UnknownShape(Exception):
    pass


def scan(pattern):
    metrics = {"length_codepoints": len(pattern),
               "length_utf16": len(pattern.encode("utf-16-le", errors="surrogatepass")) // 2,
               "ascii_only": pattern.isascii(), "character_classes": 0,
               "quantifiers": 0, "groups": 0, "max_group_depth": 0,
               "alternations": 0, "max_quantifier_nesting": 0,
               "max_counted_bound": 0}
    frames = [{"max_q": 0, "last_q": None}]
    blockers = []

    def atom(q=0):
        frames[-1]["last_q"] = q
        frames[-1]["max_q"] = max(frames[-1]["max_q"], q)

    def block(start, end, tag, context="outside_class"):
        blockers.append({"start": start, "end": end, "tag": tag, "context": context})

    def character(index, context):
        c = pattern[index]
        if ord(c) < 0x20 or ord(c) == 0x7f:
            block(index, index + 1, "CONTROL_CHARACTER", context)
        elif ord(c) > 0x7f:
            block(index, index + 1, "NON_ASCII", context)

    def escape(index, context):
        if index + 1 == len(pattern):
            raise UnknownShape("trailing escape")
        c = pattern[index + 1]
        if c in META_ESCAPES or (context == "class" and c == "-"):
            return index + 2
        if c in SHORTHANDS:
            block(index, index + 2, SHORTHANDS[c], context)
        elif c in "nrtfb" and (c != "b" or context == "class"):
            block(index, index + 2, "CONTROL_ESCAPE", context)
        else:
            raise UnknownShape("escape outside the measured lexical subset")
        return index + 2

    i = 0
    try:
        while i < len(pattern):
            c = pattern[i]
            if c == "\\":
                i = escape(i, "outside_class")
                atom()
                continue
            if c == "[":
                metrics["character_classes"] += 1
                j = i + 1
                if j < len(pattern) and pattern[j] == "^":
                    j += 1
                if j == len(pattern) or pattern[j] == "]":
                    raise UnknownShape("empty or leading-closing-bracket class")
                while j < len(pattern) and pattern[j] != "]":
                    if pattern[j] == "[" or pattern[j:j + 2] in {"&&", "~~", "--"}:
                        raise UnknownShape("nested class or class set operator")
                    if pattern[j] == "\\":
                        j = escape(j, "class")
                    else:
                        character(j, "class")
                        j += 1
                if j == len(pattern):
                    raise UnknownShape("unclosed class")
                i = j + 1
                atom()
                continue
            if c == "(":
                if pattern.startswith("(?:", i):
                    i += 3
                elif pattern.startswith("(?", i):
                    raise UnknownShape("special group outside the measured lexical subset")
                else:
                    i += 1
                metrics["groups"] += 1
                frames.append({"max_q": 0, "last_q": None})
                metrics["max_group_depth"] = max(metrics["max_group_depth"], len(frames) - 1)
                continue
            if c == ")":
                if len(frames) == 1:
                    raise UnknownShape("unmatched group close")
                atom_q = frames.pop()["max_q"]
                atom(atom_q)
            elif c == "|":
                metrics["alternations"] += 1
                frames[-1]["last_q"] = None
            elif c in "*+?{":
                if frames[-1]["last_q"] is None:
                    raise UnknownShape("quantifier without a recognized atom")
                if c == "{":
                    counted = COUNTED.match(pattern, i)
                    if counted is None:
                        raise UnknownShape("unrecognized counted quantifier")
                    lo = int(counted[1])
                    hi = int(counted[2]) if counted[2] else lo
                    if hi < lo:
                        raise UnknownShape("inverted counted bound")
                    metrics["max_counted_bound"] = max(metrics["max_counted_bound"], lo, hi)
                    i = counted.end() - 1
                if i + 1 < len(pattern) and pattern[i + 1] in "?+":
                    raise UnknownShape("lazy or possessive quantifier outside measured subset")
                metrics["quantifiers"] += 1
                q = frames[-1]["last_q"] + 1
                frames[-1]["max_q"] = max(frames[-1]["max_q"], q)
                frames[-1]["last_q"] = None
            elif c in "^$":
                block(i, i + 1, "ANCHOR_START" if c == "^" else "ANCHOR_END")
                frames[-1]["last_q"] = None
            elif c == ".":
                block(i, i + 1, "DOT_WILDCARD")
                atom()
            elif c in "]}":
                raise UnknownShape("unrecognized unescaped delimiter")
            else:
                character(i, "outside_class")
                atom()
            i += 1
        if len(frames) != 1:
            raise UnknownShape("unclosed group")
        metrics["max_quantifier_nesting"] = frames[0]["max_q"]
        return {"complete": True, "metrics": metrics, "blockers": blockers,
                "features": sorted({b["tag"] for b in blockers})}
    except UnknownShape as error:
        return {"complete": False, "limitation": str(error), "metrics": None,
                "blockers": blockers, "features": sorted({b["tag"] for b in blockers})}


def control_pattern(pattern, blockers, selected, leading_only=False):
    """Create a synthetic syntax control, never a proposed query rewrite."""
    result = []
    start = 0
    for blocker in blockers:
        if blocker["tag"] not in selected:
            continue
        if leading_only and blocker["start"] != 0:
            continue
        result.append(pattern[start:blocker["start"]])
        result.append("" if blocker["tag"] in {"ANCHOR_START", "ANCHOR_END"} else "a")
        start = blocker["end"]
    result.append(pattern[start:])
    return "".join(result)
