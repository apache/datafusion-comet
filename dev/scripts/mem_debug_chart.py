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
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv", help="CSV with columns: name,size")
    ap.add_argument("--instant", action="store_true",
                    help="Plot per-step stacked values (not cumulative totals).")
    ap.add_argument("--bar", action="store_true",
                    help="Use stacked bars instead of stacked area.")
    ap.add_argument("--title", default=None, help="Optional plot title.")
    args = ap.parse_args()

    path = Path(args.csv)
    df = pd.read_csv(path)

    # Validate + clean
    need = {"name", "size"}
    if not need.issubset(set(df.columns)):
        raise SystemExit("CSV must have columns: name,size")

    df["size"] = pd.to_numeric(df["size"], errors="coerce").fillna(0)

    # Treat each row as the next time step: t = 1..N
    df = df.reset_index(drop=True).assign(t=lambda d: d.index + 1)

    # Build wide matrix: one column per name, one row per time step
    # If multiple entries exist for the same (t, name), they’ll be summed.
    wide = (
        df.groupby(["t", "name"], as_index=False)["size"].sum()
        .pivot(index="t", columns="name", values="size")
        .fillna(0.0)
        .sort_index()
    )

    # Running totals unless --instant specified
    plot_data = wide if args.instant else wide.cumsum(axis=0)

    # Plot
    if args.bar:
        ax = plot_data.plot(kind="bar", stacked=True, figsize=(12, 6), width=1.0)
    else:
        ax = plot_data.plot.area(stacked=True, figsize=(12, 6))

    ax.set_xlabel("step")
    ax.set_ylabel("size" if args.instant else "cumulative size")
    ax.set_title(args.title or ("Stacked running totals by name" if not args.instant
                                else "Stacked per-step values by name"))
    ax.legend(title="name", bbox_to_anchor=(1.02, 1), loc="upper left")
    plt.tight_layout()

    out = path.with_suffix(".stacked.png" if args.instant else ".stacked_cumulative.png")
    plt.savefig(out, dpi=150)
    print(f"Saved plot to {out}")
    plt.show()

if __name__ == "__main__":
    main()
