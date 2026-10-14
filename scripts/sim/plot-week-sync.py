#!/usr/bin/env python3
"""Plot observed hourly coverage from run-week-sync.py's result.json."""
import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("result", type=Path)
parser.add_argument("output", type=Path, help="output filename prefix")
args = parser.parse_args()
result = json.loads(args.result.read_text())
hours = result["hourly"]
days = (len(hours) + 23) // 24
fig, axes = plt.subplots(3, 1, figsize=(12, 9), constrained_layout=True)
for axis, (key, title, unit, cmap) in zip(axes, [
    ("active", "Connected players after hourly churn", "players", "viridis"),
    ("joins", "Verified admissions and 100-byte owner snapshots", "joins", "Blues"),
    ("overload_voice_dropped", "Observed voice losses in intentional 32-frame overload windows", "frames", "Oranges"),
]):
    values = np.full((days, 24), np.nan)
    for hour in hours:
        values[hour["hour"] // 24, hour["hour"] % 24] = hour[key]
    chart = axis.imshow(values, aspect="auto", cmap=cmap, vmin=0)
    axis.set_title(title, loc="left", fontsize=12)
    axis.set_yticks(range(days), [f"Day {day + 1}" for day in range(days)])
    axis.set_xticks(range(0, 24, 2))
    axis.set_xlabel("Logical hour of day")
    fig.colorbar(chart, ax=axis, label=unit, fraction=0.018, pad=0.01)
fig.suptitle(
    f"Observed week simulation · {result['logical_hours']} logical hours · "
    f"peak {result['observed_peak']} players · {result['protocol_errors']} protocol errors\n"
    "Real loopback UDP; accelerated schedule, production timers remain wall-clock",
    fontsize=14,
)
args.output.parent.mkdir(parents=True, exist_ok=True)
for extension in ("png", "svg"):
    fig.savefig(args.output.with_suffix(f".{extension}"), dpi=160)
plt.close(fig)
