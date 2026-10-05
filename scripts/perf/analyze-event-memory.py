#!/usr/bin/env python3
"""Correlate event-dispatch task counts with Windows process memory captures."""

import argparse
import bisect
import csv
import json
import pathlib
import statistics


def rows(path):
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8-sig") as stream:
        return list(csv.DictReader(stream))


def summarize(path):
    metadata = json.loads((path / "workload.json").read_text())
    diagnostics = rows(path / "event-tasks.csv")
    process = rows(path / "process-metrics.csv")
    post = rows(path / "post-client-metrics.csv")
    peak = max(diagnostics, key=lambda row: int(row["waiting"]))
    timestamps = [float(row["unix_seconds"]) for row in diagnostics]
    pairs = []
    for sample in process:
        now = float(sample["unix_seconds"])
        index = bisect.bisect_left(timestamps, now)
        if index == 0 or index == len(timestamps):
            continue
        a, b = diagnostics[index - 1], diagnostics[index]
        elapsed = timestamps[index] - timestamps[index - 1]
        fraction = (now - timestamps[index - 1]) / elapsed
        waiting = int(a["waiting"]) + fraction * (int(b["waiting"]) - int(a["waiting"]))
        pairs.append((waiting, int(sample["server_working_set_bytes"])))
    fit = None
    if len(pairs) >= 3:
        xs, ys = zip(*pairs)
        mean_x, mean_y = statistics.mean(xs), statistics.mean(ys)
        spread_x = sum((x - mean_x) ** 2 for x in xs)
        spread_y = sum((y - mean_y) ** 2 for y in ys)
        if spread_x > 0 and spread_y > 0 and max(xs) - min(xs) >= 1000:
            slope = sum((x - mean_x) * (y - mean_y) for x, y in pairs) / spread_x
            intercept = mean_y - slope * mean_x
            fit = {"bytes_per_waiting_task": slope, "working_set_intercept_mib": intercept / 2**20,
                   "r_squared": 1 - sum((y - (intercept + slope * x)) ** 2 for x, y in pairs) / spread_y,
                   "samples": len(pairs), "method": "linear interpolation of 1s task counters at process-sample times; ordinary least squares"}
    first, last = process[0], process[-1]
    elapsed = float(last["unix_seconds"]) - float(first["unix_seconds"])
    peak_working_set = max(int(row["server_working_set_bytes"]) for row in process)
    memory_samples = process + post
    result = {
        "name": path.name, "capture": str(path), "metadata": metadata,
        "peak_waiting_tasks": int(peak["waiting"]),
        "peak_allocated_event_tasks": max(int(row["waiting"]) + int(row["running"]) for row in diagnostics),
        "voice_waiting_at_peak": int(peak["voice_waiting"]),
        "delta_waiting_at_peak": int(peak["delta_waiting"]) if "delta_waiting" in peak else None,
        "peak_tokio_alive_tasks": max(int(row["tokio_alive_tasks"]) for row in diagnostics),
        "peak_running_handlers": max(int(row["running"]) for row in diagnostics),
        "worker_limit": int(peak["worker_limit"]),
        "task_future_bytes": int(peak["task_future_bytes"]),
        "waiting_future_frames_mib_at_peak": int(peak["waiting"]) * int(peak["task_future_bytes"]) / 2**20,
        "handle_event_future_bytes": int(peak["handle_event_future_bytes"]),
        "voice_relay_future_bytes": int(peak["voice_relay_future_bytes"]),
        "load_peak_server_working_set_mib": peak_working_set / 2**20,
        "load_or_post_peak_server_working_set_mib": max(int(row["server_working_set_bytes"]) for row in memory_samples) / 2**20,
        "load_server_growth_mib_s": (int(last["server_working_set_bytes"]) - int(first["server_working_set_bytes"])) / elapsed / 2**20 if elapsed > 0 else None,
        "load_server_cpu_cores": (float(last["server_cpu_seconds"]) - float(first["server_cpu_seconds"])) / elapsed if elapsed > 0 else None,
        "post_final_server_working_set_mib": int(post[-1]["server_working_set_bytes"]) / 2**20 if post else None,
        "post_final_players_online": int(post[-1]["players_online"]) if post else None,
        "final_waiting_tasks": int(diagnostics[-1]["waiting"]),
        "final_running_handlers": int(diagnostics[-1]["running"]),
        "max_event_queue_depth": max(int(row["event_queue_depth"]) for row in diagnostics) if "event_queue_depth" in diagnostics[0] else None,
        "task_to_memory_fit": fit,
        "all_clients_active_during_measurement": all(int(row["players_online"]) == metadata["clients"] and int(row["active_states"]) == metadata["clients"] for row in process),
        "client_clean_exit": metadata["client_exit_code"] == 0,
    }
    (path / "memory-summary.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("captures", nargs="+", type=pathlib.Path)
    args = parser.parse_args()
    for path in args.captures:
        result = summarize(path.resolve())
        print(json.dumps({key: value for key, value in result.items() if key not in ("metadata", "capture")}, indent=2))


if __name__ == "__main__":
    main()
