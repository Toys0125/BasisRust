#!/usr/bin/env python3
"""Resolve a captured WPR trace into CPU leaf and per-process stack reports."""
import argparse
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import shutil
import re
import subprocess


class StackTables(HTMLParser):
    """Extract xperf's butterfly tables without loading them in a browser."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.section = None
        self.tables = {}
        self.row = None
        self.cell = None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "a" and attrs.get("id", "").startswith("Tbl"):
            self.section = attrs["id"]
        if tag == "tr":
            self.row = []
        if tag in ("td", "th"):
            self.cell = []

    def handle_data(self, data):
        if self.cell is not None:
            self.cell.append(data)

    def handle_endtag(self, tag):
        if tag in ("td", "th") and self.cell is not None:
            if self.row is not None:
                self.row.append("".join(self.cell).strip())
            self.cell = None
        if tag == "tr" and self.row is not None:
            self.tables.setdefault(self.section, []).append(self.row)
            self.row = None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("capture", type=Path)
    parser.add_argument("--server-pid", type=int, required=True)
    parser.add_argument("--client-pid", type=int, help="omit when the client runs on another machine")
    args = parser.parse_args()
    capture = args.capture.resolve()
    state = json.loads((capture / "native-status.json").read_text(encoding="utf-8-sig"))
    if state.get("phase") != "complete":
        parser.error("native capture has not completed")
    trace = capture / "cpu-stacks.etl"
    xperf = shutil.which("xperf")
    rtk = shutil.which("rtk")
    if not xperf or not rtk:
        parser.error("xperf and rtk are required on PATH")
    analysis = capture / "analysis"
    analysis.mkdir(exist_ok=True)
    symcache = capture / "symcache"
    symbols = capture / "microsoft-symbols"
    symcache.mkdir(exist_ok=True)
    symbols.mkdir(exist_ok=True)
    env = dict(os.environ)
    env["_NT_SYMBOL_PATH"] = str(capture / "symbols") + ";srv*" + str(symbols) + "*https://msdl.microsoft.com/download/symbols"
    env["_NT_SYMCACHE_PATH"] = str(symcache)
    tasks = [
        ("trace-times", ["-a", "tracestats", "-timespan"], False),
        ("trace-stats", ["-a", "tracestats", "-detail", "stack"], False),
        # Module-level CPU leaves avoid resolving every unrelated process's
        # symbols. The filtered butterfly reports resolve target functions.
        ("cpu-leaves", ["-a", "profile", "-detail"], False),
        ("server-stacks", ["-a", "stack", "-butterfly", "1", "-pid", str(args.server_pid)], True),
    ]
    if args.client_pid is not None:
        tasks.append(("client-stacks", ["-a", "stack", "-butterfly", "1", "-pid", str(args.client_pid)], True))
    for name, action, decode in tasks:
        print(f"Exporting {name}...", flush=True)
        extension = ".html" if name.endswith("-stacks") else ".txt"
        report = analysis / (name + extension)
        command = [rtk, "proxy", xperf, "-i", str(trace), "-o", str(report)]
        if decode:
            command += ["-symbols"]
        command += action
        with (analysis / (name + ".log")).open("w") as log:
            result = subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT)
        if result.returncode != 0:
            raise RuntimeError(f"{name} failed with status {result.returncode}; see {name}.log")
        if name == "trace-times":
            losses = re.findall(r"Total # Lost (?:Buffers|Events)\s*:\s*(\d+)", report.read_text())
            if len(losses) != 2 or any(int(count) for count in losses):
                raise RuntimeError("Trace loss could not be ruled out; see trace-times.txt")
        if name.endswith("-stacks"):
            tables = StackTables()
            tables.feed(report.read_text(encoding="utf-8-sig"))
            process = name.removesuffix("-stacks")
            expected_pid = args.server_pid if process == "server" else args.client_pid
            rows = tables.tables.get("TblP", [])
            if len(rows) < 2 or rows[1][1] != str(expected_pid) or int(rows[1][2]) == 0:
                raise RuntimeError(f"No sampled CPU stacks for {process} PID {expected_pid}")
            (analysis / (process + "-tables.json")).write_text(json.dumps(tables.tables, indent=2) + "\n")
    print(f"Native analysis exported to {analysis}", flush=True)


if __name__ == "__main__":
    main()
