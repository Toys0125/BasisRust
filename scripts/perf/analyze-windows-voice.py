#!/usr/bin/env python3
"""Validate a captured Windows voice workload, including sampled Opus bytes."""

import argparse
import collections
import csv
import hashlib
import json
import math
import pathlib
import shutil
import struct
import subprocess

import importlib.util

spec = importlib.util.spec_from_file_location("voice_runner", pathlib.Path(__file__).with_name("run-windows-voice-workload.py"))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


def rows(path):
    with path.open(newline="", encoding="utf-8-sig") as stream:
        return list(csv.DictReader(stream))


def crc_ogg(data):
    crc = 0
    for byte in data:
        crc ^= byte << 24
        for _ in range(8):
            crc = ((crc << 1) ^ (0x04C11DB7 if crc & 0x80000000 else 0)) & 0xFFFFFFFF
    return crc


def opus_ogg(packets):
    head = b"OpusHead" + struct.pack("<BBHIhB", 1, 1, 0, 48000, 0, 0)
    tags = b"OpusTags" + struct.pack("<I", 10) + b"BasisVoice" + struct.pack("<I", 0)
    output, granule = bytearray(), 0
    contents = [(head, 0), (tags, 0)] + packets
    for index, (packet, duration_ms) in enumerate(contents):
        granule += duration_ms * 48
        lacing = bytes([255] * (len(packet) // 255) + [len(packet) % 255])
        flags = (2 if index == 0 else 0) | (4 if index == len(contents) - 1 else 0)
        page = bytearray(b"OggS" + struct.pack("<BBQIIIB", 0, flags, granule, 1, index, 0, len(lacing)) + lacing + packet)
        struct.pack_into("<I", page, 22, crc_ogg(page))
        output.extend(page)
    return bytes(output)


def validate_samples(path, audio_folder):
    hashes = set()
    for encoded in audio_folder.glob("*.opus"):
        hashes.update(hashlib.sha256(packet).digest() for packet in runner.ogg_packets(encoded))
    data = (path / "voice.sample.bin").read_bytes()
    groups, offset, mismatches = collections.defaultdict(list), 0, 0
    while offset < len(data):
        if offset + 6 > len(data):
            raise ValueError("truncated voice sample header")
        peer, sequence, duration, length = struct.unpack_from("<HBBH", data, offset)
        offset += 6
        packet = data[offset:offset + length]
        if len(packet) != length:
            raise ValueError("truncated voice sample packet")
        offset += length
        mismatches += hashlib.sha256(packet).digest() not in hashes
        groups[peer].append((packet, duration))
    results = []
    ffmpeg, rtk = shutil.which("ffmpeg"), shutil.which("rtk")
    for peer, packets in sorted(groups.items()):
        sample = path / f"received-peer-{peer}.ogg"
        sample.write_bytes(opus_ogg(packets))
        command = [rtk, "proxy", ffmpeg, "-hide_banner", "-loglevel", "error", "-xerror", "-err_detect", "explode",
                   "-i", str(sample), "-f", "f32le", "pipe:1"]
        decoded = subprocess.run(command, capture_output=True)
        expected_bytes = sum(duration for _, duration in packets) * 48 * 4
        samples = struct.unpack(f"<{len(decoded.stdout) // 4}f", decoded.stdout) if len(decoded.stdout) % 4 == 0 else ()
        finite = all(math.isfinite(value) for value in samples)
        results.append({"peer": peer, "packets": len(packets), "duration_ms": expected_bytes / 192,
                        "decoded_bytes": len(decoded.stdout), "expected_decoded_bytes": expected_bytes,
                        "decode_exit_code": decoded.returncode, "decode_errors": decoded.stderr.decode(errors="replace"),
                        "finite_samples": finite, "non_silent": any(abs(value) > 1e-6 for value in samples),
                        "valid": decoded.returncode == 0 and len(decoded.stdout) == expected_bytes and finite and bool(samples)})
    return {"sampled_sources": len(groups), "sampled_packets": sum(map(len, groups.values())),
            "payload_mismatches": mismatches, "decodes": results,
            "valid": bool(results) and mismatches == 0 and all(result["valid"] for result in results)}


def summarize(path, audio_folder):
    meta = json.loads((path / "workload.json").read_text())
    observer = {}
    with (path / "observer.csv").open(newline="", encoding="utf-8-sig") as stream:
        for row in csv.reader(stream):
            if len(row) != 2 or row[0] == "observed_channel":
                break
            if row[0] != "metric":
                observer[row[0]] = row[1]
    process = rows(path / "process-metrics.csv")
    health = [json.loads(line) for line in (path / "health.jsonl").read_text().splitlines()]
    elapsed = float(process[-1]["unix_seconds"]) - float(process[0]["unix_seconds"])
    metrics = {"process_counter_seconds": elapsed}
    for label in ("server", "client"):
        for kind in ("cpu", "kernel_cpu", "user_cpu"):
            key = f"{label}_{kind}_seconds"
            metrics[f"{label}_{kind}_cores"] = (float(process[-1][key]) - float(process[0][key])) / elapsed if elapsed > 0 else None
        for kind in ("working_set", "commit_charge"):
            metrics[f"{label}_{kind}_peak_mib"] = max(float(row[f"{label}_{kind}_bytes"]) for row in process) / 2**20
    seconds = health[-1]["unix_seconds"] - health[0]["unix_seconds"]
    metrics["health_counter_seconds"] = seconds
    for key, name, divisor in (("bytesOut", "udp_payload_egress_mb_s", 1_000_000),
                               ("bytesIn", "udp_payload_ingress_mb_s", 1_000_000),
                               ("packetsOut", "udp_datagrams_out_s", 1), ("packetsIn", "udp_datagrams_in_s", 1)):
        metrics[name] = (health[-1]["extended"]["rawUdp"][key] - health[0]["extended"]["rawUdp"][key]) / seconds / divisor if seconds > 0 else None
    for key in ("gap_p50_ms", "gap_p95_ms"):
        metrics["avatar_" + key] = float(observer.get(key, 0))
    metrics["avatar_observer_applied_items_s"] = (int(observer.get("applied_full_items", 0)) + int(observer.get("applied_delta_items", 0))) / max(int(observer["window_ms"]) / 1000, 0.001)
    senders = rows(path / "observer.sender.csv")
    checks = {
        "measurement_completed": meta.get("measurement_complete", True),
        "all_clients_authenticated_active": all(int(row["players_online"]) == meta["clients"] and int(row["active_states"]) == meta["clients"] for row in process),
        "sender_records_complete": len(senders) == meta["clients"],
        "all_avatar_senders_connected": all(row["connected_at_end"] == "true" for row in senders),
        "zero_avatar_send_errors": all(int(row["send_errors"]) == 0 for row in senders),
        "all_avatar_senders_progressed": all(int(row["socket_sent_full"]) + int(row["socket_sent_delta"]) > 0 for row in senders),
        "avatar_window_complete": observer.get("window_started") == "true" and int(observer["window_ms"]) == meta["measurement_window_seconds"] * 1000,
        "observer_avatar_peers_complete": int(observer["near_peers"]) == meta["clients"] - 1,
        "client_clean_exit": meta["client_exit_code"] == 0,
        "server_controlled_stop": meta["server_exit_code"] in (0, 3221225786),
        "zero_udp_would_block": all(item["extended"]["rawUdp"]["wouldBlock"] == 0 for item in health),
        "zero_server_protocol_errors": all(item["extended"]["appMessages"]["protocolErrors"] == 0 for item in health),
        "zero_non_reliable_drops": all(item.get("transport", {}).get("nonReliableDroppedDatagrams", 0) == 0 for item in health),
    }
    for key in ("missing_expected_peers", "stale_peers_500ms", "decode_errors", "unapplied_deltas", "malformed_items", "non_newer_sequences"):
        checks["zero_avatar_" + key] = int(observer.get(key, 0)) == 0
    voice, audio_validation, capacity = None, None, {}
    if meta["voice"]:
        voice_rows = rows(path / "voice.csv")
        voice = {row["metric"]: int(row["value"]) for row in rows(path / "voice.summary.csv")}
        peers = rows(path / "voice.peers.csv")
        gaps = rows(path / "voice.gaps.csv")
        window = max(voice["window_ms"] / 1000, 0.001)
        received_counts = [int(row["received_packets"]) for row in voice_rows]
        sent_counts = [int(row["sent_packets"]) for row in voice_rows]
        total_gaps = sum(int(row["count"]) for row in gaps)
        for percentile in (50, 95, 99):
            cumulative = 0
            for row in gaps:
                cumulative += int(row["count"])
                if cumulative >= total_gaps * percentile / 100:
                    metrics[f"voice_gap_p{percentile}_floor_ms"] = int(row["gap_floor_ms"])
                    break
        metrics.update(voice_sent_packets_s=voice["sent_packets"] / window,
                       voice_received_packets_s=voice["received_packets"] / window,
                       voice_sent_opus_mb_s=voice["sent_opus_bytes"] / window / 1_000_000,
                       voice_received_opus_mb_s=voice["received_opus_bytes"] / window / 1_000_000,
                       voice_senders_observed=sum(value > 0 for value in sent_counts),
                       voice_receivers_observed=sum(value > 0 for value in received_counts),
                       voice_min_received_packets=min(received_counts), voice_max_received_packets=max(received_counts),
                       voice_min_sent_packets=min(sent_counts),
                       voice_forward_missing_mod256=sum(int(row["forward_missing_mod256"]) for row in peers),
                       voice_duplicate_packets=sum(int(row["duplicates"]) for row in peers),
                       voice_reordered_or_ambiguous=sum(int(row["reordered_or_ambiguous"]) for row in peers),
                       voice_max_gap_ms=max((float(row["max_gap_ms"]) for row in peers), default=0))
        fanout_expected = voice["sent_packets"] * (meta["clients"] - 1)
        metrics["voice_receipt_to_expected_fanout_ratio"] = voice["received_packets"] / fanout_expected if fanout_expected else 0
        target_speakers = math.ceil(meta["clients"] * meta["voice_speaker_percent"] / 100)
        metrics["voice_target_speakers"] = target_speakers
        metrics["voice_nominal_send_packets_s"] = target_speakers * 50
        metrics["voice_send_cadence_fraction"] = metrics["voice_sent_packets_s"] / (target_speakers * 50)
        audio_validation = validate_samples(path, audio_folder)
        checks.update(voice_window_complete=abs(voice["window_ms"] - meta["measurement_window_seconds"] * 1000) < 100,
                      voice_records_complete=len(voice_rows) == meta["clients"],
                      voice_active=voice["sent_packets"] > 0 and voice["received_packets"] > 0,
                      all_clients_received_voice=all(value > 0 for value in received_counts),
                      zero_voice_send_errors=voice["send_errors"] == 0,
                      zero_skipped_voice_packets=voice["skipped_packets"] == 0,
                      zero_malformed_voice_packets=voice["malformed_packets"] == 0,
                      zero_self_voice_packets=voice["self_received_packets"] == 0,
                      received_audio_matches_and_decodes=audio_validation["valid"])
        if meta["voice_speaker_percent"] == 100:
            checks["all_clients_sent_voice"] = all(value > 0 for value in sent_counts)
        capacity = {"at_least_95pct_nominal_voice_send_rate": metrics["voice_send_cadence_fraction"] >= 0.95,
                    "at_least_99pct_expected_fanout_received": metrics["voice_receipt_to_expected_fanout_ratio"] >= 0.99,
                    "observer_voice_p95_under_40ms": metrics.get("voice_gap_p95_floor_ms", 10000) < 40}
    return {"name": path.name, "capture": str(path), "valid": all(checks.values()), "checks": checks,
            "capacity_pass": all(capacity.values()) if capacity else None, "capacity_checks": capacity,
            "metrics": metrics, "voice": voice, "audio_validation": audio_validation,
            "avatar_observer": observer, "readiness_samples": len(process), "metadata": meta}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("capture", type=pathlib.Path)
    parser.add_argument("--audio-folder", type=pathlib.Path, required=True)
    args = parser.parse_args()
    result = summarize(args.capture.resolve(), args.audio_folder.resolve())
    (args.capture / "validated-summary.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"name": result["name"], "valid": result["valid"], "capacity_pass": result["capacity_pass"],
                      "failed_checks": [key for key, value in result["checks"].items() if not value],
                      "metrics": result["metrics"]}, indent=2), flush=True)


if __name__ == "__main__":
    main()
