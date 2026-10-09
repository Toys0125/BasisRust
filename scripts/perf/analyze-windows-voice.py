#!/usr/bin/env python3
"""Validate a captured Windows voice workload, including sampled Opus bytes."""

import argparse
import collections
import csv
import hashlib
import json
import math
import os
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


def server_controlled_stop(exit_code, platform_name=None):
    platform_name = os.name if platform_name is None else platform_name
    accepted = (0, 3221225786) if platform_name == "nt" else (0, -2)
    return exit_code in accepted


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
    if not groups:
        return {"sampled_sources": 0, "sampled_packets": 0, "payload_mismatches": mismatches,
                "decodes": results, "valid": False, "error": "no sampled voice packets"}
    ffmpeg, rtk = shutil.which("ffmpeg"), shutil.which("rtk")
    missing = [name for name, value in (("ffmpeg", ffmpeg), ("rtk", rtk)) if not value]
    if missing:
        error = "required decode tool(s) unavailable: " + ", ".join(missing)
        return {"sampled_sources": len(groups), "sampled_packets": sum(map(len, groups.values())),
                "payload_mismatches": mismatches,
                "decodes": [{"peer": peer, "packets": len(packets), "valid": False, "error": error}
                            for peer, packets in sorted(groups.items())],
                "valid": False, "error": error}
    for peer, packets in sorted(groups.items()):
        sample = path / f"received-peer-{peer}.ogg"
        sample.write_bytes(opus_ogg(packets))
        command = [rtk, "proxy", ffmpeg, "-hide_banner", "-loglevel", "error", "-xerror", "-err_detect", "explode",
                   "-i", str(sample), "-f", "f32le", "pipe:1"]
        try:
            decoded = subprocess.run(command, capture_output=True, timeout=60)
        except subprocess.TimeoutExpired:
            results.append({"peer": peer, "packets": len(packets), "duration_ms": sum(duration for _, duration in packets),
                            "decode_exit_code": None, "decode_errors": "ffmpeg decode timed out after 60 seconds",
                            "finite_samples": False, "non_silent": False, "valid": False})
            continue
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


def analyze_receipt_metrics(voice, voice_rows, clients):
    """Summarize raw arrivals and bounded sequence-tracked unique frames.

    Old captures have no per-client unique fields. Their raw ratio remains
    available for comparison, while unique delivery stays unavailable so an
    old capture cannot pass a unique-frame capacity check.
    """
    required = ("received_unique_packets", "duplicate_packets", "reordered_packets",
                "wrapped_sequence_packets", "ambiguous_sequence_packets", "unique_count_available")
    fields_present = bool(voice_rows) and all(all(key in row for key in required) for row in voice_rows)
    all_clients_present = len(voice_rows) == clients and fields_present
    available = all_clients_present and all(row["unique_count_available"].lower() == "true" for row in voice_rows)
    expected = voice["sent_packets"] * (clients - 1)
    raw_ratio = voice["received_packets"] / expected if expected else 0
    if fields_present:
        unique_received = sum(int(row["received_unique_packets"]) for row in voice_rows)
        duplicates = sum(int(row["duplicate_packets"]) for row in voice_rows)
        reordered = sum(int(row["reordered_packets"]) for row in voice_rows)
        wrapped = sum(int(row["wrapped_sequence_packets"]) for row in voice_rows)
        ambiguous = sum(int(row["ambiguous_sequence_packets"]) for row in voice_rows)
    else:
        unique_received = duplicates = reordered = wrapped = ambiguous = None
    unique_ratio = unique_received / expected if available and expected else None
    checks = {
        "voice_unique_receipt_metrics_available": available,
        "zero_voice_duplicate_packets": available and duplicates == 0,
        "zero_voice_reordered_packets": available and reordered == 0,
        "zero_voice_ambiguous_sequences": available and ambiguous == 0,
        "voice_sequence_wraps_reported": available and wrapped is not None,
    }
    metrics = {
        "voice_raw_received_packets": voice["received_packets"],
        "voice_unique_received_packets": unique_received,
        "voice_duplicate_packets": duplicates,
        "voice_reordered_packets": reordered,
        "voice_wrapped_sequence_packets": wrapped,
        "voice_ambiguous_sequence_packets": ambiguous,
        "voice_unique_receipt_metrics_available": available,
        "voice_raw_receipt_to_expected_fanout_ratio": raw_ratio,
        "voice_unique_receipt_to_expected_fanout_ratio": unique_ratio,
        # Compatibility alias; this remains the duplicate-inclusive raw ratio.
        "voice_receipt_to_expected_fanout_ratio": raw_ratio,
    }
    return metrics, checks, unique_ratio


def voice_capacity_checks(send_fraction, unique_ratio, gap_p95_ms, receipt_checks, workload_validation_passed=True):
    return {
        "at_least_95pct_nominal_voice_send_rate": send_fraction >= 0.95,
        "at_least_99pct_unique_expected_fanout_received": unique_ratio is not None and unique_ratio >= 0.99,
        "unique_receipt_metrics_available": receipt_checks["voice_unique_receipt_metrics_available"],
        "zero_voice_duplicates": receipt_checks["zero_voice_duplicate_packets"],
        "zero_voice_reordering": receipt_checks["zero_voice_reordered_packets"],
        "zero_voice_sequence_ambiguity": receipt_checks["zero_voice_ambiguous_sequences"],
        "observer_voice_p95_under_40ms": gap_p95_ms < 40,
        "workload_validation_passed": workload_validation_passed,
    }


def summarize(path, audio_folder):
    meta = json.loads((path / "workload.json").read_text())
    csharp = meta.get("server_kind") == "csharp"
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
        counter = {"bytesOut": "sent", "bytesIn": "recv", "packetsOut": "packetsSent", "packetsIn": "packetsRecv"}[key]
        first = health[0][counter] if csharp else health[0]["extended"]["rawUdp"][key]
        last = health[-1][counter] if csharp else health[-1]["extended"]["rawUdp"][key]
        metrics[name] = (last - first) / seconds / divisor if seconds > 0 else None
    for key in ("gap_p50_ms", "gap_p95_ms"):
        metrics["avatar_" + key] = float(observer.get(key, 0))
    metrics["avatar_observer_applied_items_s"] = (int(observer.get("applied_full_items", 0)) + int(observer.get("applied_delta_items", 0))) / max(int(observer["window_ms"]) / 1000, 0.001)
    senders = rows(path / "observer.sender.csv")
    checks = {
        "measurement_completed": meta.get("measurement_complete", True),
        "all_clients_authenticated_active": all(int(row["players_online"]) == meta["clients"] and int(row["client_join_logs" if csharp else "active_states"]) == meta["clients"] for row in process),
        "sender_records_complete": len(senders) == meta["clients"],
        "all_avatar_senders_connected": all(row["connected_at_end"] == "true" for row in senders),
        "zero_avatar_send_errors": all(int(row["send_errors"]) == 0 for row in senders),
        "all_avatar_senders_progressed": all(int(row["socket_sent_full"]) + int(row["socket_sent_delta"]) > 0 for row in senders),
        "avatar_window_complete": observer.get("window_started") == "true" and int(observer["window_ms"]) == meta["measurement_window_seconds"] * 1000,
        "observer_avatar_peers_complete": int(observer["near_peers"]) == meta["clients"] - 1,
        "client_clean_exit": meta["client_exit_code"] == 0,
        "server_controlled_stop": server_controlled_stop(meta["server_exit_code"], meta.get("os_name", "nt")),
        "zero_non_reliable_drops": all((item["droppedUnreliable"] if csharp else item.get("transport", {}).get("nonReliableDroppedDatagrams", 0)) == 0 for item in health),
    }
    unavailable_checks = []
    if csharp:
        # C# exposes explicit shedding counters, but not these Rust diagnostics.
        # Missing instrumentation must not be reported as a successful zero check.
        checks["zero_reported_voice_drops"] = all(item["droppedVoice"] == 0 for item in health)
        unavailable_checks = ["zero_udp_would_block", "zero_server_protocol_errors"]
        metrics["server_reported_unreliable_drops"] = health[-1]["droppedUnreliable"] - health[0]["droppedUnreliable"]
        metrics["server_reported_voice_drops"] = health[-1]["droppedVoice"] - health[0]["droppedVoice"]
    else:
        checks["zero_udp_would_block"] = all(item["extended"]["rawUdp"]["wouldBlock"] == 0 for item in health)
        checks["zero_server_protocol_errors"] = all(item["extended"]["appMessages"]["protocolErrors"] == 0 for item in health)
    for key in ("missing_expected_peers", "stale_peers_500ms", "decode_errors", "unapplied_deltas", "malformed_items", "non_newer_sequences"):
        checks["zero_avatar_" + key] = int(observer.get(key, 0)) == 0
    voice, audio_validation, capacity, voice_delivery_checks = None, None, {}, {}
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
                       voice_observer_duplicate_packets=sum(int(row["duplicates"]) for row in peers),
                       voice_reordered_or_ambiguous=sum(int(row["reordered_or_ambiguous"]) for row in peers),
                       voice_max_gap_ms=max((float(row["max_gap_ms"]) for row in peers), default=0))
        receipt_metrics, receipt_checks, unique_ratio = analyze_receipt_metrics(voice, voice_rows, meta["clients"])
        metrics.update(receipt_metrics)
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
        voice_delivery_checks = receipt_checks
        checks["all_clients_unique_receipt_records_complete"] = len(voice_rows) == meta["clients"] and all(
            all(key in row for key in ("received_unique_packets", "duplicate_packets", "reordered_packets",
                                       "wrapped_sequence_packets", "ambiguous_sequence_packets", "unique_count_available"))
            for row in voice_rows
        )
        if meta["voice_speaker_percent"] == 100:
            checks["all_clients_sent_voice"] = all(value > 0 for value in sent_counts)
        capacity = voice_capacity_checks(
            metrics["voice_send_cadence_fraction"],
            unique_ratio,
            metrics.get("voice_gap_p95_floor_ms", 10000),
            receipt_checks,
            all(checks.values()),
        )
    return {"name": path.name, "capture": str(path), "valid": all(checks.values()), "checks": checks,
            "unavailable_checks": unavailable_checks,
            "capacity_pass": all(capacity.values()) if capacity else None, "capacity_checks": capacity,
            "voice_delivery_checks": voice_delivery_checks,
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
