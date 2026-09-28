#!/usr/bin/env python3

import argparse
import csv
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

def parse_line(line: str) -> Dict[str, str]:
    fields = {}
    for token in line.split():
        if "=" in token:
            key, value = token.split("=", 1)
            fields[key] = value
    return fields


def analyze_log(path: Path) -> Tuple[List[int], List[int]]:
    node_probes = []
    value_results = []

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            fields = parse_line(line)

            if fields.get("event") != "lookup_end":
                continue

            kind = fields.get("kind")

            if kind == "node" and "probes" in fields:
                node_probes.append(int(fields["probes"]))

            if kind == "value" and "success" in fields:
                value_results.append(1 if fields["success"] == "true" else 0)

    return node_probes, value_results


def mean(values: List[float]) -> float:
    return statistics.mean(values) if values else 0.0


def variance(values: List[float]) -> float:
    return statistics.variance(values) if len(values) > 1 else 0.0


def print_analysis(node_probes: List[int], value_results: List[int]) -> None:
    print(f"node lookups: {len(node_probes)}")
    print(f"average probes: {mean(node_probes):.3f}")
    print(f"probe variance: {variance(node_probes):.3f}")
    print(f"value lookups: {len(value_results)}")
    print(f"success rate: {mean(value_results):.3f}")


def record_run(args: argparse.Namespace, node_probes: List[int], value_results: List[int]) -> None:
    fields = [
        "network_size",
        "packet_loss",
        "seed",
        "average_probes",
        "probe_variance",
        "success_rate",
    ]

    write_header = not args.results.exists() or args.results.stat().st_size == 0

    with args.results.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        if write_header:
            writer.writeheader()
        writer.writerow(
            {
                "network_size": args.network_size,
                "packet_loss": args.packet_loss,
                "seed": args.seed,
                "average_probes": mean(node_probes),
                "probe_variance": variance(node_probes),
                "success_rate": mean(value_results),
            }
        )

    print(f"saved run to {args.results}")


def summarize(path: Path) -> None:
    rows = []
    with path.open("r", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append(
                {
                    "network_size": int(row["network_size"]),
                    "packet_loss": float(row["packet_loss"]),
                    "average_probes": float(row["average_probes"]),
                    "success_rate": float(row["success_rate"]),
                }
            )

    by_n = defaultdict(list)
    by_loss = defaultdict(list)

    for row in rows:
        by_n[row["network_size"]].append(row["average_probes"])
        by_loss[row["packet_loss"]].append(row["success_rate"])

    print("\nLookup scalability")
    print("N,average_probes,variance")
    for n in sorted(by_n):
        values = by_n[n]
        print(f"{n},{mean(values):.3f},{variance(values):.3f}")

    print("\nLookup reliability")
    print("packet_loss,average_success_rate,variance")
    for loss in sorted(by_loss):
        values = by_loss[loss]
        print(f"{loss:.3f},{mean(values):.3f},{variance(values):.3f}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("log", nargs="?", type=Path)
    parser.add_argument("--network-size", type=int)
    parser.add_argument("--packet-loss", type=float)
    parser.add_argument("--seed", type=int)
    parser.add_argument("--results", type=Path, default=Path("results.csv"))
    parser.add_argument("--summary", type=Path)
    args = parser.parse_args()

    if args.summary:
        summarize(args.summary)
        return

    if args.log is None:
        parser.error("metrics.log is required unless --summary is used")

    node_probes, value_results = analyze_log(args.log)

    metadata = [args.network_size, args.packet_loss, args.seed]
    supplied = sum(value is not None for value in metadata)

    if supplied not in (0, 3):
        parser.error(
            "either omit experiment metadata entirely, or provide "
            "--network-size, --packet-loss and --seed together"
        )

    if supplied == 3:
        print(
            f"N={args.network_size} "
            f"packet_loss={args.packet_loss} "
            f"seed={args.seed}"
        )

    print_analysis(node_probes, value_results)

    if supplied == 3:
        record_run(args, node_probes, value_results)


if __name__ == "__main__":
    main()
