#!/usr/bin/env python3
"""Tables, plots and an HTML report for the package experiments (part 2).

    experiments/.venv/bin/python experiments/analyze_packages.py \\
        [--results experiments/results-packages] [--publish experiments/report-packages]

Reads every finished run of `run_suite.py --suite packages` (the
`--workload packages` mode of examples/experiment.rs) and writes, into the
results directory:

    summary.csv      one row per run: its configuration and its metrics
    aggregate.csv    one row per configuration: mean, variance, std over seeds
    plots/*.png      the figures
    report.html      figures, tables and the notes on what they show

What a run logs, and what is taken from it:

    event=publish   one per version publish: success, error, acks per value,
                    duration, and its own op id (>= 10_000_000)
    event=package_stored  per package after publishing: versions that made
                    it, holders of its latest pointer
    event=op kind=install  one per measured install: success; correct (the
                    version asked for, with exactly the bytes published);
                    stale (asked for latest, got an older one); error kind
    event=rpc / event=lookup_end with op=  the RPCs and lookups each publish
                    or install made

The plotting style and the report page come from analyze.py.
"""

import argparse
import csv
import gzip
import json
import math
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import analyze as base  # noqa: E402
from analyze import (INK_2, MUTED, SERIES, Report, band, draw_heatmap, draw_surface, fields,  # noqa: E402
                     html_table, log2_axis, plt, ramp, truthy)

import numpy as np  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
CACHE = "pkg_summary.json"
CACHE_VERSION = 3
PUBLISH_OP_BASE = 10_000_000
K = 10
# Part 1's measured value-lookup success under loss, for the install model.
PART1_AGGREGATE = ROOT / "experiments" / "report" / "aggregate.csv"
TIME_BIN_S = 30

PARAMS = ["nodes", "loss", "latency_ms", "churn", "liveness_s", "republish_s", "duration_s", "versions",
          "packages", "package_bytes", "lossy_publish", "lookups"]

# The error kinds an install can end in, as error_kind() in experiment.rs
# spells them, and how the report names them.
ERRORS = [
    ("InvalidHistory.NoLatestPointer", "no latest pointer found"),
    ("InvalidHistory.MissingRecord", "a version record missing"),
    ("BlobUnavailable", "package blob missing"),
    ("VersionNotFound", "version not in chain"),
    ("other", "other"),
]


# ---------------------------------------------------------------- parsing

def number(value):
    if value in ("true", "false"):
        return 1.0 if value == "true" else 0.0
    try:
        return float(value)
    except ValueError:
        return value


def parse_run(path):
    config = {}
    phase = None
    publishes, installs, stored = [], [], []
    rpcs = Counter()        # op -> RPCs sent (all attempts)
    rpc_calls = Counter()   # op -> RPC calls
    lookups = Counter()     # op -> value lookups (each also runs a node lookup, not counted)
    churn_leaves = 0
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt") as f:
        for line in f:
            if not line.startswith("event="):
                continue
            e = fields(line)
            kind = e.get("event")
            if kind == "run_config":
                config = {k: number(v) for k, v in e.items() if k != "event"}
            elif kind == "phase":
                phase = e["name"]
            elif kind == "publish":
                publishes.append({
                    "op": int(e["op"]), "index": int(e["index"]), "success": truthy(e["success"]),
                    "error": e["error"], "us": int(e["duration_us"]),
                    "acks": [int(e["blob_acks"]), int(e["record_acks"]), int(e["latest_acks"])],
                })
            elif kind == "package_stored":
                stored.append({"versions": int(e["versions"]), "of": int(e["of"]),
                               "holders": int(e["latest_holders"]), "closest": int(e["closest_holders"])})
            elif kind == "op" and e.get("kind") == "install":
                if "skipped" in e:
                    installs.append({"skipped": True})
                    continue
                installs.append({
                    "op": int(e["op"]), "t": int(e["t_ms"]) / 1e3, "chain": int(e["chain"]),
                    "latest": e["requested"] == "latest", "success": truthy(e["success"]),
                    "correct": truthy(e["correct"]), "stale": truthy(e["stale"]), "error": e["error"],
                    "us": int(e["duration_us"]), "holders": int(e["latest_holders"]),
                })
            elif kind == "rpc":
                op = int(e.get("op", 0))
                if op:
                    rpc_calls[op] += 1
                    rpcs[op] += int(e.get("attempts", 1))
            elif kind == "lookup_end" and e.get("kind") == "value":
                op = int(e.get("op", 0))
                if op:
                    lookups[op] += 1
            elif kind == "churn" and e.get("action") == "leave":
                churn_leaves += 1
    if phase != "done":
        return None
    for p in publishes:
        p["rpcs"] = rpcs.get(p["op"], 0)
        p["lookups"] = lookups.get(p["op"], 0)
    for i in installs:
        if not i.get("skipped"):
            i["rpcs"] = rpcs.get(i["op"], 0)
            i["calls"] = rpc_calls.get(i["op"], 0)
            i["lookups"] = lookups.get(i["op"], 0)
    return {"version": CACHE_VERSION, "config": config, "publishes": publishes, "installs": installs,
            "stored": stored, "churn_leaves": churn_leaves}


def load_runs(results):
    runs = []
    for done in sorted(results.glob("*/*/DONE")):
        directory = done.parent
        log = directory / "metrics.log.gz"
        if not log.exists():
            log = directory / "metrics.log"
        if not log.exists():
            continue
        cache = directory / CACHE
        data = None
        if cache.exists() and cache.stat().st_mtime >= log.stat().st_mtime:
            data = json.loads(cache.read_text())
            if data.get("version") != CACHE_VERSION:
                data = None
        if data is None:
            data = parse_run(log)
            if data is None:
                print(f"skipping {directory}: run did not finish")
                continue
            cache.write_text(json.dumps(data))
        data["experiment"] = directory.parent.name
        runs.append(data)
    return runs


# ---------------------------------------------------------------- per-run metrics

def mean(xs):
    return statistics.mean(xs) if xs else float("nan")


def rate(flags):
    flags = list(flags)
    return sum(flags) / len(flags) if flags else float("nan")


def pct(xs, q):
    return float(np.percentile(xs, q)) if xs else float("nan")


def error_bucket(error):
    known = {name for name, _ in ERRORS}
    return error if error in known else "other"


def metrics(run):
    cached = run.get("_metrics")
    if cached is not None:
        return cached
    ins = [i for i in run["installs"] if not i.get("skipped")]
    ok = [i for i in ins if i["success"]]
    pubs = run["publishes"]
    out = {
        "install_success": rate(i["success"] for i in ins),
        "install_correct": rate(i["correct"] for i in ins),
        # Success with the wrong version or bytes: the thing signatures and
        # hashes exist to make impossible.
        "install_wrong": rate(i["success"] and not i["correct"] for i in ins),
        "install_stale": rate(i["stale"] for i in ins if i["latest"]),
        "install_success_latest": rate(i["success"] for i in ins if i["latest"]),
        "install_success_pinned": rate(i["success"] for i in ins if not i["latest"]),
        "install_ms_mean": mean([i["us"] / 1e3 for i in ok]),
        "install_ms_median": pct([i["us"] / 1e3 for i in ok], 50),
        "install_ms_p90": pct([i["us"] / 1e3 for i in ok], 90),
        "install_rpcs": mean([i["rpcs"] for i in ok]),
        "install_lookups": mean([i["lookups"] for i in ok]),
        "install_skipped": rate(i.get("skipped", False) for i in run["installs"]),
        "publish_success": rate(p["success"] for p in pubs),
        "publish_ms_mean": mean([p["us"] / 1e3 for p in pubs if p["success"]]),
        "publish_rpcs": mean([p["rpcs"] for p in pubs if p["success"]]),
        "publish_lookups": mean([p["lookups"] for p in pubs if p["success"]]),
        "blob_acks": mean([p["acks"][0] for p in pubs if p["success"]]),
        "record_acks": mean([p["acks"][1] for p in pubs if p["success"]]),
        "latest_acks": mean([p["acks"][2] for p in pubs if p["success"]]),
        "versions_published": mean([s["versions"] / s["of"] for s in run["stored"]]),
        "latest_holders": mean([s["holders"] for s in run["stored"]]),
        "churned_fraction": run["churn_leaves"] / float(run["config"].get("nodes", 1)),
    }
    for name, _ in ERRORS:
        out[f"err_{name}"] = rate(error_bucket(i["error"]) == name for i in ins if not i["success"]) \
            if any(not i["success"] for i in ins) else 0.0
    run["_metrics"] = out
    return out


def param(run, key):
    value = run["config"].get(key, float("nan"))
    return value if isinstance(value, float) else float("nan")


def group(runs, experiment, keys):
    out = defaultdict(list)
    for r in runs:
        if r["experiment"] == experiment:
            out[tuple(param(r, k) for k in keys)].append(r)
    return dict(sorted(out.items()))


def across_seeds(members, metric):
    vals = [metrics(r)[metric] for r in members]
    vals = [v for v in vals if not math.isnan(v)]
    if not vals:
        return float("nan"), float("nan"), 0
    return statistics.mean(vals), statistics.stdev(vals) if len(vals) > 1 else 0.0, len(vals)


def series(runs, experiment, x_key, metric, where=None):
    where = where or {}
    keys = [x_key] + list(where)
    xs, ms, ss = [], [], []
    for key, members in group(runs, experiment, keys).items():
        if any(key[i + 1] != v for i, v in enumerate(where.values())):
            continue
        m, s, _ = across_seeds(members, metric)
        if not math.isnan(m):
            xs.append(key[0])
            ms.append(m)
            ss.append(s)
    return np.array(xs), np.array(ms), np.array(ss)


def values_of(runs, experiment, key):
    return sorted({param(r, key) for r in runs if r["experiment"] == experiment})


def heat_grid(runs, experiment, xkey, ykey, metric):
    xs = values_of(runs, experiment, xkey)
    ys = values_of(runs, experiment, ykey)
    Z = np.full((len(ys), len(xs)), np.nan)
    for (x, y), members in group(runs, experiment, [xkey, ykey]).items():
        Z[ys.index(y), xs.index(x)] = across_seeds(members, metric)[0]
    return xs, ys, Z


def has(runs, experiment):
    return any(r["experiment"] == experiment for r in runs)


def summary_table(runs, experiment, keys, cols):
    """mean ± std (and variance) over seeds, one row per configuration."""
    head = [k for k in keys] + ["seeds"] + [f"{label}" for _, label, _ in cols]
    rows = []
    for key, members in group(runs, experiment, keys).items():
        row = [f"{v:g}" for v in key] + [str(len(members))]
        for metric, _, fmt in cols:
            m, s, n = across_seeds(members, metric)
            var = s * s
            row.append("–" if math.isnan(m) else f"{fmt.format(m)} ± {fmt.format(s)} (var {var:.3g})")
        rows.append(row)
    return html_table(head, rows)


def pct_text(x):
    return "–" if math.isnan(x) else f"{100 * x:.1f}%"


# ---------------------------------------------------------------- models

def part1_value_success():
    """{loss: mean value-lookup success} from part 1's loss experiment."""
    out = {}
    try:
        with PART1_AGGREGATE.open() as f:
            for row in csv.DictReader(f):
                if row["experiment"] == "loss":
                    out[float(row["loss"])] = float(row["value_success_mean"])
    except (OSError, KeyError, ValueError):
        pass
    return out


# ---------------------------------------------------------------- sections

def sec_scalability(runs, report):
    E = "pkg_scalability"
    if not has(runs, E):
        return
    report.section("Install and publish cost vs network size",
                   "20 packages × 5 versions published on a clean wire, then 500 installs from random nodes "
                   "(half the latest version, half a random published one). An install fetches the latest "
                   "pointer (asking all K closest nodes, so a stale replica cannot hide a newer version), "
                   "walks every version record back to the first, verifying each signature and hash, and "
                   "then fetches the package blob.")
    fig, axes = plt.subplots(1, 3, figsize=(15, 4.2))
    x, m, s = series(runs, E, "nodes", "install_rpcs")
    band(axes[0], x, m, s, SERIES[0], "install")
    x2, m2, s2 = series(runs, E, "nodes", "publish_rpcs")
    band(axes[0], x2, m2, s2, SERIES[1], "publish")
    axes[0].set_title("RPCs per operation")
    axes[0].set_ylabel("RPCs sent")
    x, m, s = series(runs, E, "nodes", "install_ms_median")
    band(axes[1], x, m, s, SERIES[0], "install, median")
    x, m, s = series(runs, E, "nodes", "install_ms_p90")
    band(axes[1], x, m, s, SERIES[0], "install, p90", linestyle="--", marker="s")
    x, m, s = series(runs, E, "nodes", "publish_ms_mean")
    band(axes[1], x, m, s, SERIES[1], "publish, mean")
    axes[1].set_title("Time per operation")
    axes[1].set_ylabel("ms")
    x, m, s = series(runs, E, "nodes", "install_lookups")
    band(axes[2], x, m, s, SERIES[0], "install")
    x, m, s = series(runs, E, "nodes", "publish_lookups")
    band(axes[2], x, m, s, SERIES[1], "publish")
    axes[2].set_title("Value lookups per operation")
    for ax in axes:
        log2_axis(ax, values_of(runs, E, "nodes"))
        ax.set_xlabel("nodes N")
        ax.legend()
    report.save(fig, "pkg_scalability", "Cost of one install and one publish as N doubles (mean ± std over seeds).")
    report.table(summary_table(runs, E, ["nodes"], [
        ("install_success", "install success", "{:.3f}"), ("install_correct", "correct", "{:.3f}"),
        ("install_rpcs", "RPCs/install", "{:.1f}"), ("install_ms_median", "install ms (median)", "{:.0f}"),
        ("publish_rpcs", "RPCs/publish", "{:.1f}"), ("publish_ms_mean", "publish ms", "{:.0f}")]))
    x, m, _ = series(runs, E, "nodes", "install_rpcs")
    if len(x) > 2:
        slope = np.polyfit(np.log2(x), m, 1)[0]
        report.note(f"Each doubling of N adds about {slope:.1f} RPCs to an install: the logarithmic growth "
                    f"of every lookup inside it, times the number of lookups an install makes.")
    x, m, _ = series(runs, E, "nodes", "install_success")
    if len(m):
        report.note(f"Install success on a clean wire: {pct_text(min(m))} at worst over all N.")


def sec_chain(runs, report):
    E = "pkg_chain"
    if not has(runs, E):
        return
    report.section("Cost vs length of the version chain",
                   "10 packages with 1–64 versions each, on 1000 nodes. Installing any version walks the "
                   "chain from the latest pointer back to the first version, one lookup per record, so the "
                   "cost should grow linearly with the number of versions. Publishing version j first loads "
                   "the j − 1 records before it, so publishing a whole history of V versions costs O(V²).")
    fig, axes = plt.subplots(1, 3, figsize=(15, 4.2))
    x, m, s = series(runs, E, "versions", "install_lookups")
    band(axes[0], x, m, s, SERIES[0], "measured")
    if len(x) > 1:
        a, b = np.polyfit(x, m, 1)
        axes[0].plot(x, a * x + b, color=MUTED, linestyle=":", label=f"fit {a:.2f}·V + {b:.1f}")
        report.note(f"An install makes {a:.2f} lookups per version in the chain plus {b:.1f} fixed "
                    f"(latest pointer and blob): linear, as the chain walk predicts.")
    axes[0].set_title("Value lookups per install")
    x, m, s = series(runs, E, "versions", "install_ms_median")
    band(axes[1], x, m, s, SERIES[0], "install, median")
    x, m, s = series(runs, E, "versions", "install_ms_p90")
    band(axes[1], x, m, s, SERIES[0], "install, p90", linestyle="--", marker="s")
    axes[1].set_title("Install time")
    axes[1].set_ylabel("ms")
    x, m, s = series(runs, E, "versions", "install_rpcs")
    band(axes[2], x, m, s, SERIES[0], "install")
    axes[2].set_title("RPCs per install")
    for ax in axes:
        ax.set_xlabel("versions in the chain V")
        ax.legend()
    report.save(fig, "pkg_chain", "Install cost against the number of versions published (mean ± std over seeds).")

    # Publish time by the index of the version being published, from the
    # longest chains: the walk before each publish grows with the index.
    longest = max(values_of(runs, E, "versions"))
    by_index = defaultdict(list)
    for r in runs:
        if r["experiment"] == E and param(r, "versions") == longest:
            for p in r["publishes"]:
                if p["success"]:
                    by_index[p["index"] + 1].append((p["us"] / 1e3, p["lookups"]))
    if by_index:
        idx = np.array(sorted(by_index))
        fig, axes = plt.subplots(1, 2, figsize=(11, 4.2))
        axes[0].plot(idx, [mean([t for t, _ in by_index[i]]) for i in idx], color=SERIES[1], marker=".")
        axes[0].set_title(f"Time to publish version j (V = {longest:g})")
        axes[0].set_ylabel("ms")
        axes[1].plot(idx, [mean([lk for _, lk in by_index[i]]) for i in idx], color=SERIES[1], marker=".")
        axes[1].set_title("Value lookups to publish version j")
        for ax in axes:
            ax.set_xlabel("version index j")
        report.save(fig, "pkg_chain_publish", "Each publish first loads and verifies the whole existing "
                                              "history, so its cost rises with the version being published.")
    report.table(summary_table(runs, E, ["versions"], [
        ("install_success", "install success", "{:.3f}"), ("install_lookups", "lookups/install", "{:.1f}"),
        ("install_rpcs", "RPCs/install", "{:.1f}"), ("install_ms_median", "install ms (median)", "{:.0f}"),
        ("publish_ms_mean", "publish ms", "{:.0f}")]))


def sec_loss(runs, report):
    E = "pkg_loss"
    if not has(runs, E):
        return
    report.section("Install reliability vs packet loss",
                   "Packages published on a clean wire, then installed with each datagram dropped with "
                   "probability p. An install chains V + 2 = 7 lookups (pointer, 5 records, blob) and fails "
                   "if any of them does, so if a single value lookup succeeds with probability s(p), an "
                   "install should succeed with about s(p)^7.")
    s1 = part1_value_success()
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.4))
    x, m, s = series(runs, E, "loss", "install_success")
    band(axes[0], x, m, s, SERIES[0], "install success")
    x, m, s = series(runs, E, "loss", "install_correct")
    band(axes[0], x, m, s, SERIES[2], "correct (right version and bytes)", marker="s", linestyle="--")
    if s1:
        lx = np.array(sorted(s1))
        v = 5
        axes[0].plot(lx, [s1[p] for p in lx], color=MUTED, linestyle=":", label="one value lookup (part 1, N = 5000)")
        axes[0].plot(lx, [s1[p] ** (v + 2) for p in lx], color=INK_2, linestyle="-.",
                     label="model: lookup success^(V+2)")
    axes[0].set_ylim(-0.03, 1.03)
    axes[0].set_title("Install success")
    axes[0].set_xlabel("datagram loss p")
    axes[0].legend(fontsize=8)
    x, m, s = series(runs, E, "loss", "install_ms_median")
    band(axes[1], x, m, s, SERIES[0], "median")
    x, m, s = series(runs, E, "loss", "install_ms_p90")
    band(axes[1], x, m, s, SERIES[0], "p90", linestyle="--", marker="s")
    axes[1].set_yscale("log")
    axes[1].set_title("Install time (successful installs)")
    axes[1].set_ylabel("ms")
    axes[1].set_xlabel("datagram loss p")
    axes[1].legend()
    report.save(fig, "pkg_loss", "Install success and time against loss, with the chained-lookup model.")
    errors_figure(runs, report, E, "loss", "datagram loss p", "pkg_loss_errors")
    report.table(summary_table(runs, E, ["loss"], [
        ("install_success", "success", "{:.3f}"), ("install_correct", "correct", "{:.3f}"),
        ("install_wrong", "wrong", "{:.4f}"), ("install_stale", "stale latest", "{:.4f}"),
        ("install_ms_median", "ms (median)", "{:.0f}"), ("install_rpcs", "RPCs", "{:.0f}")]))
    wrong = [across_seeds(m_, "install_wrong")[0] for _, m_ in group(runs, E, ["loss"]).items()]
    if wrong:
        report.note(f"Installs that returned the wrong version or wrong bytes: {pct_text(max(wrong))} at "
                    "worst. Loss makes installs fail, never succeed with something else.")


def errors_figure(runs, report, E, key, xlabel, name, where=None):
    """Stacked bars: why the failed installs failed, as a share of all installs."""
    xs = [x for x in values_of(runs, E, key)]
    groups = group(runs, E, [key] + list((where or {}).keys()))
    rows = {}
    for k, members in groups.items():
        if where and any(k[i + 1] != v for i, v in enumerate(where.values())):
            continue
        fail = 1 - across_seeds(members, "install_success")[0]
        rows[k[0]] = [fail * across_seeds(members, f"err_{e}")[0] for e, _ in ERRORS]
    if not rows or all(sum(v) == 0 for v in rows.values()):
        return
    xs = [x for x in xs if x in rows]
    fig, ax = plt.subplots(figsize=(8, 4.2))
    bottom = np.zeros(len(xs))
    for j, (_, label) in enumerate(ERRORS):
        h = np.array([rows[x][j] for x in xs])
        h = np.nan_to_num(h)
        ax.bar(range(len(xs)), h, bottom=bottom, color=SERIES[j], label=label)
        bottom += h
    ax.set_xticks(range(len(xs)))
    ax.set_xticklabels([f"{x:g}" for x in xs])
    ax.set_xlabel(xlabel)
    ax.set_ylabel("share of all installs")
    ax.set_title("Why installs failed")
    ax.legend(fontsize=8)
    report.save(fig, name, "Failed installs split by the error they ended in.")


def sec_publish_loss(runs, report):
    E = "pkg_publish_loss"
    if not has(runs, E):
        return
    report.section("Publishing under packet loss",
                   "40 packages × 3 versions published with loss applied during publishing too. A publish "
                   "stores three values (the blob, the signed version record, the signed latest pointer), "
                   "each on the K = 10 closest nodes it finds, and fails if any of them reaches no node. The "
                   "acknowledgement counts show how many replicas each value got.")
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.2))
    x, m, s = series(runs, E, "loss", "publish_success")
    band(axes[0], x, m, s, SERIES[1], "publish success")
    x, m, s = series(runs, E, "loss", "install_success")
    band(axes[0], x, m, s, SERIES[0], "install success afterwards", marker="s")
    axes[0].set_ylim(-0.03, 1.03)
    axes[0].set_title("Publish and install success")
    axes[0].legend()
    for j, (metric, label) in enumerate([("blob_acks", "blob"), ("record_acks", "version record"),
                                         ("latest_acks", "latest pointer")]):
        x, m, s = series(runs, E, "loss", metric)
        band(axes[1], x, m, s, SERIES[j + 2], label)
    axes[1].axhline(K, color=MUTED, linestyle=":", label="K")
    axes[1].set_title("Replicas acknowledged per value (successful publishes)")
    axes[1].legend()
    for ax in axes:
        ax.set_xlabel("datagram loss p")
    report.save(fig, "pkg_publish_loss", "Publish success and replication against loss.")
    report.table(summary_table(runs, E, ["loss"], [
        ("publish_success", "publish success", "{:.3f}"), ("latest_acks", "pointer acks", "{:.1f}"),
        ("blob_acks", "blob acks", "{:.1f}"), ("publish_ms_mean", "publish ms", "{:.0f}"),
        ("install_success", "install success", "{:.3f}")]))


def sec_latency(runs, report):
    E = "pkg_latency"
    if not has(runs, E):
        return
    report.section("Install time vs latency",
                   "One-way latency L on every datagram, with and without 30% loss. An install is about "
                   "seven lookups in a row, each a few round trips, so its time should grow roughly linearly "
                   "in L until a round trip outlasts the RPC retry budget.")
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.2))
    for j, loss in enumerate(values_of(runs, E, "loss")):
        x, m, s = series(runs, E, "latency_ms", "install_ms_median", {"loss": loss})
        band(axes[0], x, m, s, SERIES[j], f"loss {loss:g}")
        x, m, s = series(runs, E, "latency_ms", "install_success", {"loss": loss})
        band(axes[1], x, m, s, SERIES[j], f"loss {loss:g}")
    axes[0].set_title("Install time (median)")
    axes[0].set_ylabel("ms")
    axes[1].set_title("Install success")
    axes[1].set_ylim(-0.03, 1.03)
    for ax in axes:
        ax.set_xlabel("one-way latency L (ms)")
        ax.legend()
    report.save(fig, "pkg_latency", "Install time and success against latency.")
    report.table(summary_table(runs, E, ["latency_ms", "loss"], [
        ("install_success", "success", "{:.3f}"), ("install_ms_median", "ms (median)", "{:.0f}"),
        ("install_ms_p90", "ms (p90)", "{:.0f}")]))


def sec_churn(runs, report):
    E = "pkg_churn"
    if not has(runs, E):
        return
    report.section("Package availability under churn",
                   "1000 nodes, installs spread over 5 minutes while c nodes per second are replaced. A "
                   "package stays installable only while its latest pointer, every version record and the "
                   "blob asked for all survive. Republishing copies each value to the current K closest "
                   "nodes; a latest pointer is republished only together with a chain it could verify.")
    reps = values_of(runs, E, "republish_s")
    colors = ramp(len(reps))
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.2))
    for j, rep in enumerate(reps):
        label = "no republish" if rep >= 3600 else f"republish every {rep:g} s"
        x, m, s = series(runs, E, "churn", "install_success", {"republish_s": rep})
        band(axes[0], x, m, s, colors[j], label)
        x, m, s = series(runs, E, "churn", "install_success_pinned", {"republish_s": rep})
        band(axes[1], x, m, s, colors[j], label)
    axes[0].set_title("Install success, all installs")
    axes[1].set_title("Install success, pinned version")
    for ax in axes:
        ax.set_ylim(-0.03, 1.03)
        ax.set_xlabel("churn c (nodes replaced per second)")
        ax.legend(fontsize=8)
    report.save(fig, "pkg_churn", "Install success against churn, with and without republishing.")

    # Success over time at the highest churn, per republish interval.
    top = max(values_of(runs, E, "churn"))
    fig, ax = plt.subplots(figsize=(8, 4.2))
    for j, rep in enumerate(reps):
        bins = defaultdict(list)
        for r in runs:
            if r["experiment"] == E and param(r, "churn") == top and param(r, "republish_s") == rep:
                for i in r["installs"]:
                    if not i.get("skipped"):
                        bins[int(i["t"] // TIME_BIN_S)].append(i["success"])
        if bins:
            t = sorted(bins)
            label = "no republish" if rep >= 3600 else f"republish every {rep:g} s"
            ax.plot([(b + 0.5) * TIME_BIN_S for b in t], [rate(bins[b]) for b in t], color=colors[j],
                    marker="o", label=label)
    ax.set_ylim(-0.03, 1.03)
    ax.set_xlabel("time into the measure phase (s)")
    ax.set_ylabel("install success")
    ax.set_title(f"Install success over time at c = {top:g} nodes/s")
    ax.legend(fontsize=8)
    report.save(fig, "pkg_churn_time", "How quickly packages become uninstallable once churn starts.")
    for rep in reps:
        errors_figure(runs, report, E, "churn", "churn c (nodes/s)", f"pkg_churn_errors_r{rep:g}",
                      where={"republish_s": rep})
        report.sections[-1]["figures"][-1] = (report.sections[-1]["figures"][-1][0],
                                              f"Why installs failed, republish interval {rep:g} s.")
    report.table(summary_table(runs, E, ["churn", "republish_s"], [
        ("install_success", "success", "{:.3f}"), ("install_success_latest", "latest", "{:.3f}"),
        ("install_success_pinned", "pinned", "{:.3f}"), ("install_wrong", "wrong", "{:.4f}"),
        ("install_stale", "stale", "{:.3f}"), ("churned_fraction", "network churned", "{:.2f}")]))


def sec_size(runs, report):
    E = "pkg_size"
    if not has(runs, E):
        return
    report.section("Install time vs package size",
                   "Blob sizes from 256 B to 256 KiB. STORE and FIND_VALUE travel over the connection "
                   "transport, which carries any size whole, so size should matter little on the simulated "
                   "wire (which models latency per message, not bandwidth).")
    fig, ax = plt.subplots(figsize=(8, 4.2))
    x, m, s = series(runs, E, "package_bytes", "install_ms_median")
    band(ax, x, m, s, SERIES[0], "install, median")
    x, m, s = series(runs, E, "package_bytes", "publish_ms_mean")
    band(ax, x, m, s, SERIES[1], "publish, mean")
    log2_axis(ax, values_of(runs, E, "package_bytes"))
    ax.set_xlabel("package size (bytes)")
    ax.set_ylabel("ms")
    ax.legend()
    report.save(fig, "pkg_size", "Install and publish time against package size.")
    report.table(summary_table(runs, E, ["package_bytes"], [
        ("install_success", "success", "{:.3f}"), ("install_ms_median", "install ms", "{:.0f}"),
        ("publish_ms_mean", "publish ms", "{:.0f}")]))


def sec_heatmaps(runs, report):
    E = "heat_pkg_chain_loss"
    if has(runs, E):
        report.section("Heatmap: chain length × loss",
                       "Install success and time over both at once (1000 nodes). Longer chains mean more "
                       "lookups that each can fail, so success should fall faster with loss the longer the "
                       "chain.")
        xs, ys, Z = heat_grid(runs, E, "loss", "versions", "install_success")
        draw_heatmap(report, xs, ys, Z, "heat_pkg_chain_loss_success", "Install success",
                     "Install success over loss and chain length.", "datagram loss p", "versions V",
                     "success", vmin=0, vmax=1)
        draw_surface(report, xs, ys, Z, "surf_pkg_chain_loss_success", "Install success",
                     "The same as a surface.", "loss p", "versions V", "success", ylog=True, zlim=(0, 1))
        xs, ys, Z = heat_grid(runs, E, "loss", "versions", "install_ms_median")
        draw_heatmap(report, xs, ys, Z, "heat_pkg_chain_loss_time", "Install time, median (ms)",
                     "Median install time over loss and chain length.", "datagram loss p", "versions V", "ms",
                     fmt="{:.0f}", log=True)
    E = "heat_pkg_churn_chain"
    if has(runs, E):
        report.section("Heatmap: churn × chain length",
                       "Install success under churn (republishing every 60 s) for chains of 1–16 versions. "
                       "Every extra version is one more value that has to survive.")
        xs, ys, Z = heat_grid(runs, E, "churn", "versions", "install_success")
        draw_heatmap(report, xs, ys, Z, "heat_pkg_churn_chain_success", "Install success",
                     "Install success over churn and chain length.", "churn c (nodes/s)", "versions V",
                     "success", vmin=0, vmax=1)


def setup_html(runs):
    by_exp = Counter(r["experiment"] for r in runs)
    items = "".join(f"<li><code>{e}</code>: {n} runs</li>" for e, n in sorted(by_exp.items()))
    return f"""
<h2>Setup</h2>
<p>Each run is one process (<code>examples/experiment.rs --workload packages</code>) simulating the
whole network on the in-process fake wire, with the fake DNS resolver and its five test domains as
publishers. Packages are published with <code>registry::publish_package</code> and installed with
<code>registry::install_package</code>, the same functions the CLI's <code>publish</code> and
<code>install</code> commands call. Package <i>i</i> belongs to test domain <i>i</i> mod 5 and is
published from one random node, version 1.0.0, 2.0.0, … in order; each version's blob is random
bytes. Installs run from random nodes; half ask for the latest version, half for a random published
one. An install counts as <b>correct</b> only if it returned the version asked for (for "latest",
the newest that was published) with exactly the bytes published for it.</p>
<p>Each configuration is repeated with several seeds; plots show the mean over seeds with a band of
±1 standard deviation, and tables give mean ± std and the variance.</p>
<ul>{items}</ul>"""


def write_csvs(runs, results):
    rows = []
    for r in runs:
        row = {"experiment": r["experiment"], "seed": r["config"].get("seed")}
        row.update({k: r["config"].get(k) for k in PARAMS})
        row.update(metrics(r))
        rows.append(row)
    if not rows:
        return
    with (results / "summary.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    names = [k for k in rows[0] if k not in PARAMS and k not in ("experiment", "seed")]
    grouped = defaultdict(list)
    for row in rows:
        grouped[(row["experiment"],) + tuple(row[k] for k in PARAMS)].append(row)
    with (results / "aggregate.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["experiment", *PARAMS, "seeds"] + [f"{m}_{s}" for m in names for s in ("mean", "var", "std")])
        for key, members in sorted(grouped.items(), key=lambda kv: tuple(str(x) for x in kv[0])):
            out = list(key) + [len(members)]
            for m in names:
                vals = [x[m] for x in members if not math.isnan(x[m])]
                mu = statistics.mean(vals) if vals else float("nan")
                var = statistics.variance(vals) if len(vals) > 1 else 0.0
                out += [mu, var, math.sqrt(var)]
            w.writerow(out)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--results", type=Path, default=ROOT / "experiments" / "results-packages")
    parser.add_argument("--publish", type=Path, help="also copy report.html, plots/ and the CSVs here")
    args = parser.parse_args()

    runs = load_runs(args.results)
    if not runs:
        raise SystemExit(f"no finished runs under {args.results}")
    print(f"{len(runs)} finished runs")
    write_csvs(runs, args.results)
    report = Report(args.results)
    for section in (sec_scalability, sec_chain, sec_loss, sec_publish_loss, sec_latency, sec_churn, sec_size,
                    sec_heatmaps):
        section(runs, report)
    report.write(setup_html(runs))
    # Which plots exist, for the Typst report: it cannot test for a file, and
    # a figure whose experiment has not run yet should not stop it compiling.
    (report.plots / "index.json").write_text(json.dumps(sorted(p.stem for p in report.plots.glob("*.png"))))
    page = args.results / "report.html"
    page.write_text(page.read_text().replace("Kademlia Experiments", "Package Manager Experiments"))
    figures = sum(len(s["figures"]) for s in report.sections)
    print(f"wrote {figures} plots, {page}, summary.csv, aggregate.csv")
    if args.publish:
        base.publish(args.results, args.publish)
        print(f"published to {args.publish}")


if __name__ == "__main__":
    main()
