#!/usr/bin/env python3
"""Turn experiment logs into tables, plots and an HTML report.

    experiments/.venv/bin/python experiments/analyze.py [--results DIR]

Reads every finished run under DIR (default experiments/results, as written
by run_suite.py), and writes into DIR:

    summary.csv          one row per run: its configuration and its metrics
    aggregate.csv        one row per configuration: mean, variance, std over seeds
    plots/*.png          the figures
    report.html          the figures and tables in one page

The log format is one event per line, `event=NAME key=value ...`: split on
whitespace, then on the first `=`. Each run's parse is cached next to its log
as summary.json, so re-running after more runs finish only parses the new ones.

Only events from the `measure` phase count toward lookup statistics; the
lookups nodes make while joining (and storing) are excluded.
"""

import argparse
import csv
import gzip
import html
import json
import math
import statistics
from collections import Counter, defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
CACHE_VERSION = 4

# Palette: categorical slots in fixed order, and a one-hue ramp for ordered
# series (a churn rate, a network size).
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
RAMP = ["#86b6ef", "#6da7ec", "#5598e7", "#3987e5", "#2a78d6", "#256abf", "#1c5cab", "#184f95", "#104281", "#0d366b"]
INK = "#0b0b0b"
INK_2 = "#52514e"
MUTED = "#8a8985"
GRID = "#e4e3df"
SURFACE = "#fcfcfb"

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": MUTED, "axes.labelcolor": INK_2, "xtick.color": INK_2, "ytick.color": INK_2,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8, "axes.spines.top": False,
    "axes.spines.right": False, "axes.titlesize": 12, "axes.titleweight": "bold", "axes.titlecolor": INK,
    "axes.titlelocation": "left", "font.size": 10, "legend.frameon": False, "lines.linewidth": 2,
    "lines.markersize": 6, "figure.dpi": 110,
})


def ramp(n):
    """n ordered colours from light to dark, spread over the ramp."""
    if n == 1:
        return [RAMP[6]]
    return [RAMP[round(i * (len(RAMP) - 1) / (n - 1))] for i in range(n)]


# ---------------------------------------------------------------- parsing

def fields(line):
    out = {}
    for token in line.split():
        key, sep, value = token.partition("=")
        if sep:
            out[key] = value
    return out


def truthy(value):
    return value == "true"


def quantiles(values, n=200):
    """A compact stand-in for a long sample: n evenly spaced quantiles."""
    if not values:
        return []
    return [float(q) for q in np.quantile(np.asarray(values, dtype=float), np.linspace(0, 1, n))]


def parse_run(path):
    """Everything the plots need from one metrics log."""
    config = {}
    phase = None
    node_lookups = {}  # lookup_id -> dict, measure-phase node lookups
    value_lookups = []
    ops_value, ops_node = [], []
    rpc = defaultdict(lambda: {"attempts": Counter(), "failed": 0, "durations": []})
    stored = []
    tables = {}
    churn_leaves = 0
    joins = Counter()
    build_probes = []
    background_lookups = 0

    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as f:
        for line in f:
            e = fields(line)
            event = e.get("event")
            if event == "run_config":
                config = e
            elif event == "phase":
                phase = e["name"]
            elif event == "lookup_end":
                # op=0 is background work: joins, stores, republishing.
                measured = phase == "measure" and e.get("op", "0") != "0"
                if not measured and phase == "measure":
                    background_lookups += 1
                if e["kind"] == "node":
                    if phase == "build" and e.get("parent") == "0":
                        build_probes.append(int(e["probes"]))
                    if not measured:
                        continue
                    node_lookups[e["lookup_id"]] = {
                        "parent": e.get("parent", "0"), "probes": int(e["probes"]),
                        "hops": int(e.get("hops", 0)), "exact": truthy(e["exact_match"]),
                        "us": int(e.get("duration_us", 0)),
                    }
                elif measured:
                    value_lookups.append({
                        "id": e["lookup_id"], "probes": int(e["probes"]), "success": truthy(e["success"]),
                        "us": int(e.get("duration_us", 0)),
                    })
            elif event == "op":
                if e["kind"] == "value":
                    ops_value.append({
                        "t": int(e["t_ms"]), "success": truthy(e["success"]), "correct": truthy(e["correct"]),
                        "available": truthy(e["available"]), "holders": int(e["holders"]),
                        "us": int(e["duration_us"]),
                    })
                else:
                    ops_node.append({
                        "t": int(e["t_ms"]), "success": truthy(e["success"]),
                        "recall": int(e["recall"]) / max(1, int(e["of"])), "us": int(e["duration_us"]),
                    })
            elif event == "rpc":
                r = rpc[e["method"]]
                if truthy(e["success"]):
                    r["attempts"][int(e["attempts"])] += 1
                    r["durations"].append(int(e["duration_us"]))
                else:
                    r["failed"] += 1
            elif event == "stored":
                stored.append((int(e["holders"]), int(e["closest_holders"])))
            elif event == "routing_tables":
                tables = {"mean": float(e["mean"]), "min": int(e["min"]), "max": int(e["max"])}
            elif event == "churn":
                churn_leaves += 1
            elif event == "join":
                joins[e["success"]] += 1

    if phase != "done":
        return None

    # Standalone node lookups vs the ones a value lookup started with.
    standalone = [n for n in node_lookups.values() if n["parent"] == "0"]
    children = {n["parent"]: n for n in node_lookups.values() if n["parent"] != "0"}
    value_total = [v["probes"] + children[v["id"]]["probes"] for v in value_lookups if v["id"] in children]

    return {
        "version": CACHE_VERSION,
        "config": config,
        "node": {
            "probes": [n["probes"] for n in standalone],
            "hops": [n["hops"] for n in standalone],
            "us": [n["us"] for n in standalone],
            "exact": [n["exact"] for n in standalone],
        },
        "value": {
            "fv_probes": [v["probes"] for v in value_lookups],
            "total_probes": value_total,
            "us": [v["us"] for v in value_lookups],
        },
        "ops_value": ops_value,
        "ops_node": ops_node,
        "rpc": {
            m: {"attempts": dict(r["attempts"]), "failed": r["failed"], "durations": quantiles(r["durations"])}
            for m, r in rpc.items()
        },
        "stored": stored,
        "tables": tables,
        "churn_leaves": churn_leaves,
        "joins": dict(joins),
        "build_probes_mean": statistics.mean(build_probes) if build_probes else None,
        "background_lookups": background_lookups,
    }


def load_runs(results):
    runs = []
    for done in sorted(results.glob("*/*/DONE")):
        directory = done.parent
        log = directory / "metrics.log.gz"
        if not log.exists():
            log = directory / "metrics.log"
        if not log.exists():
            continue
        cache = directory / "summary.json"
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
        data["dir"] = str(directory)
        runs.append(data)
    return runs


# ---------------------------------------------------------------- per-run metrics

def mean(xs):
    return statistics.mean(xs) if xs else float("nan")


def rate(flags):
    return sum(flags) / len(flags) if flags else float("nan")


def pct(xs, q):
    return float(np.percentile(xs, q)) if xs else float("nan")


def run_metrics(run):
    """Scalars for one run; these are averaged over seeds."""
    rpc_ok = sum(sum(r["attempts"].values()) for r in run["rpc"].values())
    rpc_failed = sum(r["failed"] for r in run["rpc"].values())
    fn = run["rpc"].get("FIND_NODE", {"attempts": {}, "failed": 0, "durations": []})
    fn_ok = sum(fn["attempts"].values())
    attempts = sum(int(a) * c for a, c in fn["attempts"].items()) + 5 * fn["failed"]
    ov, on = run["ops_value"], run["ops_node"]
    available = [o for o in ov if o["available"]]
    nodes = int(run["config"]["nodes"])
    return {
        "node_probes": mean(run["node"]["probes"]),
        "node_hops": mean(run["node"]["hops"]),
        "node_ms_median": pct(run["node"]["us"], 50) / 1e3,
        "node_ms_p90": pct(run["node"]["us"], 90) / 1e3,
        "value_total_probes": mean(run["value"]["total_probes"]),
        "value_fv_probes": mean(run["value"]["fv_probes"]),
        "value_ms_median": pct(run["value"]["us"], 50) / 1e3,
        "value_ms_p90": pct(run["value"]["us"], 90) / 1e3,
        "value_success": rate([o["success"] for o in ov]),
        "value_correct": rate([o["correct"] for o in ov]),
        "value_available": rate([o["available"] for o in ov]),
        "value_success_given_available": rate([o["success"] for o in available]),
        "node_success": rate([o["success"] for o in on]),
        "node_recall": mean([o["recall"] for o in on]),
        "rpc_fail_rate": rpc_failed / (rpc_ok + rpc_failed) if rpc_ok + rpc_failed else float("nan"),
        "find_node_fail_rate": fn["failed"] / (fn_ok + fn["failed"]) if fn_ok + fn["failed"] else float("nan"),
        "find_node_attempts": attempts / (fn_ok + fn["failed"]) if fn_ok + fn["failed"] else float("nan"),
        "find_node_rtt_ms": (fn["durations"][len(fn["durations"]) // 2] / 1e3) if fn["durations"] else float("nan"),
        "table_mean": run["tables"].get("mean", float("nan")),
        "replicas_in_k_closest": mean([c for _, c in run["stored"]]) / 10 if run["stored"] else float("nan"),
        "holders_mean": mean([h for h, _ in run["stored"]]),
        "churned_fraction": run["churn_leaves"] / nodes,
        "background_lookups_per_s": run.get("background_lookups", 0) / max(1.0, param(run, "duration_s")),
    }


PARAMS = ["nodes", "loss", "latency_ms", "alpha", "churn", "liveness_s", "republish_s", "refresh", "duration_s",
          "lookups"]


def param(run, key):
    return float(run["config"].get(key, "nan"))


def group(runs, experiment, keys):
    """{tuple of key values: [runs]} for one experiment."""
    out = defaultdict(list)
    for r in runs:
        if r["experiment"] == experiment:
            out[tuple(param(r, k) for k in keys)].append(r)
    return dict(sorted(out.items()))


def across_seeds(group_runs, metric):
    values = [run_metrics(r)[metric] for r in group_runs]
    values = [v for v in values if not math.isnan(v)]
    if not values:
        return float("nan"), float("nan"), 0
    sd = statistics.stdev(values) if len(values) > 1 else 0.0
    return statistics.mean(values), sd, len(values)


def series(runs, experiment, x_key, metric, where=None):
    """x, mean, std over seeds of `metric`, for runs matching `where`."""
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


def pooled(group_runs, getter):
    out = []
    for r in group_runs:
        out.extend(getter(r))
    return out


# ---------------------------------------------------------------- plotting

class Report:
    def __init__(self, results):
        self.results = results
        self.plots = results / "plots"
        self.plots.mkdir(exist_ok=True)
        self.sections = []  # (title, intro, [(png, caption)], [table html])

    def section(self, title, intro):
        self.sections.append({"title": title, "intro": intro, "figures": [], "tables": []})

    def save(self, fig, name, caption):
        fig.tight_layout()
        path = self.plots / f"{name}.png"
        fig.savefig(path, dpi=130)
        plt.close(fig)
        self.sections[-1]["figures"].append((f"plots/{name}.png", caption))

    def table(self, html_table):
        self.sections[-1]["tables"].append(html_table)

    def write(self, setup_html):
        parts = [f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Kademlia Experiments</title><style>
:root {{ --surface:#fcfcfb; --ink:#0b0b0b; --ink2:#52514e; --rule:#e4e3df; --accent:#2a78d6; }}
@media (prefers-color-scheme: dark) {{ :root:not([data-theme="light"]) {{ --surface:#1a1a19; --ink:#fff; --ink2:#c3c2b7; --rule:#383835; --accent:#3987e5; }} }}
:root[data-theme="dark"] {{ --surface:#1a1a19; --ink:#fff; --ink2:#c3c2b7; --rule:#383835; --accent:#3987e5; }}
body {{ background:var(--surface); color:var(--ink); font:15px/1.55 system-ui,sans-serif; margin:0 auto; max-width:1180px; padding:24px 16px 80px; }}
h1 {{ font-size:28px; margin:0 0 4px; }} h2 {{ margin-top:48px; border-top:1px solid var(--rule); padding-top:20px; }}
p, li {{ color:var(--ink2); max-width:80ch; }}
nav a {{ margin-right:14px; color:var(--accent); }}
.grid {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(min(100%,520px),1fr)); gap:18px; }}
figure {{ margin:0; background:#fcfcfb; border:1px solid var(--rule); border-radius:8px; padding:8px; }}
figure img {{ width:100%; height:auto; display:block; }}
figcaption {{ font-size:13px; color:#52514e; padding:6px 4px 2px; }}
table {{ border-collapse:collapse; font-size:13px; margin:14px 0; display:block; overflow-x:auto; }}
th, td {{ border-bottom:1px solid var(--rule); padding:4px 10px; text-align:right; white-space:nowrap; }}
th {{ color:var(--ink2); font-weight:600; }}
code {{ font-size:13px; }}
</style></head><body>
<h1>Kademlia Experiments</h1>
<nav>{' '.join(f'<a href="#s{i}">{html.escape(s["title"])}</a>' for i, s in enumerate(self.sections))}</nav>
{setup_html}"""]
        for i, s in enumerate(self.sections):
            parts.append(f'<h2 id="s{i}">{html.escape(s["title"])}</h2><p>{s["intro"]}</p><div class="grid">')
            for src, caption in s["figures"]:
                parts.append(f'<figure><img src="{src}" alt="{html.escape(caption)}" loading="lazy">'
                             f'<figcaption>{html.escape(caption)}</figcaption></figure>')
            parts.append("</div>")
            parts.extend(s["tables"])
        parts.append("</body></html>")
        (self.results / "report.html").write_text("\n".join(parts))


def band(ax, x, m, s, color, label, marker="o", linestyle="-"):
    ax.plot(x, m, color=color, marker=marker, linestyle=linestyle, label=label, zorder=3)
    ax.fill_between(x, m - s, m + s, color=color, alpha=0.15, linewidth=0, zorder=2)


def log2_axis(ax, xs):
    ax.set_xscale("log", base=2)
    ax.set_xticks(xs)
    ax.set_xticklabels([f"{int(x)}" for x in xs], rotation=45)
    ax.minorticks_off()


def summary_table(runs, experiment, keys, metrics):
    """HTML: one row per configuration, mean / variance / std / seeds per metric."""
    rows = []
    for key, members in group(runs, experiment, keys).items():
        cells = [f"<td>{k:g}</td>" for k in key]
        for metric in metrics:
            m, s, n = across_seeds(members, metric)
            cells.append(f"<td>{m:.4g}</td><td>{s * s:.3g}</td><td>{s:.3g}</td>")
        cells.append(f"<td>{len(members)}</td>")
        rows.append("<tr>" + "".join(cells) + "</tr>")
    head = "".join(f"<th>{k}</th>" for k in keys)
    head += "".join(f"<th>{m} mean</th><th>var</th><th>std</th>" for m in metrics) + "<th>seeds</th>"
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(rows)}</tbody></table>"


# ---------------------------------------------------------------- expectations

K = 10
RESEND_S = 0.2
ATTEMPTS = 5


def rpc_success(p, attempts=ATTEMPTS):
    """Probability a datagram RPC gets an answer: some attempt's request and
    reply both survive loss p, independently per datagram."""
    return 1 - (1 - (1 - p) ** 2) ** attempts


def expected_table_size(n):
    """Bucket i (shared prefix i) covers 2^-(i+1) of the other nodes and holds
    at most K, so a full table holds sum_i min(K, n / 2^(i+1))."""
    return sum(min(K, (n - 1) / 2 ** (i + 1)) for i in range(64))


# ---------------------------------------------------------------- sections

def scalability(runs, report):
    if not any(r["experiment"] == "scalability" for r in runs):
        return
    report.section(
        "Lookup scalability",
        "Probes (FIND_NODE RPCs sent) per node lookup as N doubles. A lookup that "
        "queries until the K closest it knows have all answered sends at least "
        "min(N-1, K) probes, so the curve has a floor at K; past that, Kademlia "
        "predicts growth of O(log N), at most about log<sub>2</sub>N hops, each "
        "hop costing up to &alpha; probes. Bands are &plusmn;1 std over seeds.")
    x, m, s = series(runs, "scalability", "nodes", "node_probes")
    xr, mr, sr = series(runs, "scalability_refresh", "nodes", "node_probes")

    fig, ax = plt.subplots(figsize=(7, 4.4))
    band(ax, x, m, s, SERIES[0], "measured")
    if len(xr):
        band(ax, xr, mr, sr, SERIES[1], "measured, with bucket refresh", marker="s")
    if len(x) > 2:
        lx = np.log2(x)
        b, a = np.polyfit(lx, m, 1)
        ax.plot(x, a + b * lx, color=MUTED, linestyle="--", linewidth=1.5,
                label=f"fit {a:.1f} + {b:.2f}·log₂N")
    ax.plot(x, np.minimum(x - 1, K), color=INK_2, linestyle=":", linewidth=1.5, label="floor min(N−1, K)")
    log2_axis(ax, x)
    ax.set_xlabel("network size N")
    ax.set_ylabel("probes per node lookup")
    ax.set_title("Probes per lookup grow logarithmically in N")
    ax.legend()
    report.save(fig, "scal_probes", "Mean FIND_NODE probes per standalone node lookup vs N, ±1 std over seeds.")

    # Hops against log2 N.
    x, m, s = series(runs, "scalability", "nodes", "node_hops")
    xr, mr, sr = series(runs, "scalability_refresh", "nodes", "node_hops")
    fig, ax = plt.subplots(figsize=(7, 4.4))
    band(ax, x, m, s, SERIES[0], "measured hops")
    if len(xr):
        band(ax, xr, mr, sr, SERIES[1], "with bucket refresh", marker="s")
    ax.plot(x, np.log2(x), color=INK_2, linestyle=":", label="log₂N (paper bound)")
    ax.plot(x, np.log2(x) / np.log2(K), color=MUTED, linestyle="--", label="log₂N / log₂K")
    log2_axis(ax, x)
    ax.set_xlabel("network size N")
    ax.set_ylabel("hops to the closest node found")
    ax.set_title("Hop count stays well under log₂N")
    ax.legend()
    report.save(fig, "scal_hops", "Hops: the length of the chain of replies that led to the closest result. "
                "0 means the origin's routing table already held it.")

    groups = group(runs, "scalability", ["nodes"])
    ns = [k[0] for k in groups]

    # Probe distribution.
    fig, ax = plt.subplots(figsize=(7, 4.4))
    data = [pooled(g, lambda r: r["node"]["probes"]) for g in groups.values()]
    bp = ax.boxplot(data, positions=range(len(ns)), widths=0.55, showfliers=False, patch_artist=True)
    for patch in bp["boxes"]:
        patch.set(facecolor=RAMP[2], edgecolor=RAMP[7], alpha=0.8)
    for part in ("whiskers", "caps", "medians"):
        for line in bp[part]:
            line.set(color=RAMP[8], linewidth=1.3)
    ax.set_xticks(range(len(ns)))
    ax.set_xticklabels([f"{int(n)}" for n in ns], rotation=45)
    ax.set_xlabel("network size N")
    ax.set_ylabel("probes per lookup")
    ax.set_title("Probe count distribution")
    report.save(fig, "scal_probe_dist", "Probes per node lookup, all seeds pooled. Box: quartiles; whiskers: 1.5 IQR.")

    # Hop distribution as stacked shares.
    fig, ax = plt.subplots(figsize=(7, 4.4))
    hop_lists = [pooled(g, lambda r: r["node"]["hops"]) for g in groups.values()]
    max_hop = max((max(h) for h in hop_lists if h), default=0)
    colors = ramp(max_hop + 1)
    bottom = np.zeros(len(ns))
    for h in range(max_hop + 1):
        share = np.array([sum(1 for v in hl if v == h) / max(1, len(hl)) for hl in hop_lists])
        ax.bar(range(len(ns)), share, bottom=bottom, color=colors[h], label=f"{h} hops",
               edgecolor=SURFACE, linewidth=1)
        bottom += share
    ax.set_xticks(range(len(ns)))
    ax.set_xticklabels([f"{int(n)}" for n in ns], rotation=45)
    ax.set_xlabel("network size N")
    ax.set_ylabel("share of lookups")
    ax.set_title("Hop count distribution")
    ax.legend(ncol=2, fontsize=8, loc="upper left", bbox_to_anchor=(1, 1))
    report.save(fig, "scal_hop_dist", "Share of node lookups by hop count, per N.")

    # Value lookups: node phase + find_value phase.
    x, m, s = series(runs, "scalability", "nodes", "value_total_probes")
    x2, m2, s2 = series(runs, "scalability", "nodes", "value_fv_probes")
    if len(x):
        fig, ax = plt.subplots(figsize=(7, 4.4))
        band(ax, x, m, s, SERIES[0], "total (node lookup + FIND_VALUE)")
        band(ax, x2, m2, s2, SERIES[1], "FIND_VALUE probes only", marker="s")
        log2_axis(ax, x)
        ax.set_xlabel("network size N")
        ax.set_ylabel("probes per value lookup")
        ax.set_title("Value lookups: where the probes go")
        ax.legend()
        report.save(fig, "scal_value_probes", "A value lookup first runs a node lookup for the key, then asks the "
                    "K closest for the value, α at a time, stopping at the first that has it.")

    # Durations.
    fig, ax = plt.subplots(figsize=(7, 4.4))
    for i, (metric, label) in enumerate([("node_ms_median", "node lookup, median"),
                                         ("node_ms_p90", "node lookup, p90"),
                                         ("value_ms_median", "value lookup, median")]):
        x, m, s = series(runs, "scalability", "nodes", metric)
        band(ax, x, m, s, SERIES[i], label, marker="os^"[i])
    log2_axis(ax, x)
    ax.set_xlabel("network size N")
    ax.set_ylabel("ms")
    ax.set_title("Lookup time vs N (5 ms one-way latency)")
    ax.legend()
    report.save(fig, "scal_time", "Wall-clock lookup time. Each round costs one 10 ms round trip, so time "
                "tracks rounds (≈ probes / α), not probes.")

    # Accuracy.
    fig, ax = plt.subplots(figsize=(7, 4.4))
    for i, (metric, label) in enumerate([("node_success", "target found (exact match)"),
                                         ("node_recall", "recall of the true K closest"),
                                         ("value_success", "value lookup success")]):
        x, m, s = series(runs, "scalability", "nodes", metric)
        band(ax, x, m, s, SERIES[i], label, marker="os^"[i])
    log2_axis(ax, x)
    ax.set_ylim(min(0.9, ax.get_ylim()[0]), 1.005)
    ax.set_xlabel("network size N")
    ax.set_ylabel("fraction")
    ax.set_title("Lookups stay accurate as N grows")
    ax.legend()
    report.save(fig, "scal_accuracy", "Recall: the fraction of the K live nodes truly closest to the target "
                "(computed from global knowledge) that the lookup returned.")

    # Routing tables.
    x, m, s = series(runs, "scalability", "nodes", "table_mean")
    xr, mr, sr = series(runs, "scalability_refresh", "nodes", "table_mean")
    fig, ax = plt.subplots(figsize=(7, 4.4))
    band(ax, x, m, s, SERIES[0], "measured")
    if len(xr):
        band(ax, xr, mr, sr, SERIES[1], "with bucket refresh", marker="s")
    ax.plot(x, [expected_table_size(n) for n in x], color=INK_2, linestyle=":",
            label="full table Σ min(K, N/2ⁱ⁺¹)")
    log2_axis(ax, x)
    ax.set_xlabel("network size N")
    ax.set_ylabel("contacts per routing table")
    ax.set_title("Routing table size vs N")
    ax.legend()
    report.save(fig, "scal_tables", "Mean routing-table size after the network is built, against the size of "
                "a table with every bucket as full as the network allows.")

    # Replica placement.
    x, m, s = series(runs, "scalability", "nodes", "replicas_in_k_closest")
    if len(x):
        fig, ax = plt.subplots(figsize=(7, 4.4))
        band(ax, x, m, s, SERIES[0], "measured")
        ax.axhline(1, color=INK_2, linestyle=":", label="ideal")
        log2_axis(ax, x)
        ax.set_ylim(0, 1.05)
        ax.set_xlabel("network size N")
        ax.set_ylabel("fraction of the true K closest holding it")
        ax.set_title("Where STORE put the replicas")
        ax.legend()
        report.save(fig, "scal_replicas", "After storing: how many of the K nodes truly closest to each key hold "
                    "the value, i.e. how accurate the store's own node lookup was.")

    report.table(summary_table(runs, "scalability", ["nodes"],
                               ["node_probes", "node_hops", "node_recall", "value_success"]))
    if any(r["experiment"] == "scalability_refresh" for r in runs):
        report.table(summary_table(runs, "scalability_refresh", ["nodes"], ["node_probes", "node_hops"]))


def loss(runs, report):
    if not any(r["experiment"] == "loss" for r in runs):
        return
    report.section(
        "Lookup reliability vs packet loss",
        "The network is built and the values stored on a lossless wire; then every "
        "datagram (request or reply) is dropped independently with probability p while "
        "the lookups run. An RPC is sent up to 5 times, 200 ms apart, and counts as "
        "answered if any reply arrives before the last wait ends, so a single RPC "
        "succeeds with probability 1 − (1 − (1−p)²)<sup>5</sup>. FIND_VALUE and STORE "
        "go over the connection transport, which loss does not touch, so a value "
        "lookup can fail only in its node-lookup phase.")
    p_grid = np.linspace(0, 0.99, 100)

    fig, ax = plt.subplots(figsize=(7, 4.4))
    for i, (metric, label) in enumerate([("value_success", "value lookup success"),
                                         ("node_success", "node lookup finds target"),
                                         ("node_recall", "node lookup recall")]):
        x, m, s = series(runs, "loss", "loss", metric)
        band(ax, x, m, s, SERIES[i], label, marker="os^"[i])
    ax.plot(p_grid, rpc_success(p_grid), color=INK_2, linestyle=":", label="single RPC, model")
    ax.plot(p_grid, (1 - p_grid) ** 2, color=MUTED, linestyle="--", linewidth=1.2, label="single attempt (1−p)²")
    ax.set_xlabel("packet loss probability p")
    ax.set_ylabel("success rate")
    ax.set_ylim(-0.02, 1.02)
    ax.set_title("Lookups outlast single RPCs under loss")
    ax.legend(loc="lower left")
    report.save(fig, "loss_success", "Success rates vs loss, ±1 std over seeds, against the success of one RPC.")

    # Observed vs modelled RPC failure.
    x, m, s = series(runs, "loss", "loss", "find_node_fail_rate")
    fig, ax = plt.subplots(figsize=(7, 4.4))
    ax.errorbar(x, m, yerr=s, color=SERIES[0], marker="o", linestyle="none", label="measured FIND_NODE timeouts",
                capsize=3, zorder=3)
    ax.plot(p_grid, 1 - rpc_success(p_grid), color=INK_2, linestyle=":", label="model (1 − (1−p)²)⁵")
    ax.set_yscale("log")
    ax.set_ylim(1e-5, 1.2)
    ax.set_xlabel("packet loss probability p")
    ax.set_ylabel("fraction of RPCs that time out")
    ax.set_title("RPC timeouts match the retry model")
    ax.legend()
    report.save(fig, "loss_rpc_fail", "Share of FIND_NODE RPCs that exhausted all 5 attempts. Zero rates are "
                "not drawn on the log scale.")

    # Attempts distribution.
    groups = group(runs, "loss", ["loss"])
    ps = [k[0] for k in groups]
    fig, ax = plt.subplots(figsize=(7, 4.4))
    colors = ramp(ATTEMPTS)
    bottom = np.zeros(len(ps))
    for a in range(1, ATTEMPTS + 1):
        share = []
        for g in groups.values():
            ok = Counter()
            failed = 0
            for r in g:
                fn = r["rpc"].get("FIND_NODE", {"attempts": {}, "failed": 0})
                ok.update({int(k): v for k, v in fn["attempts"].items()})
                failed += fn["failed"]
            share.append(ok[a] / max(1, sum(ok.values()) + failed))
        share = np.array(share)
        ax.bar(range(len(ps)), share, bottom=bottom, color=colors[a - 1], label=f"answered on try {a}",
               edgecolor=SURFACE, linewidth=1)
        bottom += share
    ax.bar(range(len(ps)), 1 - bottom, bottom=bottom, color=SERIES[7], label="timed out", edgecolor=SURFACE,
           linewidth=1)
    ax.set_xticks(range(len(ps)))
    ax.set_xticklabels([f"{p:g}" for p in ps])
    ax.set_xlabel("packet loss probability p")
    ax.set_ylabel("share of FIND_NODE RPCs")
    ax.set_title("How many sends an RPC needed")
    ax.legend(fontsize=8, loc="upper left", bbox_to_anchor=(1, 1))
    report.save(fig, "loss_attempts", "Attempt on which each FIND_NODE got its answer. A reply to an earlier "
                "send that arrives late is counted on the attempt in progress.")

    # Probes and time.
    fig, ax = plt.subplots(figsize=(7, 4.4))
    for i, (metric, label) in enumerate([("node_probes", "node lookup"), ("value_total_probes", "value lookup")]):
        x, m, s = series(runs, "loss", "loss", metric)
        band(ax, x, m, s, SERIES[i], label, marker="os"[i])
    ax.set_xlabel("packet loss probability p")
    ax.set_ylabel("probes per lookup")
    ax.set_title("Probes per lookup vs loss")
    ax.legend()
    report.save(fig, "loss_probes", "A probe that times out still counts. Loss also hides contacts, which "
                "can make lookups stop early with fewer probes.")

    fig, ax = plt.subplots(figsize=(7, 4.4))
    for i, (metric, label) in enumerate([("node_ms_median", "node lookup, median"),
                                         ("node_ms_p90", "node lookup, p90"),
                                         ("value_ms_median", "value lookup, median")]):
        x, m, s = series(runs, "loss", "loss", metric)
        band(ax, x, m, s, SERIES[i], label, marker="os^"[i])
    ax.set_yscale("log")
    ax.set_xlabel("packet loss probability p")
    ax.set_ylabel("ms (log scale)")
    ax.set_title("Loss costs time before it costs success")
    ax.legend()
    report.save(fig, "loss_time", "Lookup time vs loss. Every lost datagram costs a 200 ms resend wait, "
                "so time climbs steeply long before lookups start failing.")

    # CDF of node lookup time.
    fig, ax = plt.subplots(figsize=(7, 4.4))
    chosen = [p for p in ps if p in (0.0, 0.2, 0.4, 0.6, 0.8, 0.9)] or ps
    colors = ramp(len(chosen))
    for c, p in zip(colors, chosen):
        us = np.sort(pooled(groups[(p,)], lambda r: r["node"]["us"])) / 1e3
        if len(us):
            ax.plot(us, np.linspace(0, 1, len(us)), color=c, label=f"p = {p:g}")
    ax.set_xscale("log")
    ax.set_xlabel("node lookup time, ms (log scale)")
    ax.set_ylabel("cumulative share")
    ax.set_title("Lookup time distribution by loss")
    ax.legend()
    report.save(fig, "loss_time_cdf", "CDF of node lookup times, all seeds pooled. Steps at multiples of "
                "200 ms are resends.")

    report.table(summary_table(runs, "loss", ["loss"],
                               ["value_success", "node_success", "find_node_fail_rate", "node_probes"]))

    # Several sizes.
    if any(r["experiment"] == "loss_by_size" for r in runs):
        sizes = values_of(runs, "loss_by_size", "nodes")
        colors = ramp(len(sizes))
        for metric, title, name in [("value_success", "Value lookup success vs loss, by N", "loss_size_value"),
                                    ("node_success", "Node lookup success vs loss, by N", "loss_size_node"),
                                    ("node_probes", "Probes vs loss, by N", "loss_size_probes")]:
            fig, ax = plt.subplots(figsize=(7, 4.4))
            for c, n in zip(colors, sizes):
                x, m, s = series(runs, "loss_by_size", "loss", metric, {"nodes": n})
                band(ax, x, m, s, c, f"N = {int(n)}")
            if metric != "node_probes":
                ax.plot(p_grid, rpc_success(p_grid), color=INK_2, linestyle=":", label="single RPC, model")
            ax.set_xlabel("packet loss probability p")
            ax.set_ylabel(metric.replace("_", " "))
            ax.set_title(title)
            ax.legend()
            report.save(fig, name, f"{title}, ±1 std over seeds.")
        report.table(summary_table(runs, "loss_by_size", ["nodes", "loss"], ["value_success", "node_success"]))


def latency(runs, report):
    if not any(r["experiment"] == "latency" for r in runs):
        return
    report.section(
        "Time between request and response vs latency",
        "Every datagram is delayed by a fixed one-way latency L, so a reply takes 2L. "
        "The transport resends after 200 ms of silence and gives up after 5 sends "
        "(1 s total). Expect RTT ≈ 2L while 2L &lt; 200 ms; past that each request is "
        "resent before its reply can arrive (wasted traffic, same answer), and once "
        "2L passes 1 s no reply can arrive in time and every datagram RPC fails.")
    losses = values_of(runs, "latency", "loss")
    L = np.array(values_of(runs, "latency", "latency_ms"))

    fig, ax = plt.subplots(figsize=(7, 4.4))
    for i, p in enumerate(losses):
        x, m, s = series(runs, "latency", "latency_ms", "find_node_rtt_ms", {"loss": p})
        band(ax, x, m, s, SERIES[i], f"median FIND_NODE RTT, loss {p:g}")
    ax.plot(L, 2 * L, color=INK_2, linestyle=":", label="2L")
    ax.axvline(500, color=SERIES[7], linestyle="--", linewidth=1.2, label="2L = retry budget (1 s)")
    ax.axvline(100, color=MUTED, linestyle="--", linewidth=1.2, label="2L = resend interval")
    ax.set_xlabel("one-way latency L, ms")
    ax.set_ylabel("ms")
    ax.set_title("Round-trip time follows 2L until the budget")
    ax.legend(fontsize=8)
    report.save(fig, "lat_rtt", "Median time from first send to reply, successful FIND_NODEs only.")

    fig, ax = plt.subplots(figsize=(7, 4.4))
    for i, p in enumerate(losses):
        x, m, s = series(runs, "latency", "latency_ms", "find_node_attempts", {"loss": p})
        band(ax, x, m, s, SERIES[i], f"loss {p:g}")
    ax.axvline(100, color=MUTED, linestyle="--", linewidth=1.2, label="2L = resend interval")
    ax.set_xlabel("one-way latency L, ms")
    ax.set_ylabel("sends per FIND_NODE")
    ax.set_title("Resends start once 2L passes 200 ms")
    ax.legend()
    report.save(fig, "lat_attempts", "Mean sends per FIND_NODE (5 for one that timed out).")

    fig, ax = plt.subplots(figsize=(7, 4.4))
    for i, p in enumerate(losses):
        x, m, s = series(runs, "latency", "latency_ms", "node_ms_median", {"loss": p})
        band(ax, x, m, s, SERIES[i], f"node lookup median, loss {p:g}")
    ax.set_xlabel("one-way latency L, ms")
    ax.set_ylabel("ms")
    ax.set_title("Lookup time vs latency")
    ax.legend()
    report.save(fig, "lat_lookup_time", "Median node lookup time. Roughly rounds × 2L, plus 200 ms per resend.")

    fig, ax = plt.subplots(figsize=(7, 4.4))
    for i, p in enumerate(losses):
        x, m, s = series(runs, "latency", "latency_ms", "value_success", {"loss": p})
        band(ax, x, m, s, SERIES[i], f"value lookup, loss {p:g}")
        x, m, s = series(runs, "latency", "latency_ms", "node_success", {"loss": p})
        band(ax, x, m, s, SERIES[i], f"node lookup, loss {p:g}", marker="s", linestyle="--")
    ax.axvline(500, color=SERIES[7], linestyle="--", linewidth=1.2, label="2L = retry budget")
    ax.set_xlabel("one-way latency L, ms")
    ax.set_ylabel("success rate")
    ax.set_ylim(-0.02, 1.02)
    ax.set_title("Lookups fail once replies can't beat the timeout")
    ax.legend(fontsize=8)
    report.save(fig, "lat_success", "Success vs latency.")
    report.table(summary_table(runs, "latency", ["loss", "latency_ms"],
                               ["find_node_rtt_ms", "find_node_attempts", "node_ms_median", "value_success"]))


def alpha(runs, report):
    if not any(r["experiment"] == "alpha" for r in runs):
        return
    report.section(
        "Probes and lookup time vs α",
        "α is how many FIND_NODEs a lookup keeps in flight. Expect: more α means more "
        "probes, since the lookup speculatively queries nodes a sequential lookup would "
        "have skipped once something closer turned up, but fewer rounds, so lower "
        "latency, with diminishing returns once α approaches K (the lookup can never "
        "have more than K useful candidates). Under loss, a larger α also hides "
        "timeouts: other queries make progress while one waits out its resends.")
    losses = values_of(runs, "alpha", "loss")
    for metric, ylabel, title, name in [
        ("node_probes", "probes per node lookup", "More parallelism, more probes", "alpha_probes"),
        ("node_ms_median", "median node lookup time, ms", "…and less waiting", "alpha_time"),
        ("node_hops", "hops", "Hop count vs α", "alpha_hops"),
        ("value_success", "value lookup success", "Success vs α", "alpha_success"),
    ]:
        fig, ax = plt.subplots(figsize=(7, 4.4))
        for i, p in enumerate(losses):
            x, m, s = series(runs, "alpha", "alpha", metric, {"loss": p})
            band(ax, x, m, s, SERIES[i], f"loss {p:g}", marker="os"[i % 2])
        ax.set_xlabel("α")
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        ax.legend()
        report.save(fig, name, f"{ylabel} vs α, ±1 std over seeds.")
    report.table(summary_table(runs, "alpha", ["loss", "alpha"], ["node_probes", "node_ms_median", "value_success"]))


def churn(runs, report):
    if not any(r["experiment"] == "churn" for r in runs):
        return
    report.section(
        "Lookup reliability vs churn",
        "During a paced measurement window, c random nodes per second leave (stop "
        "answering, without notice) and as many new ones join. Values are stored "
        "once before churn. Without republishing, a value disappears once every "
        "node holding it has left: a node survives t seconds with probability about "
        "e<sup>−ct/N</sup>, so a value with h holders is still available with "
        "probability 1 − (1 − e<sup>−ct/N</sup>)<sup>h</sup>. With republishing "
        "every T seconds, each surviving holder copies the value back onto the "
        "current K closest, so a value is lost only if all K holders leave within "
        "one interval, (1 − e<sup>−cT/N</sup>)<sup>K</sup> per interval. Lookups "
        "can also fail while the value exists, when routing tables still point at "
        "departed nodes; the liveness interval decides how long those linger.")
    rates = values_of(runs, "churn", "churn")
    variants = [k[1:] for k in group(runs, "churn", ["churn", "liveness_s", "republish_s"])]
    variants = sorted(set(variants))

    def label(live, republish):
        window = max(param(r, "duration_s") for r in runs if r["experiment"] == "churn")
        rep_text = "no republish" if republish >= window else f"republish {republish:g} s"
        return f"liveness {live:g} s, {rep_text}"

    for metric, ylabel, title, name in [
        ("value_success", "value lookup success", "Value lookups vs churn", "churn_value"),
        ("value_available", "value still held by a live node", "Value availability vs churn",
         "churn_available"),
        ("value_success_given_available", "success when the value still exists",
         "Routing reliability vs churn", "churn_routing"),
        ("node_success", "node lookup finds target", "Node lookups vs churn", "churn_node"),
        ("node_recall", "recall of true K closest", "Node lookup recall vs churn", "churn_recall"),
        ("node_ms_median", "median node lookup time, ms", "Lookup time vs churn", "churn_time"),
        ("node_probes", "probes per node lookup", "Probes vs churn", "churn_probes"),
    ]:
        fig, ax = plt.subplots(figsize=(7, 4.4))
        for i, (live, republish) in enumerate(variants):
            x, m, s = series(runs, "churn", "churn", metric, {"liveness_s": live, "republish_s": republish})
            band(ax, x, m, s, SERIES[i], label(live, republish), marker="os^D"[i % 4])
        ax.set_xlabel("churn, nodes replaced per second")
        ax.set_ylabel(ylabel)
        if metric not in ("node_ms_median", "node_probes"):
            ax.set_ylim(-0.02, 1.02)
        ax.set_title(title)
        ax.legend(fontsize=8)
        report.save(fig, name, f"{ylabel} vs churn rate over the whole window, ±1 std over seeds.")

    # Over time, against the availability model, one panel per variant.
    groups = group(runs, "churn", ["churn", "liveness_s", "republish_s"])
    colors = ramp(len(rates))
    for kind, title in [("available", "Availability over time"), ("success", "Value lookup success over time")]:
        fig, axes = plt.subplots(1, len(variants), figsize=(5 * len(variants), 4.2), sharey=True, squeeze=False)
        for ax, (live, republish) in zip(axes[0], variants):
            for c, rate_ in zip(colors, rates):
                members = groups.get((rate_, live, republish), [])
                if not members:
                    continue
                duration = param(members[0], "duration_s") or 1
                n = param(members[0], "nodes")
                bins = np.linspace(0, duration * 1000, 11)
                ops = pooled(members, lambda r: r["ops_value"])
                t = np.array([o["t"] for o in ops])
                y = np.array([o[kind] for o in ops], dtype=float)
                centers, means = [], []
                for lo, hi in zip(bins[:-1], bins[1:]):
                    sel = (t >= lo) & (t < hi)
                    if sel.any():
                        centers.append((lo + hi) / 2000)
                        means.append(y[sel].mean())
                ax.plot(centers, means, color=c, marker="o", markersize=4, label=f"c = {rate_:g}/s")
                if kind == "available" and rate_ > 0:
                    ts = np.linspace(0, duration, 100)
                    if republish >= duration:
                        h = mean(pooled(members, lambda r: [hh for hh, _ in r["stored"]])) or K
                        model = 1 - (1 - np.exp(-rate_ * ts / n)) ** h
                    else:
                        per_interval = (1 - np.exp(-rate_ * republish / n)) ** K
                        model = (1 - per_interval) ** (ts / republish)
                    ax.plot(ts, model, color=c, linestyle=":", linewidth=1.3)
            ax.set_title(label(live, republish), fontsize=10)
            ax.set_xlabel("seconds into the churn window")
            ax.set_ylim(-0.02, 1.02)
        axes[0][0].set_ylabel(kind)
        axes[0][-1].legend(fontsize=8, ncol=2)
        fig.suptitle(title, x=0.01, ha="left", fontweight="bold", color=INK)
        report.save(fig, f"churn_{kind}_time", f"{title}, binned into tenths of the window, seeds pooled."
                    + (" Dotted: the availability model for each rate." if kind == "available" else ""))

    report.table(summary_table(runs, "churn", ["republish_s", "liveness_s", "churn"],
                               ["value_success", "value_available", "value_success_given_available",
                                "node_success", "background_lookups_per_s"]))


def setup_html(runs):
    configs = [r["config"] for r in runs]
    c = configs[0] if configs else {}
    by_exp = Counter(r["experiment"] for r in runs)
    items = "".join(f"<li><code>{html.escape(e)}</code>: {n} runs</li>" for e, n in sorted(by_exp.items()))
    return f"""
<h2>Setup</h2>
<p>Every run is one process (<code>examples/experiment.rs</code>) simulating the whole network on an
in-process fake wire that carries the same code paths as the real UDP/TCP transports. Node
addresses are drawn at random from 10.0.0.0/8 with random ports using the run's seed; node ids are
SHA-256 of <code>ip:port</code>, so each seed gives a different topology. Values are 64 random bytes
from the same seed. Lookups are counted only in the <em>measure</em> phase, after the network is
built and the values stored.</p>
<p>RPC policy: datagram RPCs (PING, FIND_NODE) are resent every {c.get('resend_ms', '?')} ms of
silence, at most {c.get('max_attempts', '?')} sends, so a call gives up after
{int(c.get('resend_ms', 0) or 0) * int(c.get('max_attempts', 0) or 0)} ms. STORE and FIND_VALUE use
the connection transport with a {c.get('stream_deadline_ms', '?')} ms deadline; loss does not
apply to it. K = {c.get('k', '?')}; α = 3 unless varied. A failed call removes the contact from the
routing table (unless the table is down to K contacts), and every node pings its whole table
on a jittered liveness interval.</p>
<p>Each configuration is repeated with several seeds; plots show the mean over seeds and a band of
±1 standard deviation of the per-seed means; tables give mean, variance and std.</p>
<ul>{items}</ul>"""


def write_csvs(runs, results):
    rows = []
    for r in runs:
        row = {"experiment": r["experiment"], "seed": r["config"].get("seed")}
        row.update({k: r["config"].get(k) for k in PARAMS})
        row.update(run_metrics(r))
        rows.append(row)
    if not rows:
        return
    with (results / "summary.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)

    metrics = [k for k in rows[0] if k not in PARAMS and k not in ("experiment", "seed")]
    grouped = defaultdict(list)
    for row in rows:
        grouped[(row["experiment"],) + tuple(row[k] for k in PARAMS)].append(row)
    with (results / "aggregate.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["experiment", *PARAMS, "seeds"] +
                   [f"{m}_{s}" for m in metrics for s in ("mean", "var", "std")])
        for key, members in sorted(grouped.items(), key=lambda kv: tuple(str(x) for x in kv[0])):
            out = list(key) + [len(members)]
            for m in metrics:
                vals = [x[m] for x in members if not math.isnan(x[m])]
                mu = statistics.mean(vals) if vals else float("nan")
                var = statistics.variance(vals) if len(vals) > 1 else 0.0
                out += [mu, var, math.sqrt(var)]
            w.writerow(out)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--results", type=Path, default=ROOT / "experiments" / "results")
    args = parser.parse_args()

    runs = load_runs(args.results)
    if not runs:
        raise SystemExit(f"no finished runs under {args.results}")
    print(f"{len(runs)} finished runs")

    write_csvs(runs, args.results)
    report = Report(args.results)
    for section in (scalability, loss, latency, alpha, churn):
        section(runs, report)
    report.write(setup_html(runs))
    figures = sum(len(s["figures"]) for s in report.sections)
    print(f"wrote {figures} plots, {args.results / 'report.html'}, summary.csv, aggregate.csv")


if __name__ == "__main__":
    main()
