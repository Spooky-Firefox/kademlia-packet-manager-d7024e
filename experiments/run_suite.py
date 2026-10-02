#!/usr/bin/env python3
"""Run the experiment suite: many `experiment` runs, several at a time.

Each run is one configuration with one seed, in its own directory:

    experiments/results/<experiment>/<params>_seed<S>/
        metrics.log.gz   structured events (see examples/experiment.rs)
        run.out          the run's stdout/stderr
        DONE             written last; a directory without it is re-run

so the suite can be stopped at any point (Ctrl-C, a dropped SSH session) and
started again with the same command: finished runs are skipped.

    python3 experiments/run_suite.py --dry-run          # what would run, how long
    python3 experiments/run_suite.py                    # everything
    python3 experiments/run_suite.py --only loss,alpha  # some experiments
    python3 experiments/run_suite.py --suite quick      # a few minutes, for a smoke test

Runs are started largest first and only while the estimated memory of what
is running stays under --mem-budget-mb (about 0.26 MB per node), so a 16k
node run does not get scheduled next to five 5000-node ones.
"""

import argparse
import gzip
import itertools
import json
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BINARY = ROOT / "target" / "release" / "examples" / "experiment"

# Settings every experiment shares unless it says otherwise. The network size
# is deliberately NOT here: every experiment below states its own `nodes`, so
# the size behind each graph can be read straight off its definition.
BASE = {
    "loss": 0.0,             # datagram loss during the measure phase
    "latency-ms": 5,         # one-way latency during the measure phase
    "alpha": 3,
    "lookups": 1000,         # value lookups, and as many node lookups
    "values": 200,
    "churn": 0.0,            # nodes replaced per second while measuring
    "concurrency": 16,
    "liveness-secs": 60,     # the node's default liveness interval
    "republish-secs": 3600,  # the node's default; never fires inside a run
    "settle-secs": 2,
    "refresh": 0,
    "rpc-events": "ops",     # log the RPCs measured lookups make, not background
}

# The size most experiments run at. About 1.2 GB of memory per run.
DEFAULT_NODES = 5000

# Network sizes for the heatmaps that put N on an axis: doubling from 250.
HEAT_SIZES = [250, 500, 1000, 2000, 4000, 8000, 16000]


@dataclass
class Experiment:
    name: str
    description: str
    grid: dict  # flag -> list of values; the runs are the cross product
    seeds: int
    fixed: dict = field(default_factory=dict)

    def __post_init__(self):
        if "nodes" not in self.grid and "nodes" not in self.fixed:
            raise ValueError(f"experiment {self.name} must say how many nodes it runs with")

    def runs(self, seed_scale: float):
        seeds = max(1, round(self.seeds * seed_scale))
        keys = list(self.grid)
        for combo in itertools.product(*(self.grid[k] for k in keys)):
            params = {**BASE, **self.fixed, **dict(zip(keys, combo))}
            label = "_".join(f"{k}{v}" for k, v in zip(keys, combo))
            if "nodes" not in self.grid:
                label = f"nodes{params['nodes']}_{label}"
            for seed in range(1, seeds + 1):
                yield Run(self.name, label, seed, params)


@dataclass
class Run:
    experiment: str
    label: str
    seed: int
    params: dict

    def directory(self, results: Path) -> Path:
        return results / self.experiment / f"{self.label}_seed{self.seed}"

    def is_done(self, results: Path) -> bool:
        """Finished, and with exactly these settings. The directory name only
        spells out the settings an experiment varies, so a change to a fixed
        one (the network size, say) must not pass for a finished run."""
        directory = self.directory(results)
        try:
            return (directory / "DONE").exists() and json.loads((directory / "params.json").read_text()) == self.params
        except (OSError, ValueError):
            return False

    def memory_mb(self) -> float:
        # Measured: ~0.23 MB per node, a little more under churn.
        return 150 + 0.26 * self.params["nodes"] * (1.15 if self.params["churn"] else 1)

    def seconds(self) -> float:
        """A rough guess, for ordering and for the --dry-run estimate."""
        p = self.params
        build = 0.003 * p["nodes"] + 0.01 * p["nodes"] * p["refresh"] / max(1, p["concurrency"])
        if "duration-secs" in p:
            measure = p["duration-secs"] + 10
        else:
            # Each probe waits out a resend for every send that is lost.
            q = (1 - p["loss"]) ** 2
            sends = (1 - (1 - q) ** 5) / q if q > 0 else 5
            rtt = 2 * p["latency-ms"] / 1000
            per_probe = min(1.0, rtt + 0.2 * (sends - 1)) + 0.002
            per_lookup = (15 / p["alpha"] + 1) * per_probe
            measure = 2 * p["lookups"] * per_lookup / p["concurrency"]
        return 5 + build + p["settle-secs"] + measure

    def command(self, threads: int, out: Path) -> list:
        args = [str(BINARY), "--seed", str(self.seed), "--threads", str(threads), "--out", str(out)]
        for flag, value in self.params.items():
            args += [f"--{flag}", str(value)]
        return args


def experiments(suite: str):
    if suite == "quick":
        # A few minutes in all: every code path, tiny networks.
        return [
            Experiment("scalability", "probes vs N", {"nodes": [16, 64, 256, 1024]}, 2, {"lookups": 100}),
            Experiment("loss", "success vs loss", {"loss": [0.0, 0.5, 0.9]}, 2, {"nodes": 300, "lookups": 100}),
            Experiment("latency", "time vs latency", {"latency-ms": [1, 50, 600], "loss": [0.0, 0.3]}, 1,
                       {"nodes": 300, "lookups": 100}),
            Experiment("alpha", "probes vs alpha", {"alpha": [1, 3, 6], "loss": [0.0, 0.3]}, 1,
                       {"nodes": 300, "lookups": 100}),
            Experiment("churn", "reliability vs churn",
                       {"churn": [0, 2, 10], "liveness-secs": [10], "republish-secs": [3600, 10]}, 1,
                       {"nodes": 300, "lookups": 150, "duration-secs": 30}),
            Experiment("heat_loss_size", "loss x N", {"nodes": [100, 300], "loss": [0.0, 0.5, 0.8]}, 1,
                       {"lookups": 50}),
            Experiment("heat_churn_republish", "churn x republish",
                       {"churn": [0, 5], "republish-secs": [10, 3600]}, 1,
                       {"nodes": 300, "lookups": 100, "duration-secs": 30, "liveness-secs": 10}),
            Experiment("heat_alpha_size", "alpha x N", {"nodes": [100, 300], "alpha": [1, 3]}, 1, {"lookups": 50}),
            Experiment("heat_latency_loss", "latency x loss", {"latency-ms": [5, 600], "loss": [0.0, 0.5]}, 1,
                       {"nodes": 200, "lookups": 50}),
            Experiment("heat_churn_liveness", "churn x liveness",
                       {"churn": [0, 5], "liveness-secs": [5, 20]}, 1,
                       {"nodes": 300, "lookups": 100, "duration-secs": 30, "republish-secs": 10}),
        ]
    return [
        # ---- Mandatory 1: lookup scalability ------------------------------
        # N doubles from 16 to 16384 nodes (16384 takes ~4 GB on its own).
        Experiment("scalability", "Probes per lookup as a function of N",
                   {"nodes": [16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384]}, 10),
        # The same sizes with every node refreshing its far buckets after
        # joining, which bootstrap does not do yet.
        Experiment("scalability_refresh", "Probes vs N with bucket refresh after join",
                   {"nodes": [16, 64, 256, 1024, 4096, 8192, 16384]}, 5, {"refresh": 4}),

        # ---- Mandatory 2: lookup reliability vs packet loss ---------------
        # N = 5000 nodes.
        Experiment("loss", "Lookup success rate as a function of packet loss",
                   {"loss": [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95]}, 10,
                   {"nodes": DEFAULT_NODES}),

        # ---- Optional: time between request and response ------------------
        # N = 5000 nodes; one-way latency 0..600 ms (2L passes the 1 s retry
        # budget at 500 ms), with and without 30% loss.
        Experiment("latency", "RPC and lookup time vs one-way latency",
                   {"latency-ms": [0, 1, 2, 5, 10, 20, 50, 100, 150, 200, 300, 400, 500, 600],
                    "loss": [0.0, 0.3]}, 5, {"nodes": DEFAULT_NODES, "lookups": 500}),

        # ---- Optional: probes and time vs alpha ---------------------------
        # N = 5000 nodes.
        Experiment("alpha", "Probes and lookup time vs alpha",
                   {"alpha": [1, 2, 3, 4, 5, 6, 8, 10], "loss": [0.0, 0.3]}, 8, {"nodes": DEFAULT_NODES}),

        # ---- Optional: reliability vs churn --------------------------------
        # N = 5000 nodes, paced over 10 minutes. The rates are 0.05%..2% of
        # the network per second, so a full turnover (N/c) takes from 2000 s
        # (c = 2.5) down to 50 s (c = 100). Two liveness intervals, and
        # republishing off (the 1 h default never fires in 10 minutes) or
        # every minute.
        Experiment("churn", "Lookup reliability vs churn rate",
                   {"churn": [0, 2.5, 5, 10, 25, 50, 100], "liveness-secs": [10, 60],
                    "republish-secs": [3600, 60]}, 4,
                   {"nodes": DEFAULT_NODES, "lookups": 1500, "duration-secs": 600}),

        # ---- Heatmaps: two parameters at once -----------------------------
        # N = 250..16000 nodes x loss 0..0.9.
        Experiment("heat_loss_size", "Success and probes over N and loss",
                   {"nodes": HEAT_SIZES, "loss": [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]}, 3,
                   {"lookups": 500}),
        # N = 250..16000 nodes x alpha 1..10.
        Experiment("heat_alpha_size", "Probes and time over N and alpha",
                   {"nodes": HEAT_SIZES, "alpha": [1, 2, 3, 4, 5, 6, 8, 10]}, 3, {"lookups": 500}),
        # N = 2000 nodes; latency 0..600 ms x loss 0..0.8.
        Experiment("heat_latency_loss", "Time and success over latency and loss",
                   {"latency-ms": [0, 10, 25, 50, 100, 150, 200, 300, 400, 500, 600],
                    "loss": [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]}, 3,
                   {"nodes": 2000, "lookups": 300}),
        # N = 2000 nodes, 5 minutes each; churn 0..40 nodes/s (full turnover in
        # 50 s at 40/s) x republish interval 15 s..1 h, liveness every 10 s.
        Experiment("heat_churn_republish", "Reliability over churn and republish interval",
                   {"churn": [0, 1, 2, 5, 10, 20, 40], "republish-secs": [15, 30, 60, 120, 300, 3600]}, 3,
                   {"nodes": 2000, "lookups": 750, "duration-secs": 300, "liveness-secs": 10}),
        # N = 2000 nodes, 5 minutes each; churn 0..40 nodes/s x liveness
        # interval 5..80 s, republish every 60 s.
        Experiment("heat_churn_liveness", "Reliability over churn and liveness interval",
                   {"churn": [0, 1, 2, 5, 10, 20, 40], "liveness-secs": [5, 10, 20, 40, 80]}, 3,
                   {"nodes": 2000, "lookups": 750, "duration-secs": 300, "republish-secs": 60}),
    ]


def log(message: str, logfile: Path):
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {message}"
    print(line, flush=True)
    with logfile.open("a") as f:
        f.write(line + "\n")


def build():
    # A stale binary must not stand in for a failed build: delete it, so one
    # that exists afterwards can only have come from this build. (Not its
    # mtime: cargo re-links an up-to-date binary with its old timestamp.)
    if BINARY.exists():
        BINARY.unlink()
    subprocess.run(["cargo", "build", "--release", "--example", "experiment"], cwd=ROOT, check=True)
    if not BINARY.exists():
        sys.exit(f"build did not produce {BINARY}")


def finish(run: Run, directory: Path, code: int, took: float):
    metrics = directory / "metrics.log"
    if code == 0 and metrics.exists():
        with metrics.open("rb") as src, gzip.open(directory / "metrics.log.gz", "wb", 6) as dst:
            shutil.copyfileobj(src, dst)
        metrics.unlink()
        sim = directory / "sim.log"
        if sim.exists() and sim.stat().st_size == 0:
            sim.unlink()
        (directory / "DONE").write_text(f"seconds={took:.1f}\n")
        return True
    return False


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--suite", choices=["full", "quick"], default="full")
    parser.add_argument("--only", help="comma-separated experiment names")
    parser.add_argument("--jobs", type=int, default=8, help="runs at once, memory allowing (default 8)")
    parser.add_argument("--threads", type=int, default=2, help="tokio worker threads per run (default 2)")
    parser.add_argument("--mem-budget-mb", type=float, default=7000,
                        help="estimated memory all running runs may use together (default 7000)")
    parser.add_argument("--seed-scale", type=float, default=1.0, help="multiply every experiment's seed count")
    parser.add_argument("--results", type=Path, default=ROOT / "experiments" / "results")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-build", action="store_true")
    args = parser.parse_args()

    chosen = experiments(args.suite)
    if args.only:
        wanted = set(args.only.split(","))
        unknown = wanted - {e.name for e in chosen}
        if unknown:
            sys.exit(f"unknown experiments: {', '.join(sorted(unknown))}")
        chosen = [e for e in chosen if e.name in wanted]

    runs = [r for e in chosen for r in e.runs(args.seed_scale)]
    todo = [r for r in runs if not r.is_done(args.results)]
    todo.sort(key=lambda r: (-r.memory_mb(), -r.seconds()))

    total = sum(r.seconds() for r in todo)
    print(f"{len(runs)} runs in {len(chosen)} experiments, {len(runs) - len(todo)} already done")
    for e in chosen:
        mine = [r for r in todo if r.experiment == e.name]
        print(f"  {e.name:<20} {len(mine):>4} to run  ~{sum(r.seconds() for r in mine) / 3600:5.2f} run-hours")
    # Bounded by whichever runs out first: job slots, or memory.
    by_memory = sum(r.seconds() * r.memory_mb() for r in todo) / args.mem_budget_mb
    wall = max(total / args.jobs, by_memory)
    print(f"estimated wall time with {args.jobs} jobs and {args.mem_budget_mb:.0f} MB: ~{wall / 3600:.1f} h "
          "(a rough guess)")
    if args.dry_run or not todo:
        return

    if not args.no_build:
        build()

    args.results.mkdir(parents=True, exist_ok=True)
    logfile = args.results / "suite.log"
    log(f"starting {len(todo)} runs, {args.jobs} at a time", logfile)

    running = {}  # Popen -> (run, directory, started, memory)
    failed = 0
    done = 0
    stopping = False
    signalled = False

    def stop(_signum, _frame):
        nonlocal stopping
        stopping = True

    # SIGHUP too: closing the tmux session must stop the runs, which sit in
    # sessions of their own and would not get the hangup themselves.
    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(sig, stop)
    started_all = time.time()

    while (todo and not stopping) or running:
        # Start what fits.
        while todo and not stopping and len(running) < args.jobs:
            used = sum(m for (_, _, _, m) in running.values())
            fits = [r for r in todo if not running or used + r.memory_mb() <= args.mem_budget_mb]
            if not fits:
                break
            run = fits[0]
            todo.remove(run)
            directory = run.directory(args.results)
            if directory.exists():
                shutil.rmtree(directory)  # a partial run from before
            directory.mkdir(parents=True)
            (directory / "params.json").write_text(json.dumps(run.params, indent=1))
            out = (directory / "run.out").open("w")
            proc = subprocess.Popen(run.command(args.threads, directory), stdout=out, stderr=subprocess.STDOUT,
                                    start_new_session=True)
            out.close()
            running[proc] = (run, directory, time.time(), run.memory_mb())

        time.sleep(0.5)
        for proc in [p for p in running if p.poll() is not None]:
            run, directory, started, _ = running.pop(proc)
            took = time.time() - started
            if finish(run, directory, proc.returncode, took):
                done += 1
                left = len(todo) + len(running)
                rate = (time.time() - started_all) / done
                log(f"done {run.experiment}/{directory.name} in {took:.0f}s "
                    f"({done} done, {left} left, ~{left * rate / 3600:.1f} h to go)", logfile)
            else:
                failed += 1
                log(f"FAILED {run.experiment}/{directory.name} exit={proc.returncode}, see {directory}/run.out",
                    logfile)

        if stopping and running and not signalled:
            signalled = True
            for proc in running:
                proc.send_signal(signal.SIGTERM)

    log(f"finished: {done} done, {failed} failed{', stopped early' if stopping else ''}", logfile)
    print("next: experiments/.venv/bin/python experiments/analyze.py")


if __name__ == "__main__":
    main()
