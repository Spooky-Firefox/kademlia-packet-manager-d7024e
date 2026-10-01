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
is running stays under --mem-budget-mb (about 0.25 MB per node), so a 16k
node run does not get scheduled next to five others.
"""

import argparse
import gzip
import itertools
import os
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BINARY = ROOT / "target" / "release" / "examples" / "experiment"

# The defaults every experiment starts from; each one overrides some.
BASE = {
    "nodes": 1000,
    "loss": 0.0,
    "latency-ms": 5,
    "alpha": 3,
    "lookups": 1000,
    "values": 200,
    "churn": 0.0,
    "concurrency": 16,
    "liveness-secs": 60,
    "republish-secs": 3600,  # the node's default; never fires inside a run
    "settle-secs": 2,
    "refresh": 0,
}


@dataclass
class Experiment:
    name: str
    description: str
    grid: dict  # flag -> list of values; the runs are the cross product
    seeds: int
    fixed: dict = field(default_factory=dict)

    def runs(self, seed_scale: float):
        seeds = max(1, round(self.seeds * seed_scale))
        keys = list(self.grid)
        for combo in itertools.product(*(self.grid[k] for k in keys)):
            params = {**BASE, **self.fixed, **dict(zip(keys, combo))}
            label = "_".join(f"{k}{v}" for k, v in zip(keys, combo))
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

    def memory_mb(self) -> float:
        nodes = self.params["nodes"] * (1 + self.params["churn"] * 0.02)
        return 80 + 0.25 * nodes

    def seconds(self) -> float:
        """A rough guess, for ordering and for the --dry-run estimate."""
        p = self.params
        build = 0.004 * p["nodes"] + 0.01 * p["nodes"] * p["refresh"] / max(1, p["concurrency"])
        if "duration-secs" in p:
            measure = p["duration-secs"] + 5
        else:
            # A probe that never gets through costs the whole retry budget.
            rtt = 2 * p["latency-ms"] / 1000 + 0.002
            per_probe = rtt + p["loss"] ** 2 * 1.0
            per_lookup = 6 * per_probe * 3 / p["alpha"] + 0.01
            measure = 2 * p["lookups"] * per_lookup / p["concurrency"]
        return 2 + build + p["settle-secs"] + measure

    def command(self, threads: int, out: Path) -> list:
        args = [str(BINARY), "--seed", str(self.seed), "--threads", str(threads), "--out", str(out)]
        for flag, value in self.params.items():
            args += [f"--{flag}", str(value)]
        return args


def experiments(suite: str):
    if suite == "quick":
        return [
            Experiment("scalability", "probes vs N", {"nodes": [16, 64, 256, 1024]}, 2, {"lookups": 100}),
            Experiment("loss", "success vs loss", {"loss": [0.0, 0.5, 0.9]}, 2, {"nodes": 300, "lookups": 100}),
            Experiment("latency", "time vs latency", {"latency-ms": [1, 50, 300], "loss": [0.0, 0.3]}, 1, {"nodes": 300, "lookups": 100}),
            Experiment("alpha", "probes vs alpha", {"alpha": [1, 3, 6], "loss": [0.0, 0.3]}, 1, {"nodes": 300, "lookups": 100}),
            Experiment("churn", "reliability vs churn",
                       {"churn": [0, 2, 10], "liveness-secs": [10], "republish-secs": [3600, 10]}, 1,
                       {"nodes": 300, "lookups": 150, "duration-secs": 30}),
        ]
    return [
        # Mandatory 1: lookup scalability. Doubling N from 16 to 16384.
        Experiment("scalability", "Probes per lookup as a function of N",
                   {"nodes": [16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384]}, 10),
        # The same with every node refreshing its far buckets after joining,
        # which bootstrap does not do yet: how much of the probe count is that.
        Experiment("scalability_refresh", "Probes vs N with bucket refresh after join",
                   {"nodes": [16, 64, 256, 1024, 4096, 8192]}, 5, {"refresh": 4}),
        # Mandatory 2: lookup reliability as a function of packet loss.
        Experiment("loss", "Lookup success rate as a function of packet loss",
                   {"loss": [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95]}, 10),
        Experiment("loss_by_size", "Success vs loss for several network sizes",
                   {"nodes": [100, 1000, 5000], "loss": [0.0, 0.2, 0.4, 0.6, 0.7, 0.8, 0.9]}, 5),
        # Optional: time between request and response vs latency and loss.
        Experiment("latency", "RPC and lookup time vs one-way latency",
                   {"latency-ms": [0, 1, 2, 5, 10, 20, 50, 100, 150, 200, 300, 400, 500, 600],
                    "loss": [0.0, 0.3]}, 5, {"lookups": 500}),
        # Optional: probes and time vs alpha.
        Experiment("alpha", "Probes and lookup time vs alpha",
                   {"alpha": [1, 2, 3, 4, 5, 6, 8, 10], "loss": [0.0, 0.3]}, 8, {"nodes": 2000}),
        # Optional: reliability under churn, paced over ten minutes so the
        # churn has time to act. Two liveness intervals (how fast dead
        # contacts get noticed) and republishing off (the 1 h default, which
        # never fires in the window) or every minute.
        Experiment("churn", "Lookup reliability vs churn rate",
                   {"churn": [0, 0.5, 1, 2, 5, 10, 20], "liveness-secs": [10, 60],
                    "republish-secs": [3600, 60]}, 4,
                   {"lookups": 1500, "duration-secs": 600}),
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
    parser.add_argument("--jobs", type=int, default=6, help="runs at once (default 6)")
    parser.add_argument("--threads", type=int, default=2, help="tokio worker threads per run (default 2)")
    parser.add_argument("--mem-budget-mb", type=float, default=6000)
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
    todo = [r for r in runs if not (r.directory(args.results) / "DONE").exists()]
    todo.sort(key=lambda r: (-r.memory_mb(), -r.seconds()))

    total = sum(r.seconds() for r in todo)
    print(f"{len(runs)} runs in {len(chosen)} experiments, {len(runs) - len(todo)} already done")
    for e in chosen:
        mine = [r for r in todo if r.experiment == e.name]
        print(f"  {e.name:<20} {len(mine):>4} to run  ~{sum(r.seconds() for r in mine) / 3600:5.2f} run-hours")
    print(f"estimated wall time with {args.jobs} jobs: ~{total / args.jobs / 3600:.1f} h (a rough guess)")
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

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
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
