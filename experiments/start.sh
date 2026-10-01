#!/usr/bin/env bash
# Run the whole suite, then the analysis, detached from the terminal: it keeps
# going when the SSH session drops. Arguments go to run_suite.py.
#
#   experiments/start.sh                  # everything
#   experiments/start.sh --only loss      # some of it
#   tmux attach -t kademlia-experiments   # watch it (Ctrl-b d to detach again)
#   tail -f experiments/results/suite.log # or just follow the log
#
# Stopping (tmux kill-session, Ctrl-C inside it) is safe: running the same
# command again skips every run that already finished.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -x experiments/.venv/bin/python ]; then
    python3 -m venv experiments/.venv
    experiments/.venv/bin/pip install -q matplotlib numpy
fi

mkdir -p experiments/results
cmd="python3 experiments/run_suite.py $*; experiments/.venv/bin/python experiments/analyze.py"

if command -v tmux >/dev/null; then
    if tmux has-session -t kademlia-experiments 2>/dev/null; then
        echo "already running: tmux attach -t kademlia-experiments"
        exit 1
    fi
    tmux new-session -d -s kademlia-experiments "$cmd 2>&1 | tee -a experiments/results/console.log"
    echo "started in tmux session kademlia-experiments"
    echo "  watch:  tmux attach -t kademlia-experiments"
else
    nohup bash -c "$cmd" >> experiments/results/console.log 2>&1 &
    echo "started with nohup, pid $!"
fi
echo "  log:    tail -f experiments/results/suite.log"
echo "  report: experiments/results/report.html once it finishes"
