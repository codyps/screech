#!/usr/bin/env python3
"""Run the non-vacuity and data-loss negative controls for the Jepsen checker."""
import json
from pathlib import Path
import subprocess

REPO = Path(__file__).resolve().parents[2]
if __name__ == '__main__':
    runtime = json.loads((REPO / 'results/jepsen-runtime/runtime.json').read_text())
    classpath = (REPO / 'results/jepsen-runtime/classpath.txt').read_text().strip()
    expression = """(require 'jepsen.screech.checker-test)
    (let [r (clojure.test/run-tests 'jepsen.screech.checker-test)]
      (shutdown-agents)
      (System/exit (if (zero? (+ (:fail r) (:error r))) 0 1)))"""
    raise SystemExit(subprocess.call([runtime['java'], '-cp', classpath, 'clojure.main', '-e', expression]))
