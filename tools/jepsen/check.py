#!/usr/bin/env python3
"""Run the non-vacuity and data-loss negative controls for the Jepsen checker."""
import argparse
import json
from pathlib import Path
import subprocess

REPO = Path(__file__).resolve().parents[2]
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--recheck', type=Path, help='Recheck saved consumer histories in a completed campaign')
    args = parser.parse_args()
    if not args.recheck:
        subprocess.run(['python3', '-m', 'unittest', 'discover', '-s', 'tests', '-v'],
                       cwd=REPO / 'tools/jepsen', check=True)
    runtime = json.loads((REPO / 'results/jepsen-runtime/runtime.json').read_text())
    classpath = (REPO / 'results/jepsen-runtime/classpath.txt').read_text().strip()
    expression = """(require 'jepsen.screech.checker-test 'jepsen.screech.consumer-test 'jepsen.screech.mixed-test)
    (let [r (clojure.test/run-tests 'jepsen.screech.checker-test 'jepsen.screech.consumer-test 'jepsen.screech.mixed-test)]
      (shutdown-agents)
      (System/exit (if (zero? (+ (:fail r) (:error r))) 0 1)))"""
    command = [runtime['java'], '-cp', classpath, 'clojure.main']
    command += ['-m', 'jepsen.screech.recheck', str(args.recheck.resolve())] if args.recheck else ['-e', expression]
    raise SystemExit(subprocess.call(command))
