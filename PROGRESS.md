# Progress log

Add a new entry at the end of every session (and whenever you stop working).

## Mon 05 Oct (Lesson 9)
- Done:
  - Forked the starter, created the venv, set `UVA_ID` in `.env`; `python kit/smoke_test.py` prints `Emulator OK`.
  - Wrote `populate_queue`, `get_counts` and `monitor_queue` in `prefect-flow.py`.
  - Flow run finished as Completed in fast mode (about a minute): counters went from 21 delayed to 21 visible, total 21 on every poll.
  - Tested the timeout (raises after the deadline) and a scatter API error (raises with the API's message).
  - Committed and pushed.
- Next:
  - Dashboard screenshot of a Completed run in `docs/`.
  - DAG sketch at `docs/dag.jpg` and the polling strategy in the README.
  - `collect_messages` (Checkpoint B).
- Blocked:
  - Nothing blocking. Open item: I work on a remote cluster node, so the dashboard needs port 4200 forwarded before I can take the screenshot. Not tested yet.

## Thu 08 Oct (Lesson 10)
- Done:
  - Added the polling strategy section to the README (what I chose, why, what I rejected).
  - Brought this log up to date.
- Next:
  - Still open from Checkpoint A: dashboard screenshot and `docs/dag.jpg`.
  - Checkpoint B: `collect_messages` (receive, land in `raw.fragments`, then delete; all counters at 0).
  - Checkpoint C: dbt staging model, tests and `assembled_phrase`.
  - Checkpoint D: `submit_solution` and the receipt code.
- Blocked:
  - Nothing.
