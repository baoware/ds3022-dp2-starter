"""DS 3022 Data Project 2: Prefect flow.

Your pipeline must, without human intervention:
  1. POST to the scatter API to populate your queue (once per run, never inside a retry loop)
  2. Monitor the queue (get_queue_attributes) and collect all 21 messages
  3. Land each fragment in DuckDB (raw.fragments) BEFORE deleting its message
  4. Run dbt: tests are the quality gate, the mart model holds the phrase
  5. Submit the phrase to the 'dp2-submit' queue and log the HTTP status code

Run:   python prefect-flow.py
Then:  python kit/check_submission.py

Every task already has a header. Checkpoint A (Lesson 9): fill in populate_queue,
get_counts and monitor_queue. Lesson 10: collect_messages, dbt_build, read_phrase
and submit_solution. The tasks not called by the flow yet cannot fail, so you can
run the file at any point. The skeleton is a suggestion: rename, split or add
tasks as your design needs, and keep your DAG sketch in step with them.
Use Prefect's logger (get_run_logger) for logging. Never hard-code your
computing ID or the endpoint: read them from the environment (.env).
"""
import os
import time

import boto3
import duckdb
import requests
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger

# load the .env file into the environment
load_dotenv()

UVA_ID = os.environ["UVA_ID"] # computing id from .env
# scatter api link for this computing id
SCATTER_URL = f"http://127.0.0.1:{os.environ.get('SCATTER_PORT', '8000')}/api/scatter/{UVA_ID}"
DUCKDB_PATH = os.environ.get("DP2_DUCKDB", "dp2.duckdb") # duckdb file shared with dbt

sqs = boto3.client("sqs")  # the endpoint comes from AWS_ENDPOINT_URL_SQS

# short name and sqs attribute for the three counters
# together the three add up to every message in the queue
COUNTERS = {
    "visible": "ApproximateNumberOfMessages",
    "not_visible": "ApproximateNumberOfMessagesNotVisible",
    "delayed": "ApproximateNumberOfMessagesDelayed",
}


# function to start each run with an empty fragments table
@task
def reset_fragments() -> None:
    """Create raw.fragments if it is missing and empty it for the new run."""
    logger = get_run_logger()
    # every POST starts a fresh puzzle so rows from an older run could hide a missing fragment
    con = duckdb.connect(DUCKDB_PATH)
    try:
        # order_no stays a string here since dbt casts it later
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        con.execute("CREATE TABLE IF NOT EXISTS raw.fragments (order_no VARCHAR, word VARCHAR, received_at TIMESTAMP)")
        # empty the table
        con.execute("DELETE FROM raw.fragments")
    finally:
        # always close so dbt can open the file later
        con.close()
    logger.info("raw.fragments is ready and empty in %s", DUCKDB_PATH)


# function to fill the queue with the messages
@task
def populate_queue() -> str:
    """POST to the scatter API and return your queue URL."""
    logger = get_run_logger()
    # no retries on this task since every POST empties the queue and sends all the messages again
    # send the POST once
    resp = requests.post(SCATTER_URL, timeout=30)
    # if the POST failed then stop here with the api's own message
    if not resp.ok:
        raise RuntimeError(f"Scatter API returned HTTP {resp.status_code}: {resp.text}")
    # the response json holds the queue url so grab it
    payload = resp.json()
    queue_url = payload["sqs_url"]
    logger.info("Scattered %s messages (%s). Queue URL: %s", payload["messages"], payload["mode"], queue_url)
    return queue_url


# function to grab the three counters for a queue
def get_counts(queue_url: str) -> dict:
    """Plain helper (not a task): return the three ApproximateNumberOf... counters as integers."""
    # ask sqs for only the three counter attributes
    attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=list(COUNTERS.values()))["Attributes"]
    # the values come back as strings so cast each to int under its short name
    return {name: int(attrs[attribute]) for name, attribute in COUNTERS.items()}


# function to watch the queue until every message is past its delay
@task
def monitor_queue(queue_url: str, poll_s: int = 5, timeout_s: int = 1200) -> dict:
    """Log the three counters every poll_s seconds until nothing is delayed; raise after timeout_s."""
    logger = get_run_logger()
    # the time to give up at
    deadline = time.monotonic() + timeout_s
    # keep polling
    while True:
        # grab the current counts and log them
        counts = get_counts(queue_url)
        logger.info(
            "Queue counts: visible=%d not_visible=%d delayed=%d (total=%d)",
            counts["visible"], counts["not_visible"], counts["delayed"], sum(counts.values()),
        )
        # nothing delayed means every message can be received now
        if counts["delayed"] == 0:
            return counts
        # past the deadline so fail clearly instead of looping forever
        if time.monotonic() >= deadline:
            raise TimeoutError(f"{counts['delayed']} message(s) still delayed after {timeout_s}s; giving up")
        # wait before the next poll
        time.sleep(poll_s)


# function to receive every message, store it, then delete it
@task
def collect_messages(queue_url: str, expected: int = 21, wait_s: int = 5, timeout_s: int = 120) -> int:
    """Receive, land in DuckDB, then delete. Return how many fragments are stored."""
    logger = get_run_logger()
    # the time to give up at
    deadline = time.monotonic() + timeout_s
    con = duckdb.connect(DUCKDB_PATH)
    try:
        # keep receiving
        while True:
            # ask for up to 10 messages with their attributes
            # wait_s lets the call wait a few seconds if nothing is visible yet
            resp = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=wait_s,
                MessageAttributeNames=["All"],
            )
            # an empty response has no Messages key so default to an empty list
            messages = resp.get("Messages", [])
            for msg in messages:
                # the data is in the attributes and not the body
                attrs = msg.get("MessageAttributes", {})
                order_no = attrs.get("order_no", {}).get("StringValue")
                word = attrs.get("word", {}).get("StringValue")
                if order_no is None or word is None:
                    # a message missing either attribute is not a fragment so log it and skip the insert
                    logger.warning(
                        "Malformed message %s with attributes %s, deleting without storing",
                        msg["MessageId"], sorted(attrs),
                    )
                else:
                    # store the fragment first
                    con.execute("INSERT INTO raw.fragments VALUES (?, ?, now())", [order_no, word])
                # only then delete the message using its receipt handle
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
            # count distinct order numbers so a fragment stored twice counts once
            stored = con.execute("SELECT COUNT(DISTINCT order_no) FROM raw.fragments").fetchone()[0]
            # grab what is left in the queue
            counts = get_counts(queue_url)
            remaining = sum(counts.values())
            logger.info(
                "Received %d message(s): %d/%d fragments stored, %d left in the queue",
                len(messages), stored, expected, remaining,
            )
            # done when every fragment is stored and the queue is empty
            if stored >= expected and remaining == 0:
                logger.info(
                    "Queue counts after collecting: visible=%d not_visible=%d delayed=%d",
                    counts["visible"], counts["not_visible"], counts["delayed"],
                )
                return stored
            # past the deadline so fail clearly instead of looping forever
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Only {stored}/{expected} fragments stored and {remaining} message(s) "
                    f"left in the queue after {timeout_s}s; giving up"
                )
    finally:
        # always close so dbt can open the file later
        con.close()


@task
def dbt_build() -> None:
    """Run `dbt build --project-dir dbt --profiles-dir dbt`; raise if anything fails."""
    # TODO (Checkpoint C)
    raise NotImplementedError


@task
def read_phrase() -> str:
    """Read the phrase from the dbt mart model in DuckDB."""
    # TODO (Checkpoint C)
    raise NotImplementedError


@task
def submit_solution(phrase: str, platform: str = "prefect") -> int:
    """Send to dp2-submit with uvaid / phrase / platform attributes. Return the HTTP status code."""
    # TODO (Checkpoint D)
    raise NotImplementedError


@flow(name="dp2-pipeline", log_prints=True)
def dp2_pipeline():
    # start from an empty fragments table since the POST below starts a new puzzle
    reset_fragments()
    # fill the queue and keep its url
    queue_url = populate_queue()
    # then wait until nothing is delayed
    monitor_queue(queue_url)
    # receive, store and delete every message
    collect_messages(queue_url)
    # TODO (Lesson 10): dbt_build -> read_phrase -> submit_solution


if __name__ == "__main__":
    dp2_pipeline()
