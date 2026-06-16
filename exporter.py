from flask import Flask, Response, jsonify
from prometheus_client import Gauge, Info, generate_latest
from dotenv import load_dotenv

import requests
import os
import io
import json
import zipfile
import threading
import time

from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict

# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------

load_dotenv()

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_OWNER = os.getenv("GITHUB_OWNER")

REFRESH_INTERVAL = int(
    os.getenv("REFRESH_INTERVAL", "900")
)

REPO_INCLUDE = [
    x.strip()
    for x in os.getenv(
        "REPO_INCLUDE",
        ""
    ).split(",")
    if x.strip()
]

REPO_EXCLUDE = [
    x.strip()
    for x in os.getenv(
        "REPO_EXCLUDE",
        ""
    ).split(",")
    if x.strip()
]

LOKI_PUSH_URL = os.getenv(
    "LOKI_PUSH_URL",
    ""
)

PROCESSED_RUNS_FILE = "processed_runs.json"

if not GITHUB_TOKEN:
    raise Exception(
        "GITHUB_TOKEN is missing"
    )

if not GITHUB_OWNER:
    raise Exception(
        "GITHUB_OWNER is missing"
    )

app = Flask(__name__)

HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json"
}

# ---------------------------------------------------
# PROMETHEUS METRICS
# ---------------------------------------------------

WORKFLOW_STATUS = Gauge(
    "github_workflow_status",
    "Workflow status",
    ["repo", "workflow", "branch", "status"]
)

WORKFLOW_DURATION = Gauge(
    "github_workflow_duration_seconds",
    "Workflow duration",
    ["repo", "workflow", "branch"]
)

WORKFLOW_TOTAL_RUNS = Gauge(
    "github_workflow_total_runs",
    "Total workflow runs",
    ["repo", "workflow"]
)

WORKFLOW_SUCCESSES = Gauge(
    "github_workflow_successes",
    "Successful workflow runs",
    ["repo", "workflow"]
)

WORKFLOW_FAILURES = Gauge(
    "github_workflow_failures",
    "Failed workflow runs",
    ["repo", "workflow"]
)

BRANCH_FAILURES = Gauge(
    "github_branch_failures",
    "Branch failures",
    ["repo", "branch"]
)

EVENT_TYPE = Gauge(
    "github_event_type_total",
    "Workflow event type",
    ["repo", "event"]
)

FAILED_JOB = Gauge(
    "github_failed_job",
    "Failed jobs",
    ["repo", "workflow", "job"]
)

FAILED_STEP = Gauge(
    "github_failed_step",
    "Failed steps",
    ["repo", "workflow", "job", "step",  "run_id", "run_url"]
)

PIPELINE_INFO = Gauge(
    "github_pipeline_info",
    "Pipeline information",
    [
        "repo",
        "workflow",
        "branch",
        "status",
        "run_id",
        "run_url"
    ]
)

# ---------------------------------------------------
# PR METRICS
# ---------------------------------------------------

PR_OPEN = Gauge(
    "github_pr_open_total",
    "Open PRs",
    ["repo"]
)

PR_MERGED = Gauge(
    "github_pr_merged_total",
    "Merged PRs",
    ["repo"]
)

PR_CLOSED = Gauge(
    "github_pr_closed_total",
    "Closed PRs",
    ["repo"]
)

PR_STALE = Gauge(
    "github_pr_stale_total",
    "Stale PRs",
    ["repo"]
)

PR_LEAD_TIME = Gauge(
    "github_pr_lead_time_seconds",
    "PR lead time",
    ["repo"]
)

# ---------------------------------------------------
# DEPLOYMENT METRICS
# ---------------------------------------------------

DEPLOY_TOTAL = Gauge(
    "github_deployment_total",
    "Deployments",
    ["repo"]
)

DEPLOY_SUCCESS = Gauge(
    "github_deployment_success_total",
    "Successful deployments",
    ["repo"]
)

DEPLOY_FAILURE = Gauge(
    "github_deployment_failure_total",
    "Failed deployments",
    ["repo"]
)

# ---------------------------------------------------
# API RATE LIMIT
# ---------------------------------------------------

RATE_LIMIT_REMAINING = Gauge(
    "github_api_rate_limit_remaining",
    "Remaining GitHub API calls"
)

RATE_LIMIT_USED = Gauge(
    "github_api_rate_limit_used",
    "Used GitHub API calls"
)

RATE_LIMIT_RESET = Gauge(
    "github_api_rate_limit_reset_timestamp",
    "GitHub API reset timestamp"
)

# ---------------------------------------------------
# PROCESSED RUN STORAGE
# ---------------------------------------------------

def load_processed_runs():

    if not Path(PROCESSED_RUNS_FILE).exists():
        return set()

    try:

        with open(
            PROCESSED_RUNS_FILE,
            "r"
        ) as f:

            return set(
                json.load(f)
            )

    except Exception as e:
        print(e)

    return set()


def save_processed_runs(processed_runs):

    try:

        with open(
            PROCESSED_RUNS_FILE,
            "w"
        ) as f:

            json.dump(
                list(processed_runs),
                f
            )

    except Exception as e:
        print(e)


processed_runs = load_processed_runs()

# ---------------------------------------------------
# GITHUB HELPERS
# ---------------------------------------------------

def github_get(url):

    for _ in range(3):

        try:

            response = requests.get(
                url,
                headers=HEADERS,
                timeout=60
            )

            RATE_LIMIT_REMAINING.set(
                int(
                    response.headers.get(
                        "X-RateLimit-Remaining",
                        0
                    )
                )
            )

            RATE_LIMIT_USED.set(
                int(
                    response.headers.get(
                        "X-RateLimit-Used",
                        0
                    )
                )
            )

            RATE_LIMIT_RESET.set(
                int(
                    response.headers.get(
                        "X-RateLimit-Reset",
                        0
                    )
                )
            )

            if response.status_code == 200:
                return response.json()

            print(
                f"GitHub API error: "
                f"{response.status_code}"
            )

        except Exception as e:
            print(e)

        time.sleep(2)

    return {}


def get_repositories():

    repos = []

    page = 1

    while True:

        data = github_get(
            f"https://api.github.com/orgs/"
            f"{GITHUB_OWNER}/repos"
            f"?per_page=100&page={page}"
        )

        if not data:
            break

        repos.extend(data)

        if len(data) < 100:
            break

        page += 1

    repo_names = [
        r["name"]
        for r in repos
    ]

    if REPO_INCLUDE:

        repo_names = [
            r
            for r in repo_names
            if r in REPO_INCLUDE
        ]

    if REPO_EXCLUDE:

        repo_names = [
            r
            for r in repo_names
            if r not in REPO_EXCLUDE
        ]

    return repo_names


def duration(started, completed):

    try:

        s = datetime.strptime(
            started,
            "%Y-%m-%dT%H:%M:%SZ"
        )

        e = datetime.strptime(
            completed,
            "%Y-%m-%dT%H:%M:%SZ"
        )

        return max(
            (e - s).total_seconds(),
            0
        )

    except Exception:
        return 0
# ---------------------------------------------------
# GITHUB ACTION LOG DOWNLOAD
# ---------------------------------------------------

def get_workflow_logs(
    repo,
    run_id
):

    url = (
        f"https://api.github.com/repos/"
        f"{GITHUB_OWNER}/{repo}"
        f"/actions/runs/{run_id}/logs"
    )

    try:

        response = requests.get(
            url,
            headers=HEADERS,
            timeout=120
        )

        if response.status_code != 200:

            print(
                f"Unable to download logs "
                f"for run={run_id} "
                f"status={response.status_code}"
            )

            return []

        files = []

        with zipfile.ZipFile(
            io.BytesIO(response.content)
        ) as z:

            for filename in z.namelist():

                try:

                    content = z.read(
                        filename
                    ).decode(
                        "utf-8",
                        errors="ignore"
                    )

                    files.append(
                        {
                            "job": filename,
                            "content": content
                        }
                    )

                except Exception as e:
                    print(e)

        return files

    except Exception as e:

        print(
            f"Log download failed "
            f"run={run_id}"
        )

        print(e)

        return []

# ---------------------------------------------------
# LOG CHUNKING
# ---------------------------------------------------

def chunk_lines(
    lines,
    batch_size=500
):

    for i in range(
        0,
        len(lines),
        batch_size
    ):

        yield lines[
            i:i + batch_size
        ]

# ---------------------------------------------------
# LOKI PUSH
# ---------------------------------------------------

def send_loki_batch(
    repo,
    workflow,
    run_id,
    branch,
    status,
    job_name,
    lines
):

    if not LOKI_PUSH_URL:
        return

    if not lines:
        return

    try:

        base_ts = int(
            time.time() * 1e9
        )

        values = []

        for idx, line in enumerate(lines):

            if not line.strip():
                continue

            values.append(
                [
                    str(base_ts + idx),
                    line[:10000]
                ]
            )

        if not values:
            return

        payload = {
            "streams": [
                {
                    "stream": {
                        "repo": repo,
                        "workflow": workflow,
                        "run_id": str(run_id),
                        "branch": branch,
                        "status": status,
                        "job": job_name
                    },
                    "values": values
                }
            ]
        }

        response = requests.post(
            LOKI_PUSH_URL,
            json=payload,
            timeout=30
        )

        if response.status_code >= 300:

            print(
                "Loki ingestion failed:",
                response.status_code,
                response.text
            )

    except Exception as e:
        print(e)

# ---------------------------------------------------
# RETRYABLE LOKI PUSH
# ---------------------------------------------------

def send_loki_batch_with_retry(
    repo,
    workflow,
    run_id,
    branch,
    status,
    job_name,
    lines,
    retries=3
):

    for attempt in range(retries):

        try:

            send_loki_batch(
                repo,
                workflow,
                run_id,
                branch,
                status,
                job_name,
                lines
            )

            return

        except Exception as e:

            print(
                f"Loki retry "
                f"{attempt + 1}"
            )

            print(e)

            time.sleep(2)

# ---------------------------------------------------
# PUSH ENTIRE WORKFLOW
# ---------------------------------------------------

def push_workflow_logs(
    repo,
    workflow,
    run_id,
    branch,
    status
):

    global processed_runs

    run_key = str(run_id)

    if run_key in processed_runs:

        return

    print(
        f"Downloading logs "
        f"repo={repo} "
        f"run={run_id}"
    )

    log_files = get_workflow_logs(
        repo,
        run_id
    )

    if not log_files:

        print(
            f"No logs found "
            f"run={run_id}"
        )

        return

    total_lines = 0

    for log_file in log_files:

        job_name = log_file["job"]

        lines = (
            log_file["content"]
            .splitlines()
        )

        total_lines += len(lines)

        for batch in chunk_lines(
            lines,
            500
        ):

            send_loki_batch_with_retry(
                repo=repo,
                workflow=workflow,
                run_id=run_id,
                branch=branch,
                status=status,
                job_name=job_name,
                lines=batch
            )

    processed_runs.add(run_key)

    save_processed_runs(
        processed_runs
    )

    print(
        f"Completed Loki push "
        f"run={run_id} "
        f"lines={total_lines}"
    )

# ---------------------------------------------------
# PROCESS COMPLETED WORKFLOW
# ---------------------------------------------------

def process_workflow_logs(
    repo,
    workflow,
    run_id,
    branch,
    status
):

    if status not in [
        "success",
        "failure",
        "cancelled",
        "timed_out",
        "startup_failure"
    ]:
        return

    push_workflow_logs(
        repo=repo,
        workflow=workflow,
        run_id=run_id,
        branch=branch,
        status=status
    )
# ---------------------------------------------------
# MAIN METRIC COLLECTION
# ---------------------------------------------------

def update_metrics():

    repos = get_repositories()

    wf_stats = defaultdict(
        lambda: {
            "total": 0,
            "success": 0,
            "failure": 0
        }
    )

    for repo in repos:

        print(f"Collecting repo={repo}")

        branch_failures = defaultdict(int)

        runs_response = github_get(
            f"https://api.github.com/repos/"
            f"{GITHUB_OWNER}/{repo}"
            f"/actions/runs"
            f"?status=completed"
            f"&per_page=100"
        )

        workflow_runs = runs_response.get(
            "workflow_runs",
            []
        )

        for run in workflow_runs:

            workflow = run.get(
                "name",
                "unknown"
            )

            branch = run.get(
                "head_branch",
                "unknown"
            )

            status = run.get(
                "conclusion",
                "unknown"
            )

            event = run.get(
                "event",
                "unknown"
            )

            run_id = run.get(
                "id"
            )

            run_url = run.get(
                "html_url",
                ""
            )

            # ----------------------------------------
            # WORKFLOW STATUS
            # ----------------------------------------

            WORKFLOW_STATUS.labels(
                repo,
                workflow,
                branch,
                status
            ).set(1)

            WORKFLOW_DURATION.labels(
                repo,
                workflow,
                branch
            ).set(
                duration(
                    run.get("run_started_at"),
                    run.get("updated_at")
                )
            )

            wf_stats[
                (repo, workflow)
            ]["total"] += 1

            if status == "success":

                wf_stats[
                    (repo, workflow)
                ]["success"] += 1

            if status == "failure":

                wf_stats[
                    (repo, workflow)
                ]["failure"] += 1

                branch_failures[
                    branch
                ] += 1

            # ----------------------------------------
            # EVENT COUNTER
            # ----------------------------------------

            try:
                EVENT_TYPE.labels(
                    repo,
                    event
                ).inc()
            except Exception:
                pass

            # ----------------------------------------
            # PIPELINE INFO
            # ----------------------------------------

            PIPELINE_INFO.labels(
                repo,
                workflow,
                branch,
                str(status),
                str(run_id),
                run_url
            ).set(1)
            # ----------------------------------------
            # PUSH LOGS TO LOKI
            # ----------------------------------------

            process_workflow_logs(
                repo,
                workflow,
                run_id,
                branch,
                status
            )

            # ----------------------------------------
            # JOB DETAILS
            # ----------------------------------------

            jobs_response = github_get(
                f"https://api.github.com/repos/"
                f"{GITHUB_OWNER}/{repo}"
                f"/actions/runs/{run_id}/jobs"
            )

            jobs = jobs_response.get(
                "jobs",
                []
            )

            for job in jobs:

                job_name = job.get(
                    "name",
                    "unknown"
                )

                if job.get(
                    "conclusion"
                ) == "failure":

                    FAILED_JOB.labels(
                        repo,
                        workflow,
                        job_name
                    ).set(1)

                for step in job.get(
                    "steps",
                    []
                ):

                    if step.get(
                        "conclusion"
                    ) == "failure":

                        FAILED_STEP.labels(
                            repo,
                            workflow,
                            job_name,
                            step.get("name", "unknown"),
                            str(run_id),
                            run.get("html_url", "")
                        ).set(1)

        # ----------------------------------------
        # BRANCH FAILURES
        # ----------------------------------------

        for branch_name, count in branch_failures.items():

            BRANCH_FAILURES.labels(
                repo,
                branch_name
            ).set(count)

        # ----------------------------------------
        # DEPLOYMENT METRICS
        # ----------------------------------------

        DEPLOY_TOTAL.labels(
            repo
        ).set(
            len(workflow_runs)
        )

        DEPLOY_SUCCESS.labels(
            repo
        ).set(
            sum(
                1
                for r in workflow_runs
                if r.get(
                    "conclusion"
                ) == "success"
            )
        )

        DEPLOY_FAILURE.labels(
            repo
        ).set(
            sum(
                1
                for r in workflow_runs
                if r.get(
                    "conclusion"
                ) == "failure"
            )
        )

        # ----------------------------------------
        # PR METRICS
        # ----------------------------------------

        pulls = github_get(
            f"https://api.github.com/repos/"
            f"{GITHUB_OWNER}/{repo}"
            f"/pulls?state=all"
            f"&per_page=100"
        )

        open_count = 0
        merged_count = 0
        closed_count = 0
        stale_count = 0

        lead_times = []

        if isinstance(
            pulls,
            list
        ):

            for pr in pulls:

                state = pr.get(
                    "state"
                )

                if state == "open":
                    open_count += 1

                if state == "closed":
                    closed_count += 1

                if pr.get(
                    "merged_at"
                ):

                    merged_count += 1

                    try:

                        created = datetime.strptime(
                            pr["created_at"],
                            "%Y-%m-%dT%H:%M:%SZ"
                        )

                        merged = datetime.strptime(
                            pr["merged_at"],
                            "%Y-%m-%dT%H:%M:%SZ"
                        )

                        lead_times.append(
                            (
                                merged - created
                            ).total_seconds()
                        )

                    except Exception:
                        pass

                try:

                    created = datetime.strptime(
                        pr["created_at"],
                        "%Y-%m-%dT%H:%M:%SZ"
                    ).replace(
                        tzinfo=timezone.utc
                    )

                    age = (
                        datetime.now(
                            timezone.utc
                        ) - created
                    ).days

                    if (
                        state == "open"
                        and age > 14
                    ):
                        stale_count += 1

                except Exception:
                    pass

        PR_OPEN.labels(
            repo
        ).set(open_count)

        PR_MERGED.labels(
            repo
        ).set(merged_count)

        PR_CLOSED.labels(
            repo
        ).set(closed_count)

        PR_STALE.labels(
            repo
        ).set(stale_count)

        avg_lead = (
            sum(lead_times)
            / len(lead_times)
            if lead_times
            else 0
        )

        PR_LEAD_TIME.labels(
            repo
        ).set(avg_lead)

    # --------------------------------------------
    # WORKFLOW TOTALS
    # --------------------------------------------

    for (
        repo,
        workflow
    ), stats in wf_stats.items():

        WORKFLOW_TOTAL_RUNS.labels(
            repo,
            workflow
        ).set(
            stats["total"]
        )

        WORKFLOW_SUCCESSES.labels(
            repo,
            workflow
        ).set(
            stats["success"]
        )

        WORKFLOW_FAILURES.labels(
            repo,
            workflow
        ).set(
            stats["failure"]
        )

# ---------------------------------------------------
# BACKGROUND REFRESH THREAD
# ---------------------------------------------------

def updater():

    while True:

        try:

            print(
                "Starting collection cycle..."
            )

            update_metrics()

            print(
                "Collection cycle complete"
            )

        except Exception as e:

            print(
                "Collector error:"
            )

            print(e)

        time.sleep(
            REFRESH_INTERVAL
        )

# ---------------------------------------------------
# FLASK ROUTES
# ---------------------------------------------------

@app.route("/")
def home():

    return jsonify({
        "service": "github-exporter",
        "owner": GITHUB_OWNER
    })

@app.route("/health")
def health():

    return jsonify({
        "status": "healthy"
    })

@app.route("/metrics")
def metrics():

    return Response(
        generate_latest(),
        mimetype="text/plain"
    )

# ---------------------------------------------------
# MAIN
# ---------------------------------------------------

if __name__ == "__main__":

    thread = threading.Thread(
        target=updater,
        daemon=True
    )

    thread.start()

    app.run(
        host="0.0.0.0",
        port=8000,
        threaded=True
    )
