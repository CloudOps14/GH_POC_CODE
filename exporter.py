from flask import Flask, Response, jsonify
from prometheus_client import Gauge, Info, generate_latest
from dotenv import load_dotenv
import requests
import os
import threading
import time
from datetime import datetime, timezone
from collections import defaultdict

# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------
load_dotenv()

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_OWNER = os.getenv("GITHUB_OWNER")
REFRESH_INTERVAL = int(os.getenv("REFRESH_INTERVAL", "900"))
REPO_INCLUDE = [x.strip() for x in os.getenv("REPO_INCLUDE", "").split(",") if x.strip()]
REPO_EXCLUDE = [x.strip() for x in os.getenv("REPO_EXCLUDE", "").split(",") if x.strip()]

LOKI_URL = os.getenv("LOKI_URL", "")

if not GITHUB_TOKEN:
    raise Exception("GITHUB_TOKEN is missing")

if not GITHUB_OWNER:
    raise Exception("GITHUB_OWNER is missing")

app = Flask(__name__)

HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json"
}

# ---------------------------------------------------
# METRICS
# ---------------------------------------------------
WORKFLOW_STATUS = Gauge("github_workflow_status","Workflow status",["repo","workflow","branch","status"])
WORKFLOW_DURATION = Gauge("github_workflow_duration_seconds","Workflow duration",["repo","workflow","branch"])
WORKFLOW_TOTAL_RUNS = Gauge("github_workflow_total_runs","Total runs",["repo","workflow"])
WORKFLOW_SUCCESSES = Gauge("github_workflow_successes","Success runs",["repo","workflow"])
WORKFLOW_FAILURES = Gauge("github_workflow_failures","Failure runs",["repo","workflow"])
BRANCH_FAILURES = Gauge("github_branch_failures","Branch failures",["repo","branch"])
EVENT_TYPE = Gauge("github_event_type_total","Events",["repo","event"])
FAILED_JOB = Gauge("github_failed_job","Failed jobs",["repo","workflow","job"])
FAILED_STEP = Gauge("github_failed_step","Failed steps",["repo","workflow","job","step"])
PIPELINE_INFO = Info("github_pipeline_info","Pipeline info")

PR_OPEN = Gauge("github_pr_open_total","Open PRs",["repo"])
PR_MERGED = Gauge("github_pr_merged_total","Merged PRs",["repo"])
PR_CLOSED = Gauge("github_pr_closed_total","Closed PRs",["repo"])
PR_STALE = Gauge("github_pr_stale_total","Stale PRs",["repo"])
PR_LEAD_TIME = Gauge("github_pr_lead_time_seconds","PR lead time",["repo"])

DEPLOY_TOTAL = Gauge("github_deployment_total","Deployments",["repo"])
DEPLOY_SUCCESS = Gauge("github_deployment_success_total","Successful deployments",["repo"])
DEPLOY_FAILURE = Gauge("github_deployment_failure_total","Failed deployments",["repo"])






RATE_LIMIT_REMAINING = Gauge("github_api_rate_limit_remaining","Remaining API calls")
RATE_LIMIT_USED = Gauge("github_api_rate_limit_used","Used API calls")
RATE_LIMIT_RESET = Gauge("github_api_rate_limit_reset_timestamp","Reset timestamp")

# ---------------------------------------------------
# HELPERS
# ---------------------------------------------------
def github_get(url):
    for _ in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            RATE_LIMIT_REMAINING.set(int(r.headers.get("X-RateLimit-Remaining", 0)))
            RATE_LIMIT_USED.set(int(r.headers.get("X-RateLimit-Used", 0)))
            RATE_LIMIT_RESET.set(int(r.headers.get("X-RateLimit-Reset", 0)))
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
        time.sleep(2)
    return {}

def get_repositories():
    repos = []
    page = 1
    while True:
        data = github_get(f"https://api.github.com/orgs/{GITHUB_OWNER}/repos?per_page=100&page={page}")
        if not data:
            break
        repos.extend(data)
        if len(data) < 100:
            break
        page += 1

    names = [r["name"] for r in repos]

    if REPO_INCLUDE:
        names = [r for r in names if r in REPO_INCLUDE]

    if REPO_EXCLUDE:
        names = [r for r in names if r not in REPO_EXCLUDE]

    return names

def duration(started, completed):
    try:
        s = datetime.strptime(started,"%Y-%m-%dT%H:%M:%SZ")
        e = datetime.strptime(completed,"%Y-%m-%dT%H:%M:%SZ")
        return max((e-s).total_seconds(),0)
    except Exception:
        return 0

def send_loki(repo, workflow, message):
    if not LOKI_URL:
        return
    payload = {
        "streams":[{
            "stream":{"repo":repo,"workflow":workflow},
            "values":[[str(int(time.time()*1e9)), message]]
        }]
    }
    try:
        requests.post(LOKI_URL, json=payload, timeout=10)
    except Exception:
        pass

# ---------------------------------------------------
# DATA COLLECTION
# ---------------------------------------------------
def update_metrics():
    repos = get_repositories()

    wf_stats = defaultdict(lambda: {"total":0,"success":0,"failure":0})

    for repo in repos:

        runs = github_get(
            f"https://api.github.com/repos/{GITHUB_OWNER}/{repo}/actions/runs?per_page=20"
        ).get("workflow_runs", [])

        branch_failures = defaultdict(int)

        for run in runs:
            workflow = run.get("name","unknown")
            branch = run.get("head_branch","unknown")
            status = run.get("conclusion","unknown")
            event = run.get("event","unknown")
            run_id = run.get("id")

            WORKFLOW_STATUS.labels(repo,workflow,branch,status).set(1)
            WORKFLOW_DURATION.labels(
                repo,workflow,branch
            ).set(duration(run.get("run_started_at"), run.get("updated_at")))

            wf_stats[(repo,workflow)]["total"] += 1

            if status == "success":
                wf_stats[(repo,workflow)]["success"] += 1

            if status == "failure":
                wf_stats[(repo,workflow)]["failure"] += 1
                branch_failures[branch] += 1

                send_loki(repo, workflow, f"Workflow failed run_id={run_id}")

                jobs = github_get(
                    f"https://api.github.com/repos/{GITHUB_OWNER}/{repo}/actions/runs/{run_id}/jobs"
                ).get("jobs", [])

                for job in jobs:
                    if job.get("conclusion") == "failure":
                        FAILED_JOB.labels(repo,workflow,job.get("name","unknown")).set(1)

                        for step in job.get("steps", []):
                            if step.get("conclusion") == "failure":
                                FAILED_STEP.labels(
                                    repo,workflow,job.get("name","unknown"),step.get("name","unknown")
                                ).set(1)

            EVENT_TYPE.labels(repo,event).set(EVENT_TYPE.labels(repo,event)._value.get() + 1)

            PIPELINE_INFO.info({
                "repo": repo,
                "workflow": workflow,
                "branch": branch,
                "status": str(status),
                "run_id": str(run_id)
            })

        for b,c in branch_failures.items():
            BRANCH_FAILURES.labels(repo,b).set(c)

        pulls = github_get(
            f"https://api.github.com/repos/{GITHUB_OWNER}/{repo}/pulls?state=all&per_page=100"
        )

        open_count = 0
        merged_count = 0
        closed_count = 0
        stale_count = 0
        lead_times = []

        for pr in pulls if isinstance(pulls, list) else []:
            state = pr.get("state")

            if state == "open":
                open_count += 1

            if pr.get("merged_at"):
                merged_count += 1

                try:
                    created = datetime.strptime(pr["created_at"], "%Y-%m-%dT%H:%M:%SZ")
                    merged = datetime.strptime(pr["merged_at"], "%Y-%m-%dT%H:%M:%SZ")
                    lead_times.append((merged-created).total_seconds())
                except Exception:
                    pass

            if state == "closed":
                closed_count += 1

            try:
                created = datetime.strptime(pr["created_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                age = (datetime.now(timezone.utc)-created).days
                if state == "open" and age > 14:
                    stale_count += 1
            except Exception:
                pass

        PR_OPEN.labels(repo).set(open_count)
        PR_MERGED.labels(repo).set(merged_count)
        PR_CLOSED.labels(repo).set(closed_count)
        PR_STALE.labels(repo).set(stale_count)

        avg_lead = sum(lead_times)/len(lead_times) if lead_times else 0
        PR_LEAD_TIME.labels(repo).set(avg_lead)


        DEPLOY_TOTAL.labels(repo).set(len(runs))
        DEPLOY_SUCCESS.labels(repo).set(sum(1 for r in runs if r.get("conclusion")=="success"))
        DEPLOY_FAILURE.labels(repo).set(sum(1 for r in runs if r.get("conclusion")=="failure"))





    for (repo,wf), stats in wf_stats.items():
        WORKFLOW_TOTAL_RUNS.labels(repo,wf).set(stats["total"])
        WORKFLOW_SUCCESSES.labels(repo,wf).set(stats["success"])
        WORKFLOW_FAILURES.labels(repo,wf).set(stats["failure"])

def updater():
    while True:
        try:
            update_metrics()
        except Exception as e:
            print(e)
        time.sleep(REFRESH_INTERVAL)

@app.route("/")
def home():
    return jsonify({"service":"github-exporter","owner":GITHUB_OWNER})

@app.route("/health")
def health():
    return jsonify({"status":"healthy"})

@app.route("/metrics")
def metrics():
    return Response(generate_latest(), mimetype="text/plain")

if __name__ == "__main__":
    t = threading.Thread(target=updater, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=8000, threaded=True)
