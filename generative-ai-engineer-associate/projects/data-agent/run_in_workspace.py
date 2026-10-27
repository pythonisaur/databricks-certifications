"""Run the data-agent notebooks as serverless jobs from a laptop and print their exit values.

    python run_in_workspace.py [--profile lakehouse] [--only 01,02raw,02ctx,03] [--endpoint databricks-gpt-oss-120b]

Needs the notebooks imported at WS_DIR (databricks workspace import-dir . <WS_DIR> --overwrite) and the
food_delivery.main tables (ingest_csv.py). Results go to runs/<timestamp>.json next to this file.
"""
import argparse, json, os, subprocess, sys, time
from datetime import datetime
from pathlib import Path
sys.stdout.reconfigure(encoding="utf-8")
os.environ["MSYS_NO_PATHCONV"] = "1"
HERE = Path(__file__).resolve().parent
WS_DIR = "/Users/byambaa1982@gmail.com/databricks-certifications/genai/projects/data-agent"


def cli(*args):
    r = subprocess.run(["databricks", *args], capture_output=True, text=True, encoding="utf-8")
    if r.returncode:
        raise SystemExit(f"databricks {' '.join(args[:3])}...\n{r.stderr.strip()}")
    return r.stdout


def submit(profile, key, notebook, params):
    body = {"run_name": f"data-agent {key}", "tasks": [{"task_key": key, "notebook_task": {
        "notebook_path": f"{WS_DIR}/{notebook}", "base_parameters": params}}]}          # no cluster = serverless
    return json.loads(cli("jobs", "submit", "-p", profile, "--no-wait", "--json", json.dumps(body)))["run_id"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="lakehouse"); ap.add_argument("--only", default="01,02raw,02ctx,03")
    ap.add_argument("--endpoint", default="databricks-gpt-oss-120b")
    a = ap.parse_args()
    common = {"llm_endpoint": a.endpoint, "catalog": "food_delivery", "schema": "main", "today": "2026-10-27"}
    PLAN = {
        "01": ("01_plain_chatbot", {"llm_endpoint": a.endpoint}),
        "02raw": ("02_data_agent", {**common, "use_context": "false"}),
        "02ctx": ("02_data_agent", {**common, "use_context": "true", "question": "What was revenue last month?"}),
        "03": ("03_right_and_wrong", {**common, "use_context": "false"}),
    }
    runs = {}
    for key in a.only.split(","):
        nb, params = PLAN[key]
        runs[key] = submit(a.profile, key, nb, params)
        print(f"submitted {key:6s} run_id={runs[key]}  {nb}")

    results, t0 = {}, time.time()
    pending = dict(runs)
    while pending:
        time.sleep(20)
        for key, rid in list(pending.items()):
            run = json.loads(cli("jobs", "get-run", str(rid), "-p", a.profile, "-o", "json"))
            st = run["state"]
            if st.get("life_cycle_state") in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                task_run_id = run["tasks"][0]["run_id"]
                out = json.loads(cli("jobs", "get-run-output", str(task_run_id), "-p", a.profile, "-o", "json"))
                exit_val = (out.get("notebook_output") or {}).get("result")
                try:
                    exit_val = json.loads(exit_val) if exit_val else None
                except json.JSONDecodeError:
                    pass
                results[key] = {"run_id": rid, "result_state": st.get("result_state"), "message": st.get("state_message"),
                                "error": out.get("error"), "exit": exit_val, "url": run.get("run_page_url"),
                                "seconds": round(time.time() - t0)}
                print(f"[{round(time.time() - t0):4d}s] {key:6s} {st.get('result_state')}  {run.get('run_page_url')}")
                del pending[key]
        if pending and int(time.time() - t0) % 120 < 20:
            print(f"[{round(time.time() - t0):4d}s] waiting on {list(pending)}")

    (HERE / "runs").mkdir(exist_ok=True)
    out_path = HERE / "runs" / f"{datetime.now():%Y-%m-%d_%H%M}.json"
    out_path.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
    print("\nsaved", out_path)
    for key, r in results.items():
        print("\n" + "=" * 90 + f"\n{key}  {r['result_state']}  {r['url']}")
        if r["error"]:
            print("ERROR:", str(r["error"])[:1500])
        print(json.dumps(r["exit"], indent=2, ensure_ascii=False)[:3500] if r["exit"] else "(no exit value)")


if __name__ == "__main__":
    main()
