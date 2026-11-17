# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # What makes an Asset Bundle: `databricks.yml` at the root
# MAGIC
# MAGIC Data Engineer Associate, exam-guide section "Productionizing Data Pipelines" (this repo's folder
# MAGIC `04-lakeflow-jobs`). Objective: describe Databricks Asset Bundles and explain their role in CI/CD for data
# MAGIC pipelines.
# MAGIC
# MAGIC Import this file into a Databricks workspace (Workspace > Import) and run it top to bottom. Serverless
# MAGIC compute is enough and Free Edition works. Bundles are validated and deployed by the Databricks CLI from a
# MAGIC laptop or a CI runner, not from a notebook, so every CLI command appears as the exact command you would type,
# MAGIC and the notebook runs a small local stand-in for it: it writes each option's project tree to `/tmp` on the
# MAGIC driver, and a Python model of `databricks bundle validate` / `deploy` / `run` reads that tree with the same
# MAGIC rules the CLI applies (root file, `include` globs, path resolution, unknown-field warnings, target modes).
# MAGIC Nothing is created in Unity Catalog and nothing is deployed.
# MAGIC
# MAGIC **The exam question this lesson answers**
# MAGIC
# MAGIC A data engineer is setting up a new Databricks Asset Bundle project. After running `databricks bundle init`,
# MAGIC they examine the generated project structure. Which of the following BEST describes the required top-level
# MAGIC components of a well-formed Asset Bundle, and what role each component plays in the deployment lifecycle?
# MAGIC
# MAGIC * **A** A `databricks.yml` root file declares the bundle name and its targets; notebooks and Python scripts stay in the workspace file system and are resolved by path at run time instead of being deployed from the local project tree.
# MAGIC * **B** A `databricks.yml` file declares the targets, a `requirements.txt` pins the Python dependencies and a `Makefile` drives the build; `databricks bundle validate` rejects a project that is missing any one of these three files.
# MAGIC * **C** A `databricks.yml` root file declares the bundle name, the workspace targets for each environment and the resource definitions; notebooks, Python scripts and extra YAML resource files sit in the project tree and are referenced from that root.
# MAGIC * **D** A `bundle_manifest.json` lists each asset with its SHA-256 hash, a `databricks.yml` file declares the targets, and a `clusters/` directory holds one JSON file per cluster definition that the jobs in the bundle refer to.
# MAGIC * **E** A `databricks.yml` file links the targets to a `pipeline_spec.json` for the Lakeflow Spark Declarative Pipelines and a `jobs_spec.json` for the Lakeflow Jobs, as the bundle schema for pipelines and jobs is written in JSON spec files.
# MAGIC
# MAGIC The correct option (C) is built and run first: validated, deployed to a `dev` and a `prod` target and run in a
# MAGIC fresh production workspace. Then A, B, D and E are each written as a project tree and put through the same
# MAGIC three steps, so you see exactly where each one breaks or what it silently ignores.

# COMMAND ----------

# DBTITLE 1,Question metadata
QUESTION = {
    "key": "e58476c849",
    "track": "de_associate",
    "section": "Productionizing Data Pipelines",
    "stem": "A data engineer is setting up a new Databricks Asset Bundle project. After running `databricks bundle init`, they examine the generated project structure. Which of the following BEST describes the required top-level components of a well-formed Asset Bundle, and what role each component plays in the deployment lifecycle?",
    "correct": "C",
    "options": {
        "A": "A `databricks.yml` root file declares the bundle name and its targets; notebooks and Python scripts stay in the workspace file system and are resolved by path at run time instead of being deployed from the local project tree.",
        "B": "A `databricks.yml` file declares the targets, a `requirements.txt` pins the Python dependencies and a `Makefile` drives the build; `databricks bundle validate` rejects a project that is missing any one of these three files.",
        "C": "A `databricks.yml` root file declares the bundle name, the workspace targets for each environment and the resource definitions; notebooks, Python scripts and extra YAML resource files sit in the project tree and are referenced from that root.",
        "D": "A `bundle_manifest.json` lists each asset with its SHA-256 hash, a `databricks.yml` file declares the targets, and a `clusters/` directory holds one JSON file per cluster definition that the jobs in the bundle refer to.",
        "E": "A `databricks.yml` file links the targets to a `pipeline_spec.json` for the Lakeflow Spark Declarative Pipelines and a `jobs_spec.json` for the Lakeflow Jobs, as the bundle schema for pipelines and jobs is written in JSON spec files."
    },
    "objective": "Describe Databricks Asset Bundles and explain their role in CI/CD for data pipelines.",
    "source": "testlogichub exams/fixes/qfix/e58476c849.py"
}

# COMMAND ----------

# MAGIC %pip install -q pyyaml

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup: a scratch folder, and stand-ins for the three CLI commands
# MAGIC
# MAGIC The real lifecycle of a bundle is four CLI commands, run from the project root on a laptop or in CI:
# MAGIC
# MAGIC ```bash
# MAGIC databricks bundle init default-python      # scaffold a project (asks for a name, writes the tree)
# MAGIC databricks bundle validate -t dev          # load databricks.yml + includes, resolve paths, check the schema
# MAGIC databricks bundle deploy   -t prod         # upload the project files and create/update the resources
# MAGIC databricks bundle run      -t prod orders_etl_job   # trigger a deployed job or pipeline by its resource key
# MAGIC ```
# MAGIC
# MAGIC None of them needs anything except one file at the project root: `databricks.yml`. The CLI walks up from
# MAGIC the current directory until it finds it; that directory *is* the bundle root. Everything else (notebooks,
# MAGIC `.py` files, more YAML under `resources/`) is optional, lives in the project tree, and is pulled in because
# MAGIC `databricks.yml` points at it. The cells below build three small stand-ins that apply those rules to a
# MAGIC folder on the driver's local disk.

# COMMAND ----------

# DBTITLE 1,Scratch folder under /tmp and a helper that writes a project tree
import json, re, shutil, textwrap
from pathlib import Path
import yaml

SCRATCH = Path("/tmp/qnote_e58476c849")
if SCRATCH.exists():
    shutil.rmtree(SCRATCH)          # only this lesson's own scratch folder under /tmp
SCRATCH.mkdir(parents=True)

try:
    USER = spark.sql("SELECT current_user()").first()[0]
except Exception:
    USER = "you@example.com"

def write_project(name: str, files: dict) -> Path:
    """Write {relative path: text} under /tmp/qnote_e58476c849/<name> and return the project root."""
    root = SCRATCH / name
    for rel, text in files.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(textwrap.dedent(text).lstrip("\n"))
    return root

def show_tree(root: Path):
    print(f"{root.name}/")
    for p in sorted(root.rglob("*")):
        if p.is_file():
            print("  " + p.relative_to(root).as_posix())

RESULTS = {}   # one verdict per option, compared in the last section
print("scratch:", SCRATCH, "| current user:", USER)

# COMMAND ----------

# DBTITLE 1,Stand-in for `databricks bundle validate`: root file, includes, schema, paths
KNOWN_TOP = {"bundle", "include", "targets", "resources", "variables", "workspace", "artifacts",
             "sync", "permissions", "run_as", "presets", "experimental", "scripts", "python"}
KNOWN_RESOURCES = {"jobs", "pipelines", "schemas", "volumes", "clusters", "dashboards", "apps",
                   "experiments", "models", "registered_models", "model_serving_endpoints",
                   "quality_monitors", "secret_scopes", "database_instances"}
KNOWN_TASK = {"task_key", "depends_on", "notebook_task", "spark_python_task", "python_wheel_task",
              "pipeline_task", "sql_task", "run_job_task", "condition_task", "for_each_task",
              "job_cluster_key", "new_cluster", "existing_cluster_id", "environment_key", "libraries",
              "max_retries", "run_if", "timeout_seconds", "email_notifications"}

class BundleError(Exception):
    pass

def _yaml(path: Path):
    return yaml.safe_load(path.read_text()) or {}

def bundle_validate(root: Path, quiet=False):
    """Return (config, warnings) or raise BundleError, like `databricks bundle validate` exits non-zero."""
    cfg_path = root / "databricks.yml"
    if not cfg_path.exists():
        raise BundleError("unable to locate bundle root: databricks.yml not found")
    cfg = _yaml(cfg_path)
    warnings = [f"unknown field: {k}  (databricks.yml)" for k in cfg if k not in KNOWN_TOP]
    if not (cfg.get("bundle") or {}).get("name"):
        raise BundleError("databricks.yml: bundle.name is required")
    if not cfg.get("targets"):
        warnings.append("no targets declared; every deploy goes to the single default workspace")
    resources, files_loaded = {}, ["databricks.yml"]

    def merge(doc, src: Path):
        res = doc.get("resources") or {}
        if not isinstance(res, dict):
            raise BundleError(f"{src.name}: resources: expected a map, found a {type(res).__name__}")
        for rtype, items in res.items():
            if rtype not in KNOWN_RESOURCES:
                warnings.append(f"unknown field: resources.{rtype}  ({src.name})")
                continue
            if not isinstance(items, dict):
                raise BundleError(f"{src.name}: resources.{rtype}: expected a map, found a {type(items).__name__}")
            for key, d in items.items():
                resources.setdefault(rtype, {})[key] = (d, src)

    merge(cfg, cfg_path)
    for pattern in cfg.get("include", []) or []:
        for p in sorted(root.glob(pattern)):
            files_loaded.append(p.relative_to(root).as_posix())
            merge(_yaml(p), p)

    def check_path(ref: str, src: Path, what: str):
        if ref.startswith("/"):
            warnings.append(f"{what} {ref} is a workspace path: not part of the bundle, nothing is deployed for it")
            return
        target = (src.parent / ref).resolve()
        if not target.exists():
            raise BundleError(f"{what} {ref} not found (resolved relative to {src.relative_to(root)})")

    for key, (job, src) in resources.get("jobs", {}).items():
        for t in job.get("tasks", []):
            for k in t:
                if k not in KNOWN_TASK:
                    warnings.append(f"unknown field: {k}  (jobs.{key}.tasks[{t.get('task_key')}])")
            if "notebook_task" in t:
                check_path(t["notebook_task"]["notebook_path"], src, "notebook")
            if "spark_python_task" in t:
                check_path(t["spark_python_task"]["python_file"], src, "file")
    for key, (pl, src) in resources.get("pipelines", {}).items():
        for lib in pl.get("libraries", []):
            for kind in ("notebook", "file"):
                if kind in lib:
                    check_path(lib[kind]["path"], src, kind)

    if not quiet:
        print("Name:", cfg["bundle"]["name"], "| targets:", list((cfg.get("targets") or {}).keys()))
        print("Config files loaded:", files_loaded)
        print("Resources:", {r: list(v) for r, v in resources.items()} or "none")
        for w in warnings:
            print("Warning:", w)
        print("Validation OK!")
    return {"cfg": cfg, "resources": resources, "root": root}, warnings

# COMMAND ----------

# DBTITLE 1,Stand-ins for `deploy` and `run`: what lands in the workspace, and what a run finds there
WORKSPACES = {"dev": set(), "prod": set()}   # workspace paths that exist, per target workspace

def _is_notebook(p: Path) -> bool:
    return p.suffix == ".py" and p.read_text().startswith("# Databricks notebook source")

def _subst(s, ctx):
    return re.sub(r"\$\{([^}]+)\}", lambda m: str(ctx.get(m.group(1), m.group(0))), s)

def bundle_deploy(bundle, target: str):
    """Upload the project tree and translate local paths, like `databricks bundle deploy -t <target>`."""
    cfg, root = bundle["cfg"], bundle["root"]
    tcfg = (cfg.get("targets") or {}).get(target, {}) or {}
    name, mode = cfg["bundle"]["name"], tcfg.get("mode")
    ctx = {"bundle.name": name, "bundle.target": target, "workspace.current_user.userName": USER}
    variables = {k: (v or {}).get("default") for k, v in (cfg.get("variables") or {}).items()}
    variables.update(tcfg.get("variables") or {})
    ctx.update({f"var.{k}": v for k, v in variables.items()})
    root_path = _subst((tcfg.get("workspace") or {}).get(
        "root_path", "/Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}"), ctx)
    file_path = root_path + "/files"
    synced = []
    for p in sorted(root.rglob("*")):
        if p.is_file():
            rel = p.relative_to(root).as_posix()
            ws = f"{file_path}/{rel[:-3] if _is_notebook(p) else rel}"
            WORKSPACES[target].add(ws)
            synced.append(ws)

    def translate(ref, src):
        if ref.startswith("/"):
            return ref                                    # left as is: whatever is (or is not) at that path
        local = (src.parent / ref).resolve()
        rel = local.relative_to(root.resolve()).as_posix()
        return f"{file_path}/{rel[:-3] if _is_notebook(local) else rel}"

    prefix = f"[dev {USER.split('@')[0]}] " if mode == "development" else ""
    payloads = {}
    for key, (job, src) in bundle["resources"].get("jobs", {}).items():
        job = json.loads(_subst(json.dumps(job), ctx))
        job["tasks"] = [{k: v for k, v in t.items() if k in KNOWN_TASK} for t in job.get("tasks", [])]  # unknown fields are dropped
        for t in job["tasks"]:
            if "notebook_task" in t:
                t["notebook_task"]["notebook_path"] = translate(t["notebook_task"]["notebook_path"], src)
        job["name"] = prefix + job.get("name", key)
        payloads[f"jobs.{key}"] = job
    for key, (pl, src) in bundle["resources"].get("pipelines", {}).items():
        pl = json.loads(_subst(json.dumps(pl), ctx))
        for lib in pl.get("libraries", []):
            for kind in ("notebook", "file"):
                if kind in lib:
                    lib[kind]["path"] = translate(lib[kind]["path"], src)
        pl["name"] = prefix + pl.get("name", key)
        if mode == "development":
            pl["development"] = True
        payloads[f"pipelines.{key}"] = pl
    return {"target": target, "mode": mode, "root_path": root_path, "synced": synced, "resources": payloads}

def bundle_run(deployment, job_key: str):
    """Check every task's code path exists in the target workspace, like the job run would."""
    ws = WORKSPACES[deployment["target"]]
    job = deployment["resources"][f"jobs.{job_key}"]
    for t in job["tasks"]:
        path = t.get("notebook_task", {}).get("notebook_path")
        if path and path not in ws:
            return f"FAILED task {t['task_key']}: Unable to access the notebook \"{path}\". It does not exist in this workspace."
    return f"SUCCESS: {job['name']} ran {[t['task_key'] for t in job['tasks']]}"

def show_deploy(d):
    print(f"target={d['target']} mode={d['mode']} root_path={d['root_path']}")
    print(f"uploaded {len(d['synced'])} files:")
    for s in d["synced"]:
        print("   ", s)
    print("resource payloads sent to the Jobs / Pipelines APIs:")
    print(json.dumps(d["resources"], indent=1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Option C, run first: one root file that names the bundle, its targets and its resources
# MAGIC
# MAGIC This is the shape `databricks bundle init default-python` produces, trimmed to the parts that matter:
# MAGIC
# MAGIC | file | role in the lifecycle |
# MAGIC |---|---|
# MAGIC | `databricks.yml` | **required.** `bundle.name`, `targets` (one per environment: `dev`, `prod`, each with its own workspace and mode), and the resource definitions, inline or pulled in with `include` |
# MAGIC | `resources/*.yml` | optional extra YAML: a Lakeflow Jobs definition and a Lakeflow Spark Declarative Pipelines definition, referenced from the root through `include: [resources/*.yml]` |
# MAGIC | `src/*.py` | optional source: the notebook the job runs and the pipeline's transformation file, referenced by **relative path** from the YAML |
# MAGIC
# MAGIC The template also writes conveniences such as a `README.md`, a `tests/` folder and Python packaging files; none
# MAGIC of them is what makes the folder a bundle.

# COMMAND ----------

# DBTITLE 1,Write option C's project tree
C_FILES = {
    "databricks.yml": """
        bundle:
          name: orders_etl

        include:
          - resources/*.yml

        variables:
          catalog:
            description: Catalog the pipeline publishes to
            default: dev_catalog

        targets:
          dev:
            mode: development
            default: true
            workspace:
              host: https://dev-workspace.example.cloud.databricks.com
          prod:
            mode: production
            workspace:
              host: https://prod-workspace.example.cloud.databricks.com
              root_path: /Workspace/Shared/.bundle/${bundle.name}/${bundle.target}
            variables:
              catalog: prod_catalog
        """,
    "resources/orders_etl.job.yml": """
        resources:
          jobs:
            orders_etl_job:
              name: orders_etl_job
              tasks:
                - task_key: ingest
                  notebook_task:
                    notebook_path: ../src/ingest.py
                - task_key: refresh_pipeline
                  depends_on:
                    - task_key: ingest
                  pipeline_task:
                    pipeline_id: ${resources.pipelines.orders_pipeline.id}
        """,
    "resources/orders_etl.pipeline.yml": """
        resources:
          pipelines:
            orders_pipeline:
              name: orders_pipeline
              catalog: ${var.catalog}
              schema: orders
              serverless: true
              libraries:
                - file:
                    path: ../src/transform.py
        """,
    "src/ingest.py": """
        # Databricks notebook source
        print("land the day's order files")
        """,
    "src/transform.py": """
        from pyspark import pipelines as dp

        @dp.table
        def orders_silver():
            return spark.readStream.table("orders_bronze").where("amount > 0")
        """,
}
root_c = write_project("option_c", C_FILES)
show_tree(root_c)
print("\n--- databricks.yml ---")
print((root_c / "databricks.yml").read_text())

# COMMAND ----------

# DBTITLE 1,databricks bundle validate -t dev   (stand-in)
bundle_c, warn_c = bundle_validate(root_c)

# COMMAND ----------

# DBTITLE 1,databricks bundle deploy -t dev, then -t prod   (stand-in)
dev_c = bundle_deploy(bundle_c, "dev")
show_deploy(dev_c)
print("\n================ prod ================")
prod_c = bundle_deploy(bundle_c, "prod")
show_deploy(prod_c)

# COMMAND ----------

# DBTITLE 1,databricks bundle run -t prod orders_etl_job   (stand-in), in a workspace that had nothing before
print(bundle_run(prod_c, "orders_etl_job"))
RESULTS["C"] = {"validate": "OK", "prod run": "SUCCESS", "note": "one root file; code shipped with the bundle"}

# COMMAND ----------

# MAGIC %md
# MAGIC What the output shows:
# MAGIC
# MAGIC * `validate` loaded `databricks.yml` first and then the two files its `include` glob matched; the job and the
# MAGIC   pipeline exist only because the root pointed at them. Each relative path (`../src/ingest.py`) was resolved
# MAGIC   against the YAML file that contains it and checked to exist locally.
# MAGIC * `deploy` uploaded the **local project tree** to the target's `root_path/files` and rewrote every relative path
# MAGIC   to the uploaded copy. The same source produced two deployments: in `dev` (`mode: development`) the resources
# MAGIC   are prefixed `[dev <user>]` and the pipeline runs in development mode under your home folder; in `prod` the
# MAGIC   names are clean, the files go to `/Workspace/Shared/.bundle/...`, and `${var.catalog}` became `prod_catalog`.
# MAGIC   (`${resources.pipelines.orders_pipeline.id}` is left as written by the stand-in; the real deploy creates the
# MAGIC   pipeline first and substitutes its ID, which is how resources in one bundle reference each other.)
# MAGIC * `run` succeeded in a production workspace that had nothing in it before the deploy. That is the CI/CD point
# MAGIC   of a bundle: the Git commit is the complete, versioned definition of code plus jobs plus pipelines, and
# MAGIC   `databricks bundle deploy -t <target>` reproduces it in any environment.
# MAGIC
# MAGIC So C is right: `databricks.yml` is the mandatory anchor (bundle name, targets, resource definitions), and the
# MAGIC notebooks, `.py` files and sub-YAML resource files are optional, live in the project directory and are
# MAGIC referenced from that root.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Option A: the right root file, but code "resolved by path at run time" from the workspace
# MAGIC
# MAGIC A gets `databricks.yml` right and then breaks the other half of the contract. If the job points at a notebook
# MAGIC already sitting in someone's workspace folder, the bundle does not carry that code: nothing is deployed for
# MAGIC it, and the run depends on whatever happens to be at that path in that workspace. Below, the notebook was
# MAGIC uploaded by hand to the **dev** workspace only.

# COMMAND ----------

# DBTITLE 1,Option A: notebook_path points into the workspace, src/ is not in the project
HAND_COPY = f"/Workspace/Users/{USER}/etl/ingest"
WORKSPACES["dev"].add(HAND_COPY)            # someone uploaded it to dev by hand once
A_FILES = {
    "databricks.yml": C_FILES["databricks.yml"].replace("include:\n          - resources/*.yml\n", "") + f"""
        resources:
          jobs:
            orders_etl_job:
              name: orders_etl_job
              tasks:
                - task_key: ingest
                  notebook_task:
                    notebook_path: {HAND_COPY}
        """,
}
root_a = write_project("option_a", A_FILES)
show_tree(root_a)
bundle_a, _ = bundle_validate(root_a)
dev_a, prod_a = bundle_deploy(bundle_a, "dev"), bundle_deploy(bundle_a, "prod")
print("\nfiles uploaded to prod:", prod_a["synced"])
print("prod job notebook_path:", prod_a["resources"]["jobs.orders_etl_job"]["tasks"][0]["notebook_task"]["notebook_path"])
print("\ndev  run:", bundle_run(dev_a, "orders_etl_job"))
print("prod run:", bundle_run(prod_a, "orders_etl_job"))

# COMMAND ----------

# DBTITLE 1,Option A's other variant: a relative path to source that is not in the local tree
A2_FILES = {"databricks.yml": A_FILES["databricks.yml"].replace(HAND_COPY, "src/ingest.py")}
root_a2 = write_project("option_a_relative", A2_FILES)
try:
    bundle_validate(root_a2)
except BundleError as e:
    print("Error:", e)
RESULTS["A"] = {"validate": "OK with warning", "prod run": "FAILED", "note": "code not in the bundle; works only where someone uploaded it"}

# COMMAND ----------

# MAGIC %md
# MAGIC **Why A is wrong.** Treating notebooks and scripts as workspace files resolved dynamically at run time
# MAGIC misrepresents how bundles work: the source files must be present locally in the project so the bundle can
# MAGIC deploy them. With a workspace path, `deploy` uploads only `databricks.yml` and leaves the path untouched, the
# MAGIC job runs in dev (where the hand copy exists) and fails in prod. With a relative path to a file that is not in
# MAGIC the tree, `validate` stops before anything is deployed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Option B: `requirements.txt` and a `Makefile` as required components
# MAGIC
# MAGIC B claims `databricks bundle validate` rejects a project that lacks either file. Option C's project has
# MAGIC neither, and it validated, deployed and ran in section 2. Adding them changes nothing about validation: they
# MAGIC are ordinary project files that `deploy` uploads like any other.

# COMMAND ----------

# DBTITLE 1,Option B: validate with and without requirements.txt and a Makefile
print("C's tree has requirements.txt:", (root_c / "requirements.txt").exists(), "| Makefile:", (root_c / "Makefile").exists())
_, w = bundle_validate(root_c, quiet=True)
print("validate without them -> OK, warnings:", w)

B_FILES = dict(C_FILES, **{
    "requirements.txt": "pyyaml==6.0.2\n",
    "Makefile": "deploy:\n\tdatabricks bundle deploy -t prod\n",
})
root_b = write_project("option_b", B_FILES)
bundle_b, w = bundle_validate(root_b, quiet=True)
print("validate with them    -> OK, warnings:", w)
print("they are just uploaded files:", [s for s in bundle_deploy(bundle_b, "dev")["synced"] if s.endswith(("requirements.txt", "Makefile"))])

print("\nNow delete databricks.yml from the same tree, keeping requirements.txt and the Makefile:")
(root_b / "databricks.yml").unlink()
try:
    bundle_validate(root_b)
except BundleError as e:
    print("Error:", e)
RESULTS["B"] = {"validate": "OK without them", "prod run": "n/a", "note": "conventions, not bundle components"}

# COMMAND ----------

# MAGIC %md
# MAGIC **Why B is wrong.** `requirements.txt` and a `Makefile` are common project conventions, not mandatory
# MAGIC components of an Asset Bundle, and validation does not require them. The only file whose absence fails
# MAGIC validation is `databricks.yml` (last line above). Python dependencies for a bundle's jobs are declared in the
# MAGIC YAML (task `libraries` or a job `environments` block, or a wheel built through `artifacts`), and the CLI, not
# MAGIC `make`, drives the deploy.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Option D: a `bundle_manifest.json` with hashes and a `clusters/` directory of JSON files
# MAGIC
# MAGIC Neither exists in the Asset Bundle specification. Build them anyway and watch what the CLI does with them:
# MAGIC nothing. The manifest is never read, and a job cannot "refer to" a cluster JSON file, so the cluster the author
# MAGIC wanted is silently not used.

# COMMAND ----------

# DBTITLE 1,Option D: the manifest and clusters/ are ignored; the field pointing at them is unknown
import hashlib
D_FILES = {
    "databricks.yml": C_FILES["databricks.yml"].replace("include:\n          - resources/*.yml\n", "manifest: bundle_manifest.json\n") + """
        resources:
          jobs:
            orders_etl_job:
              name: orders_etl_job
              tasks:
                - task_key: ingest
                  cluster_file: clusters/etl_cluster.json
                  notebook_task:
                    notebook_path: src/ingest.py
        """,
    "clusters/etl_cluster.json": json.dumps({"spark_version": "16.4.x-scala2.12", "node_type_id": "i3.xlarge", "num_workers": 2}, indent=1),
    "src/ingest.py": C_FILES["src/ingest.py"],
}
D_FILES["bundle_manifest.json"] = json.dumps({rel: hashlib.sha256(t.encode()).hexdigest() for rel, t in D_FILES.items()}, indent=1)
root_d = write_project("option_d", D_FILES)
show_tree(root_d)
bundle_d, _ = bundle_validate(root_d)
task = bundle_deploy(bundle_d, "prod")["resources"]["jobs.orders_etl_job"]["tasks"][0]
print("\ncompute in the deployed task:", {k: task.get(k) for k in ("cluster_file", "job_cluster_key", "new_cluster")})

# COMMAND ----------

# DBTITLE 1,Where a cluster really lives: inside the job's YAML resource definition
D_FIXED = {
    "resources/orders_etl.job.yml": """
        resources:
          jobs:
            orders_etl_job:
              name: orders_etl_job
              job_clusters:
                - job_cluster_key: etl_cluster
                  new_cluster:
                    spark_version: 16.4.x-scala2.12
                    node_type_id: i3.xlarge
                    num_workers: 2
              tasks:
                - task_key: ingest
                  job_cluster_key: etl_cluster
                  notebook_task:
                    notebook_path: ../src/ingest.py
        """,
}
root_d2 = write_project("option_d_fixed", dict(C_FILES, **D_FIXED))
bundle_d2, w = bundle_validate(root_d2, quiet=True)
job = bundle_deploy(bundle_d2, "prod")["resources"]["jobs.orders_etl_job"]
print("warnings:", w)
print("job_clusters:", job["job_clusters"])
print("task compute:", job["tasks"][0]["job_cluster_key"])
RESULTS["D"] = {"validate": "OK with warnings", "prod run": "wrong compute", "note": "no manifest or clusters/ in the spec; manifest never read"}

# COMMAND ----------

# MAGIC %md
# MAGIC **Why D is wrong.** There is no `bundle_manifest.json` and no required `clusters/` directory in the Asset
# MAGIC Bundle specification. The real CLI prints `Warning: unknown field: ...` for keys it does not know and carries
# MAGIC on, so the hashes are never checked and the task deploys with no compute from that file. Cluster definitions
# MAGIC are embedded in the job or pipeline YAML resource configs (`job_clusters` / `new_cluster` on a job, `clusters`
# MAGIC on a pipeline), as the second cell shows. Change tracking is Git's job, and the CLI keeps its own deployment
# MAGIC state in the workspace.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Option E: `pipeline_spec.json` and `jobs_spec.json` linked from `databricks.yml`
# MAGIC
# MAGIC E keeps `databricks.yml` but moves the Lakeflow Spark Declarative Pipelines and Lakeflow Jobs definitions into
# MAGIC separate JSON spec files. In a bundle, every resource is a map under `resources.<type>.<key>` in YAML; a file
# MAGIC name where that map belongs is a type error.

# COMMAND ----------

# DBTITLE 1,Option E: resources pointing at JSON spec files
E_FILES = {
    "databricks.yml": C_FILES["databricks.yml"].replace("include:\n          - resources/*.yml\n", "") + """
        resources:
          pipelines: pipeline_spec.json
          jobs: jobs_spec.json
        """,
    "pipeline_spec.json": json.dumps({"name": "orders_pipeline", "libraries": [{"file": {"path": "src/transform.py"}}]}, indent=1),
    "jobs_spec.json": json.dumps({"name": "orders_etl_job", "tasks": [{"task_key": "ingest", "notebook_task": {"notebook_path": "src/ingest.py"}}]}, indent=1),
    "src/ingest.py": C_FILES["src/ingest.py"],
    "src/transform.py": C_FILES["src/transform.py"],
}
root_e = write_project("option_e", E_FILES)
show_tree(root_e)
try:
    bundle_validate(root_e)
except BundleError as e:
    print("Error:", e)
RESULTS["E"] = {"validate": "FAILED", "prod run": "never deployed", "note": "resources are YAML maps, not JSON spec files"}

# COMMAND ----------

# DBTITLE 1,The same pipeline and job as YAML resources: every setting fits
print(textwrap.dedent(C_FILES["resources/orders_etl.pipeline.yml"]).strip())
print(textwrap.dedent(D_FIXED["resources/orders_etl.job.yml"]).strip())
print("\nC's resources after deploy:", list(prod_c["resources"]), "| D-fixed adds job_clusters:", "job_clusters" in job)

# COMMAND ----------

# MAGIC %md
# MAGIC **Why E is wrong.** Lakeflow Spark Declarative Pipelines (formerly Delta Live Tables) and Lakeflow Jobs are
# MAGIC defined within YAML resource files, not separate JSON spec files. The YAML schema is fully expressive for all
# MAGIC resource types: tasks, dependencies, job clusters, schedules and triggers for a job; catalog, schema,
# MAGIC serverless, libraries and channel for a pipeline. To split definitions across files you add more YAML under
# MAGIC `resources/` and list them in `include`, which is exactly option C.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. The five options side by side

# COMMAND ----------

# DBTITLE 1,What each option's project does under validate / deploy / run
print(f"{'option':7s} {'validate':18s} {'prod run':16s} note")
for letter in ["C", "A", "B", "D", "E"]:
    r = RESULTS[letter]
    print(f"{letter:7s} {r['validate']:18s} {r['prod run']:16s} {r['note']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. The rule to memorise
# MAGIC
# MAGIC An Asset Bundle has exactly one required file: `databricks.yml` at the project root, which declares the
# MAGIC bundle name, the targets (one per environment, each mapping to a workspace and a mode) and the resource
# MAGIC definitions, inline or through `include` of more YAML. Notebooks, `.py` files and sub-YAML resource files are
# MAGIC optional, live in the project tree and are referenced by relative path from that root, so
# MAGIC `databricks bundle deploy -t <target>` ships the code together with the jobs and pipelines that run it.
# MAGIC Jobs, pipelines and their clusters are YAML, never JSON spec files, manifests or a `clusters/` folder, and
# MAGIC `requirements.txt` or a `Makefile` are conventions the CLI does not need.
