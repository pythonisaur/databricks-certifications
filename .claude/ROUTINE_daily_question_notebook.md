# Cloud routine: "databricks-certifications: one tutorial notebook per fixed question daily"

Routine id `trig_01Y1AhetuUyFsxYwfvtNW77H` (https://claude.ai/code/routines/trig_01Y1AhetuUyFsxYwfvtNW77H), created 2026-11-07.
Runs daily at 13:00 UTC (7:00 AM Chicago), one hour after the TestLogicHub question-fix routine
(`trig_01WLLy6xSS9ey1TMDhqkeQt9`, 12:00 UTC) so the question fixed that morning is already on main.
Model claude-opus-5-5, environment DataLogicHub, two git sources: this repo (written) and
byambaa1982/testlogichub (read-only, the question bank), tools Bash/Read/Write/Edit/Glob/Grep, no connectors.

Each run turns ONE already-fixed exam question (a `testlogichub/exams/fixes/qfix/<key>.py` module) into one
runnable Databricks notebook `<track>/<NN-section>/q_<key>_<slug>.py`, updates that section's README, and
opens a PR on branch `qnote/<key>`. Nobody reviews it by hand: `.github/workflows/qnote-automerge.yml` runs
`tools/qnote_check.py` on the PR and squash-merges it. A PR that fails the gate stays open with a comment.

Where things are:
- `tools/qnote_check.py` — the gate (also run by the routine before it pushes).
- `tools/section_map.json` — TestLogicHub track id + `exam_section` value -> folders in this repo.
- Notebook convention: `# Databricks notebook source`, title cell with the exam question, a `QUESTION = {...}`
  metadata cell (key, track, section, exact stem, correct, options), the correct option run first, every
  wrong option run to the output it really produces, closing "rule to memorise". Keys are the first 10 hex
  chars of sha1 of the normalised stem, the same key TestLogicHub uses.

Changelog:
- 2026-11-07 created (disabled, placeholder prompt) while the workflow + gate were added; prompt stored and
  routine enabled the same day.

The prompt as currently stored:

---

You write tutorial notebooks for the databricks-certifications repository (pythonisaur/databricks-certifications): runnable Databricks notebooks, one folder per certification exam, one subfolder per official exam-guide section. Every day you turn EXACTLY ONE already-fixed TestLogicHub practice question into ONE notebook and open ONE pull request. Nothing else. You start with zero context, so follow this brief exactly.

TWO REPOSITORIES ARE CHECKED OUT
- `databricks-certifications` is the repository you change. Work from its root.
- `testlogichub` (byambaa1982/testlogichub) is the question bank. It is READ-ONLY for you: never commit, branch or push there. Find it with `ls ..` or `find / -maxdepth 4 -type d -name testlogichub 2>/dev/null`; if it is not checked out, stop and report.

WHAT HAPPENS AFTER YOU OPEN THE PR
Nobody reviews it by hand. `.github/workflows/qnote-automerge.yml` runs `python tools/qnote_check.py --key <key>` on the PR and squash-merges it when the gate passes; a PR that fails the gate stays open with a comment. So your job ends at a PR that passes `tools/qnote_check.py` locally (STEP 5). Read the docstring of `tools/qnote_check.py` first: it lists every rule the gate enforces.

STEP 0 - learn the house style
Read `README.md`, the README of the track you will write for, and two existing question notebooks end to end: `generative-ai-engineer-associate/01-design-applications/01_formatted_response.py` and `generative-ai-engineer-associate/02-data-preparation/lesson03_chunking_record_count.py`. Note the shape: `# Databricks notebook source` first line, the `# /// script` environment block, a `# MAGIC %md` title cell that states the exam question with its options, `%pip` then `dbutils.library.restartPython()` when packages are needed, endpoint widgets with Free Edition defaults, `# DBTITLE 1,<title>` on code cells, numbered markdown sections, the correct option built and run FIRST, then every wrong option run to the output it really produces, and a closing "rule to memorise". Cells are separated by a line `# COMMAND ----------`.

STEP 1 - pick the one question
- List `testlogichub/exams/fixes/qfix/*.py`. Each file name is a question key (10 hex chars). Its docstring names `Track:` and `Date:`.
- Skip a key when a notebook for it already exists (`git ls-files '*/q_<key>_*.py'` prints something) or a remote branch `qnote/<key>` exists (`git ls-remote --heads origin 'qnote/*'`), or when its track is not in `tools/section_map.json`.
- Of the remaining keys take the most recent `Date:` (ties: alphabetical key). If none remain, print "nothing left to write" and stop without a PR.
- Load the question: `exams/tracks/<track>/builds/combined_v<N>.json` with the highest N in testlogichub, the entry whose `question_text` starts with the module's `FIXES[0][0]` prefix. Keep every field: `question_text` (the stem, copy it EXACTLY, the gate hashes it), `option_a`..`option_e` (only the letters present), `correct_answer`, `explanation` (WHY / WHY NOT per letter), `exam_section`, `objective`.
- Folder: `tools/section_map.json` maps `<track>` to the track folder and `exam_section` to the `NN-section` folder. The notebook goes to `<track folder>/<NN-section>/q_<key>_<slug>.py` with a 2-4 word snake_case slug of the topic.

STEP 2 - write the notebook (you are the instructor)
Rules, all mandatory:
1. Runnable top to bottom on serverless compute or Free Edition. Build any practice data inside the notebook, small and deterministic (hash-based, no randomness without a seed). Create nothing outside `dev_catalog.<track>_qnotes` or temp views; no `DROP CATALOG`/`DROP SCHEMA`, no `dbutils.fs.rm` outside `/tmp`, no credentials, no `dbutils.secrets`.
2. Platform calls that Free Edition cannot run (model serving deploys, Vector Search endpoints, Lakehouse Monitoring, Jobs API against a real workspace) appear as the exact annotated API call AND a local stand-in that demonstrates the behaviour (a stubbed LLM, a pandas table standing for the inference table, a dict standing for the REST response), so the lesson still runs.
3. Structure: title cell (title, which exam section and objective, how to run, then `**The exam question this lesson answers**`, the stem, the options as `* **A** ...` bullets, and one sentence saying the correct option is built and run first); the metadata cell (rule 4); setup; `## 1.` setup data; `## 2.` the correct option run first with the output explained; one `##` section per wrong option, each run to its real output or failure, naming the misconception from the question's WHY NOT text; last section `## N. The rule to memorise`, three to five sentences. 12 to 40 cells.
4. The metadata cell, immediately after the title cell, is a code cell titled `# DBTITLE 1,Question metadata` containing a dict literal starting at column 0: `QUESTION = {` ... `}` with keys `key`, `track`, `section` (the exact `exam_section` value), `stem` (the exact `question_text`), `correct`, `options` (dict letter -> option text), `objective`, `source` (`testlogichub exams/fixes/qfix/<key>.py`). Generate it with `json.dumps(meta, indent=4, ensure_ascii=False)` so it stays a valid Python literal; close with `}` alone on a line.
5. Every fact in the question's explanation must appear in the notebook, as a demonstration where it can run and as prose where it cannot. Never contradict the correct letter. Current Databricks terminology: Lakeflow Spark Declarative Pipelines (not Delta Live Tables), Lakeflow Jobs (not Workflows), AUTO CDC (not APPLY CHANGES), Asset Bundles.
6. Discuss every option letter by its letter somewhere in the markdown.
7. The file must compile: `python -m py_compile <notebook>`.

STEP 3 - update the READMEs
- Add one row for the notebook to the run-order table of `<track folder>/<NN-section>/README.md`, matching that table's existing columns exactly (tables differ per section; read it first). If the section README has no table, add a short "Question notebooks" list with the file name, objective and one line on what runs.
- If `<track folder>/README.md` shows a per-section notebook count, increase it by one. Touch no other file.

STEP 4 - verify
From the repository root: `python -m py_compile <notebook>` then `python tools/qnote_check.py --key <key> --base origin/main --head HEAD` AFTER committing (STEP 5 commit first, then run the gate, then push). It must print a line starting with `qnote <key> OK`. If it prints `QNOTE CHECK FAILED`, fix the cause it names, amend the commit and re-run; after three failures stop with a report and no PR.

STEP 5 - the pull request
- Branch `qnote/<key>` from main. One commit: `qnote(<track>): <notebook title, at most 60 characters>` with a body naming the section folder and the question key, ending with the line `Co-Authored-By: Claude <noreply@anthropic.com>`.
- Run the STEP 4 gate on the commit. Then push and open the PR with `gh pr create --base main`. If `gh` is unavailable, push the branch and print its compare URL.
- PR title = the commit title. PR body, in this order: track, section folder and objective; the stem and the options with the correct letter marked; a cell-by-cell outline of what runs; the `qnote_check.py` output line; then this closing note: `Merged automatically by qnote-automerge.yml once tools/qnote_check.py passes. If this PR is still open, the gate failed; see the workflow comment below.` End the body with: Generated with [Claude Code](https://claude.com/claude-code)

HARD LIMITS
- One notebook per run. Never two, even if a second is trivial.
- Never modify or delete an existing notebook or any file other than the new notebook and the two READMEs named in STEP 3. Never touch `.github/`, `tools/`, `resources/`, `videos/`.
- Never commit, branch or push in testlogichub. Never push to main. Never force-push. No `pip install` outside a notebook cell, no deployments, no credentials of any kind.
- If anything blocks you (testlogichub missing, section not mapped, gate failing three times, push refused), stop and print a short report of what happened instead of improvising around it.
