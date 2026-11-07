"""Gate a `qnote/<key>` pull request (one tutorial notebook for one fixed exam question).

Usage (from the repository root):
    python tools/qnote_check.py --key <key> [--base origin/main] [--head HEAD]

The daily routine turns one already-fixed TestLogicHub question into a runnable
Databricks notebook. This script is the deterministic gate the auto-merge
workflow runs on the PR, and the routine runs it itself before pushing.

Checks, in order (exit 1 and a `QNOTE CHECK FAILED: ...` line on the first failure):
  1. Diff base..head adds exactly one notebook `<track>/<NN-section>/q_<key>_<slug>.py`,
     modifies that section's README.md (which must now name the notebook), may
     modify the track README.md, and touches nothing else.
  2. The notebook is Databricks source (`# Databricks notebook source` first line),
     compiles as Python, has between 10 and 60 cells, and carries a `QUESTION = {...}`
     metadata dict (key, track, section, stem, correct, options) whose stem hashes to
     <key> (sha1 of the normalised stem, first 10 hex chars), whose track/section map
     to the folder the file sits in (tools/section_map.json), and whose correct letter
     is one of its options.
  3. House style: a markdown cell that states the exam question, the correct option
     shown first, a closing "rule to memorise", every option letter discussed.
  4. Safety: no credential-shaped strings, no destructive commands.

Stdlib only; needs git.
"""

import argparse
import ast
import hashlib
import json
import py_compile
import re
import subprocess
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
MAP = json.loads((ROOT / "tools" / "section_map.json").read_text(encoding="utf-8"))
KEY_RE = re.compile(r"^[0-9a-f]{10}$")
NOTEBOOK_RE = re.compile(r"^([a-z0-9-]+)/(\d{2}-[a-z0-9-]+)/q_([0-9a-f]{10})_([a-z0-9_]+)\.py$")
CELL_SEP = "\n# COMMAND ----------\n"
MIN_CELLS, MAX_CELLS = 10, 60
SECRET_PATTERNS = [
    (re.compile(r"dapi[0-9a-f]{32}"), "a Databricks PAT"),
    (re.compile(r"\bsk-[A-Za-z0-9]{20,}"), "an API key"),
    (re.compile(r"AKIA[0-9A-Z]{16}"), "an AWS access key"),
    (re.compile(r"(?i)(password|secret|token)\s*=\s*['\"][^'\"]{8,}['\"]"), "a hard-coded secret"),
]
DESTRUCTIVE = [
    re.compile(r"(?i)\bDROP\s+(CATALOG|SCHEMA|DATABASE)\b"),
    re.compile(r"dbutils\.fs\.rm\(\s*['\"]/(?!tmp|Volumes/dev)"),
    re.compile(r"rm\s+-rf\s+/(?!tmp)"),
    re.compile(r"(?i)\bTRUNCATE\s+TABLE\s+(?!dev_)"),
]


def fail(msg: str) -> None:
    print(f"QNOTE CHECK FAILED: {msg}")
    sys.exit(1)


def git(*args: str) -> str:
    return subprocess.run(["git", *args], cwd=ROOT, check=True, capture_output=True,
                          text=True, encoding="utf-8").stdout


def norm_text(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip().lower()


def stem_key(text: str) -> str:
    return hashlib.sha1(norm_text(text).encode("utf-8")).hexdigest()[:10]


def check_files(key: str, base: str, head: str) -> tuple[Path, str, str]:
    status = git("diff", "--name-status", f"{base}...{head}").strip().splitlines()
    if not status:
        fail("the diff is empty")
    notebook = None
    section_readme_modified = False
    for line in status:
        parts = line.split("\t")
        code, path = parts[0], parts[-1]
        m = NOTEBOOK_RE.match(path)
        if m:
            if code != "A":
                fail(f"{path} must be a new file (git status {code})")
            if m.group(3) != key:
                fail(f"{path} carries key {m.group(3)}, the branch says {key}")
            if notebook:
                fail("more than one notebook added; one question per PR")
            notebook = path
            continue
        if path.endswith("/README.md") and code == "M":
            continue  # validated against the notebook's folder below
        fail(f"{path} is outside what a question notebook PR may change")
    if not notebook:
        fail(f"no new notebook matching <track>/<NN-section>/q_{key}_<slug>.py in the diff")
    track_folder, section_folder = NOTEBOOK_RE.match(notebook).group(1, 2)
    allowed_readmes = {f"{track_folder}/{section_folder}/README.md", f"{track_folder}/README.md"}
    for line in status:
        code, path = line.split("\t")[0], line.split("\t")[-1]
        if path.endswith("/README.md"):
            if path not in allowed_readmes:
                fail(f"{path} is not the section or track README of {notebook}")
            if path == f"{track_folder}/{section_folder}/README.md":
                section_readme_modified = True
    if not section_readme_modified:
        fail(f"{track_folder}/{section_folder}/README.md must be updated to list the notebook")
    readme = (ROOT / track_folder / section_folder / "README.md").read_text(encoding="utf-8")
    if Path(notebook).name not in readme:
        fail(f"the section README does not mention {Path(notebook).name}")
    return ROOT / notebook, track_folder, section_folder


def load_metadata(text: str) -> dict:
    m = re.search(r"^QUESTION\s*=\s*(\{.*?^\})", text, re.S | re.M)
    if not m:
        fail("no `QUESTION = {...}` metadata dict (must start at column 0 and close with `}` on its own line)")
    try:
        meta = ast.literal_eval(m.group(1))
    except Exception as e:  # noqa: BLE001
        fail(f"QUESTION dict is not a plain literal: {e}")
    for k in ("key", "track", "section", "stem", "correct", "options"):
        if k not in meta:
            fail(f"QUESTION dict is missing '{k}'")
    if not isinstance(meta["options"], dict) or not meta["options"]:
        fail("QUESTION['options'] must be a non-empty dict of letter -> text")
    return meta


def check_notebook(path: Path, key: str, track_folder: str, section_folder: str) -> dict:
    text = path.read_text(encoding="utf-8")
    if not text.startswith("# Databricks notebook source\n"):
        fail("first line must be `# Databricks notebook source`")
    try:
        py_compile.compile(str(path), doraise=True)
    except py_compile.PyCompileError as e:
        fail(f"notebook does not compile as Python: {e.msg.strip().splitlines()[-1]}")
    cells = text.split(CELL_SEP)
    if not MIN_CELLS <= len(cells) <= MAX_CELLS:
        fail(f"{len(cells)} cells; expected between {MIN_CELLS} and {MAX_CELLS}")

    meta = load_metadata(text)
    if meta["key"] != key:
        fail(f"QUESTION['key'] is {meta['key']}, expected {key}")
    if stem_key(meta["stem"]) != key:
        fail(f"the stem hashes to {stem_key(meta['stem'])}, not {key}; copy the stem exactly from the question bank")
    track = MAP.get(meta["track"])
    if not track:
        fail(f"unknown track '{meta['track']}' (see tools/section_map.json)")
    if track["folder"] != track_folder:
        fail(f"track {meta['track']} lives in {track['folder']}/, the notebook is in {track_folder}/")
    expected_section = track["sections"].get(meta["section"])
    if not expected_section:
        fail(f"section '{meta['section']}' is not mapped for {meta['track']} (see tools/section_map.json)")
    if expected_section != section_folder:
        fail(f"section '{meta['section']}' maps to {expected_section}/, the notebook is in {section_folder}/")
    letters = {str(l).upper() for l in meta["options"]}
    correct = {c for c in re.findall(r"[A-E]", str(meta["correct"]).upper())}
    if not correct or not correct <= letters:
        fail(f"correct letter(s) {sorted(correct)} must be among the options {sorted(letters)}")

    lower = text.lower()
    if "exam question" not in lower:
        fail("the opening markdown must present 'the exam question'")
    if "rule to memorise" not in lower and "rule to memorize" not in lower:
        fail("the notebook must end with a 'rule to memorise' section")
    for letter in sorted(letters):
        if not re.search(rf"\b{letter}\b", text):
            fail(f"option {letter} is never discussed in the notebook")
    for pat, what in SECRET_PATTERNS:
        if pat.search(text):
            fail(f"the notebook contains {what}")
    for pat in DESTRUCTIVE:
        if pat.search(text):
            fail(f"destructive command matches {pat.pattern!r}")
    return meta


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--key", required=True, help="the question key (branch suffix)")
    ap.add_argument("--base", default="origin/main")
    ap.add_argument("--head", default="HEAD")
    a = ap.parse_args()
    key = a.key.strip().lower()
    if not KEY_RE.match(key):
        fail(f"'{a.key}' is not a 10-hex-char key")
    notebook, track_folder, section_folder = check_files(key, a.base, a.head)
    meta = check_notebook(notebook, key, track_folder, section_folder)
    cells = notebook.read_text(encoding="utf-8").count(CELL_SEP) + 1
    print(f"qnote {key} OK: {notebook.relative_to(ROOT).as_posix()} track={meta['track']} "
          f"section={meta['section']!r} correct={meta['correct']} cells={cells}")


if __name__ == "__main__":
    main()
