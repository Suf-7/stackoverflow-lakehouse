"""Save the notebooks of a Databricks job run, WITH their outputs, as Jupyter .ipynb files.

Databricks exports a job run only as HTML, which GitHub shows as raw code. This
script exports every task of a run, decodes the notebook model inside the HTML
(cells, printed output and displayed tables) and writes a standard Jupyter
notebook that GitHub renders with its outputs. Email addresses are redacted.

    python scripts/export_databricks_run.py --run-id 530466994576723 --profile so-lakehouse --out evidence

Needs the Databricks CLI (https://docs.databricks.com/dev-tools/cli/) logged in with
`databricks auth login --host <workspace-url> --profile <name>`. Standard library only.
"""
import argparse
import base64
import html
import json
import os
import re
import subprocess
import urllib.parse
from datetime import datetime, timezone

EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
MAX_TABLE_ROWS = 300


def cli(args, profile, binary):
    out = subprocess.check_output([binary, "--profile", profile] + args + ["-o", "json"])
    return json.loads(out)


def redact(text):
    return EMAIL.sub("<your-email>", text) if isinstance(text, str) else text


def notebook_model(export_html):
    m = re.search(r"__DATABRICKS_NOTEBOOK_MODEL = '([^']+)'", export_html)
    if not m:
        raise ValueError("no notebook model in the exported HTML")
    return json.loads(urllib.parse.unquote(base64.b64decode(m.group(1)).decode()))


def cell_source(text):
    return [line + "\n" for line in text.split("\n")[:-1]] + [text.split("\n")[-1]] if text else []


def table_output(item):
    cols = [c["name"] for c in item.get("schema") or []]
    rows = item.get("data") or []
    shown = rows[:MAX_TABLE_ROWS]
    head = "".join(f"<th>{html.escape(c)}</th>" for c in cols)
    body = "".join("<tr>" + "".join(f"<td>{html.escape(redact('' if v is None else str(v)))}</td>" for v in r)
                   + "</tr>" for r in shown)
    note = (f"<p><em>Showing {len(shown)} of {len(rows)} rows.</em></p>" if len(rows) > len(shown) else
            f"<p><em>{len(rows)} row{'s' if len(rows) != 1 else ''}</em></p>")
    if item.get("overflow"):
        note = "<p><em>Databricks truncated this result.</em></p>" + note
    html_table = f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>{note}"
    widths = [max([len(c)] + [len(str(r[i])) for r in shown[:50] if i < len(r)]) for i, c in enumerate(cols)]
    widths = [min(w, 40) for w in widths]
    line = lambda vals: " | ".join(str(v)[:w].ljust(w) for v, w in zip(vals, widths))  # noqa: E731
    plain = "\n".join([line(cols), "-+-".join("-" * w for w in widths)] +
                      [line(["" if v is None else redact(str(v)) for v in r]) for r in shown[:50]])
    return {"output_type": "display_data", "metadata": {},
            "data": {"text/html": html_table, "text/plain": plain}}


def outputs_for(cmd):
    outs = []
    results = cmd.get("results") or {}
    for item in results.get("data") or []:
        kind = item.get("type")
        if kind == "table":
            outs.append(table_output(item))
        elif kind in ("ansi", "text"):
            text = redact(ANSI.sub("", item.get("data") or ""))
            if text:
                outs.append({"output_type": "stream", "name": item.get("name") or "stdout", "text": text})
        elif kind == "html":
            outs.append({"output_type": "display_data", "metadata": {},
                         "data": {"text/html": redact(item.get("data") or "")}})
    if cmd.get("error") or cmd.get("errorSummary"):
        outs.append({"output_type": "error", "ename": "Error",
                     "evalue": redact(ANSI.sub("", str(cmd.get("errorSummary") or "")))[:2000],
                     "traceback": [redact(ANSI.sub("", str(cmd.get("error") or "")))[:4000]]})
    return outs


def to_ipynb(model, header):
    cells = [{"cell_type": "markdown", "metadata": {}, "source": cell_source(header)}]
    count = 0
    for cmd in sorted(model["commands"], key=lambda c: c.get("position", 0)):
        src = redact(cmd.get("command") or "")
        if not src.strip():
            # output of an expanded %run lands in a sub-command with no source: attach it to the cell above
            if cells and cells[-1]["cell_type"] == "code":
                cells[-1]["outputs"].extend(outputs_for(cmd))
            continue
        if src.lstrip().startswith("%md"):
            md = re.sub(r"^\s*%md[ \t]*\n?", "", src, count=1)
            cells.append({"cell_type": "markdown", "metadata": {}, "source": cell_source(md)})
            continue
        count += 1
        cells.append({"cell_type": "code", "execution_count": count, "metadata": {},
                      "source": cell_source(src), "outputs": outputs_for(cmd)})
    for i, cell in enumerate(cells):  # nbformat 4.5 requires a unique id per cell
        cell["id"] = f"cell-{i:03d}"
    return {"nbformat": 4, "nbformat_minor": 5, "cells": cells,
            "metadata": {"kernelspec": {"name": "python3", "display_name": "Python 3", "language": "python"},
                         "language_info": {"name": "python"},
                         "databricks": {"notebook": redact(model.get("name"))}}}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--profile", default="DEFAULT")
    ap.add_argument("--out", default="evidence")
    ap.add_argument("--cli", default=os.environ.get("DATABRICKS_CLI", "databricks"))
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)

    run = cli(["jobs", "get-run", str(args.run_id)], args.profile, args.cli)
    index = []
    for task in sorted(run["tasks"], key=lambda t: t["task_key"]):
        exported = cli(["jobs", "export-run", str(task["run_id"]), "--views-to-export", "CODE"],
                       args.profile, args.cli)
        model = notebook_model(exported["views"][0]["content"])
        params = task.get("notebook_task", {}).get("base_parameters", {})
        shown = {k: v for k, v in params.items() if v not in ("", None) and k != "pii_salt"}
        started = datetime.fromtimestamp(task["start_time"] / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        state = task.get("state", {}).get("result_state")
        notebook = redact(task["notebook_task"]["notebook_path"].split("/")[-1])
        header = (f"> **Executed on Databricks** (Free Edition, serverless compute, Unity Catalog).  \n"
                  f"> Job run `{run['run_id']}` · task `{task['task_key']}` · notebook `{notebook}` · "
                  f"started {started} · result **{state}**  \n"
                  f"> Parameters: " + (", ".join(f"`{k}={v}`" for k, v in shown.items()) or "defaults") + "\n\n"
                  "Saved with `scripts/export_databricks_run.py`: the cells and outputs below are exactly what "
                  "the run produced.")
        nb = to_ipynb(model, header)
        path = os.path.join(args.out, f"{task['task_key']}.ipynb")
        with open(path, "w") as f:
            json.dump(nb, f, indent=1, ensure_ascii=False)
        index.append((task["task_key"], notebook, shown, state, started))
        print(f"wrote {path} ({len(nb['cells'])} cells)")
    with open(os.path.join(args.out, "_run.json"), "w") as f:
        json.dump({"run_id": run["run_id"], "run_name": run.get("run_name"),
                   "result": run.get("state", {}).get("result_state"),
                   "tasks": [{"task_key": k, "notebook": n, "parameters": p, "result": s, "started": t}
                             for k, n, p, s, t in index]}, f, indent=2)


if __name__ == "__main__":
    main()
