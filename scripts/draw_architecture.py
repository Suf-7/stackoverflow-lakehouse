"""Draw the end-to-end architecture diagram used in the Phase 3 report.

    python scripts/draw_architecture.py            # writes docs/report_assets/architecture.png
"""
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "docs", "report_assets", "architecture.png")

INK, MUTED = "#1f1f1f", "#5a5a5a"
FILL = {"source": "#eef2f7", "raw": "#f3efe6", "bronze": "#f1e0cc", "silver": "#e4e7ea", "gold": "#f6e7b4",
        "bi": "#e2eedf", "ops": "#ece6f3", "job": "#ffffff"}


def box(ax, x, y, w, h, title, lines, kind):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.12",
                                fc=FILL[kind], ec="#8a8984", lw=0.9))
    ax.text(x + w / 2, y + h - 0.28, title, ha="center", va="top", fontsize=9.5, fontweight="bold", color=INK)
    ax.text(x + w / 2, y + h - 0.68, "\n".join(lines), ha="center", va="top", fontsize=7.1, color=MUTED,
            linespacing=1.35)


def arrow(ax, x1, y1, x2, y2, label=None, dashed=False, label_dy=0.16):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>", mutation_scale=11, lw=1.0,
                                 color="#444444", linestyle=(0, (3, 2)) if dashed else "-"))
    if label:
        ax.text((x1 + x2) / 2, (y1 + y2) / 2 + label_dy, label, ha="center", va="bottom", fontsize=6.6,
                color=MUTED)


def main(out=OUT):
    fig, ax = plt.subplots(figsize=(17, 7.6), dpi=200)
    ax.set_xlim(0, 37)
    ax.set_ylim(0, 16.5)
    ax.axis("off")

    w, h, y, step = 4.4, 4.1, 6.4, 5.15
    xs = [0.3 + i * step for i in range(7)]
    box(ax, xs[0], y, w, h, "Stack Exchange API",
        ["api.stackexchange.com/2.3", "18 data and analytics tags", "questions, answers,", "deletions, snapshots"],
        "source")
    box(ax, xs[1], y, w, h, "Ingestion (Python)",
        ["full_load.py: month windows", "incremental_load.py:", "watermark minus 1 h overlap",
         "backoff and quota guard"], "source")
    box(ax, xs[2], y, w, h, "Landing volume",
        ["so_raw.landing/", "full | incremental/<batch_id>/", "*.jsonl + _manifest.json"], "raw")
    box(ax, xs[3], y, w, h, "Bronze  so_bronze",
        ["task 01_raw_to_bronze", "explicit StructType", "replaceWhere on _batch_id", "drift: mergeSchema",
         "bad rows to quarantine"], "bronze")
    box(ax, xs[4], y, w, h, "Silver  so_silver",
        ["task 02_bronze_to_silver", "epochs cast to TIMESTAMP", "PII: drop, salted hash, mask",
         "MERGE INTO, newer wins", "soft deletes"], "silver")
    box(ax, xs[5], y, w, h, "Gold  so_gold",
        ["task 05_silver_to_gold", "2 facts, 3 dims, 1 bridge,", "3 aggregates", "MERGE on record_hash",
         "liquid clustering, OPTIMIZE"], "gold")
    box(ax, xs[6], y, w + 0.4, h, "Power BI",
        ["Databricks connector", "serverless SQL warehouse", "12 tables, 1:* relationships",
         "5 pages incl. audit"], "bi")

    mid = y + h / 2
    for i in range(6):
        arrow(ax, xs[i] + w + 0.05, mid, xs[i + 1] - 0.05, mid)

    ox, ow = xs[3], xs[5] + w - xs[3]
    box(ax, ox, 0.5, ow, 3.6, "Operations  so_ops",
        ["pipeline_execution_logs: one row per file or table in every layer",
         "rows read, inserted, updated, deleted, quarantined; start, end, duration, status",
         "batch_registry: batch reached Bronze / Silver      schema_drift_events: drift and action taken"],
        "ops")
    for x in (xs[3], xs[4], xs[5]):
        arrow(ax, x + w / 2, y - 0.05, x + w / 2, 4.15, dashed=True)
    arrow(ax, ox + ow + 0.05, 2.3, xs[6] + 2.2, y - 0.05, dashed=True)
    ax.text(ox + ow + 1.1, 3.2, "audit page", fontsize=7, color=MUTED)

    jx, jw = xs[3], xs[5] + w - xs[3]
    ax.add_patch(FancyBboxPatch((jx, 11.5), jw, 2.9, boxstyle="round,pad=0.02,rounding_size=0.12",
                                fc=FILL["job"], ec="#8a8984", lw=0.9, linestyle=(0, (4, 2))))
    ax.text(jx + jw / 2, 14.15, "Lakeflow job  so_lakehouse_pipeline  (serverless)", ha="center",
            va="top", fontsize=9.5, fontweight="bold", color=INK)
    ax.text(jx + jw / 2, 13.5, "raw_to_bronze  >  bronze_to_silver  >  silver_to_gold\n"
            "each task waits for the previous one; 1 retry; 1 concurrent run\n"
            "parameters: mode, batch_ids, start_date, end_date, landing_path",
            ha="center", va="top", fontsize=7.4, color=MUTED, linespacing=1.45)
    for x in (xs[3], xs[4], xs[5]):
        arrow(ax, x + w / 2, 11.45, x + w / 2, y + h + 0.05, dashed=True)

    ax.text(0.3, 16.2, "Stack Overflow Developer Q&A Lakehouse: end-to-end data flow", fontsize=12,
            fontweight="bold", color=INK, va="top")
    ax.text(0.3, 15.5, "Unity Catalog catalog workspace on Databricks Free Edition. Solid arrows carry data; "
            "dashed arrows are orchestration and audit logging.", fontsize=8, color=MUTED, va="top")

    os.makedirs(os.path.dirname(out), exist_ok=True)
    fig.savefig(out, bbox_inches="tight", facecolor="white")
    print("wrote", out)


if __name__ == "__main__":
    main()
