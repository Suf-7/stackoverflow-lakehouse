"""Draw the Gold star-schema ER diagram used in the Phase 3 report.

    python scripts/draw_gold_er.py                 # writes docs/report_assets/gold_er.png
"""
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "docs", "report_assets", "gold_er.png")

FILL = {"fact": "#f6e7b4", "dim": "#e4ecf5", "bridge": "#f1e0cc", "agg": "#e2eedf"}
ROW = 0.42

TABLES = {
    "dim_date": ("dim", 0.4, 9.6, ["PK date_key (YYYYMMDD)", "full_date", "year, quarter, month", "year_month",
                                   "is_after_chatgpt"]),
    "dim_user": ("dim", 0.4, 4.6, ["PK user_key (salted hash)", "user_type", "reputation_band", "cohort_month"]),
    "fact_question": ("fact", 6.2, 8.6, ["PK question_id", "FK created_date_key", "FK owner_user_key", "score, views",
                                         "has_any_answer, is_closed", "hours_to_first_answer",
                                         "answered_within_24h"]),
    "fact_answer": ("fact", 6.2, 3.4, ["PK answer_id", "FK question_id", "FK created_date_key", "FK owner_user_key",
                                       "score, is_accepted", "is_first_answer"]),
    "bridge_question_tag": ("bridge", 12.0, 8.6, ["PK question_id, tag", "FK question_id", "FK tag", "is_tracked"]),
    "dim_tag": ("dim", 17.4, 8.6, ["PK tag", "category", "is_tracked"]),
    "agg_tag_month": ("agg", 17.4, 4.4, ["PK tag, month_start", "FK tag", "month_start = full_date", "questions",
                                         "share_of_tracked_questions", "pct_unanswered, median hours"]),
    "agg_tag_pairs": ("agg", 12.0, 5.2, ["PK tag_a, tag_b", "question_count"]),
    "agg_user_cohort": ("agg", 12.0, 2.9, ["PK cohort_month, months_since", "users_in_cohort",
                                           "share_still_active"]),
}
WIDTH = 4.4


def draw_table(ax, name, kind, x, top, fields):
    h = 0.55 + ROW * len(fields) + 0.15
    y = top - h
    ax.add_patch(FancyBboxPatch((x, y), WIDTH, h, boxstyle="round,pad=0.02,rounding_size=0.08",
                                fc="white", ec="#8a8984", lw=0.9))
    ax.add_patch(FancyBboxPatch((x, top - 0.55), WIDTH, 0.55, boxstyle="round,pad=0.02,rounding_size=0.08",
                                fc=FILL[kind], ec="#8a8984", lw=0.9))
    ax.text(x + 0.15, top - 0.28, name, va="center", fontsize=8.6, fontweight="bold", color="#1f1f1f")
    for i, f in enumerate(fields):
        weight = "bold" if f.startswith("PK") else "normal"
        color = "#1c5cab" if f.startswith("FK") else "#3a3a3a"
        ax.text(x + 0.15, top - 0.55 - ROW * (i + 0.6), f, va="center", fontsize=6.9, color=color,
                fontweight=weight)
    return {"x": x, "y": y, "h": h, "top": top}


def link(ax, a, b, label, a_side="right", b_side="left", a_dy=0.0, b_dy=0.0):
    ax_ = a["x"] + (WIDTH if a_side == "right" else 0)
    bx_ = b["x"] + (WIDTH if b_side == "right" else 0)
    ay = a["y"] + a["h"] / 2 + a_dy
    by = b["y"] + b["h"] / 2 + b_dy
    ax.add_patch(FancyArrowPatch((ax_, ay), (bx_, by), arrowstyle="-", lw=0.9, color="#555555",
                                 connectionstyle="arc3,rad=0.0"))
    ax.text(ax_ + (0.12 if a_side == "right" else -0.25), ay + 0.12, "1", fontsize=7, color="#1f1f1f")
    ax.text(bx_ + (-0.3 if b_side == "left" else 0.12), by + 0.12, "*", fontsize=9, color="#1f1f1f")
    ax.text((ax_ + bx_) / 2, (ay + by) / 2 - 0.3, label, fontsize=6.2, color="#5a5a5a", ha="center")


def main(out=OUT):
    fig, ax = plt.subplots(figsize=(13, 7.4), dpi=200)
    ax.set_xlim(0, 22.2)
    ax.set_ylim(0, 12.6)
    ax.axis("off")
    box = {n: draw_table(ax, n, k, x, top, f) for n, (k, x, top, f) in TABLES.items()}

    link(ax, box["dim_date"], box["fact_question"], "", a_dy=0.4, b_dy=0.6)
    link(ax, box["dim_user"], box["fact_question"], "", a_dy=0.2, b_dy=-0.6)
    link(ax, box["dim_date"], box["fact_answer"], "", a_dy=-0.6, b_dy=0.5)
    link(ax, box["dim_user"], box["fact_answer"], "", a_dy=-0.4, b_dy=-0.4)
    link(ax, box["fact_question"], box["bridge_question_tag"], "question_id", a_dy=0.6, b_dy=0.2)
    link(ax, box["dim_tag"], box["bridge_question_tag"], "tag", a_side="left", b_side="right")

    def vertical(parent, child, label):
        x = parent["x"] + WIDTH / 2
        ax.add_patch(FancyArrowPatch((x, parent["y"]), (x, child["top"]), arrowstyle="-", lw=0.9, color="#555555"))
        ax.text(x + 0.12, parent["y"] - 0.35, "1", fontsize=7)
        ax.text(x + 0.12, child["top"] + 0.12, "*", fontsize=9)
        ax.text(x + 0.35, (parent["y"] + child["top"]) / 2 - 0.1, label, fontsize=6.2, color="#5a5a5a")

    vertical(box["fact_question"], box["fact_answer"], "question_id")
    vertical(box["dim_tag"], box["agg_tag_month"], "tag")

    ax.text(0.4, 12.4, "Gold star schema (so_gold)", fontsize=11.5, fontweight="bold", va="top")
    ax.text(0.4, 11.85, "1 = one side (dimension or parent), * = many side. Blue = foreign key declared in Unity "
            "Catalog. Date links join date_key to created_date_key; user links join user_key to owner_user_key.\n"
            "agg_tag_month.month_start and agg_user_cohort.cohort_month join dim_date.full_date in the Power BI model.",
            fontsize=7.2, color="#5a5a5a", va="top", linespacing=1.5)
    legend = [("fact", "fact"), ("dim", "dimension"), ("bridge", "bridge"), ("agg", "aggregate")]
    for i, (k, label) in enumerate(legend):
        ax.add_patch(FancyBboxPatch((17.4 + i * 1.2, 0.35), 0.3, 0.3, boxstyle="square,pad=0", fc=FILL[k],
                                    ec="#8a8984", lw=0.6))
        ax.text(17.4 + i * 1.2 + 0.4, 0.5, label, fontsize=6.6, va="center")

    os.makedirs(os.path.dirname(out), exist_ok=True)
    fig.savefig(out, bbox_inches="tight", facecolor="white")
    print("wrote", out)


if __name__ == "__main__":
    main()
