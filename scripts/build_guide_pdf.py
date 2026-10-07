"""Render docs/phase2_guide.md to docs/phase2_guide.pdf (A4) with reportlab.

Supports the Markdown used in the guide: headings, paragraphs, **bold**, `code`,
[links](url), bullet and numbered lists, tables, fenced code blocks and rules.

    pip install reportlab && python scripts/build_guide_pdf.py
"""
import os
import re
import sys

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import (BaseDocTemplate, Frame, KeepTogether, PageTemplate, Paragraph, Preformatted,
                                Spacer, Table, TableStyle)
from reportlab.platypus.flowables import HRFlowable

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "docs", "phase2_guide.md")
OUT = os.path.join(ROOT, "docs", "phase2_guide.pdf")

INK, INK2, MUTED = colors.HexColor("#0b0b0b"), colors.HexColor("#52514e"), colors.HexColor("#8a8984")
ACCENT, HEAD, RULE = colors.HexColor("#1c5cab"), colors.HexColor("#f0efec"), colors.HexColor("#d9d8d4")
CODE_BG = colors.HexColor("#f6f5f2")

S = {
    "h1": ParagraphStyle("h1", fontName="Helvetica-Bold", fontSize=18, leading=22, textColor=INK, spaceAfter=6),
    "h2": ParagraphStyle("h2", fontName="Helvetica-Bold", fontSize=13, leading=16, textColor=ACCENT,
                         spaceBefore=12, spaceAfter=4),
    "h3": ParagraphStyle("h3", fontName="Helvetica-Bold", fontSize=10.5, leading=13.5, textColor=INK,
                         spaceBefore=8, spaceAfter=3),
    "body": ParagraphStyle("body", fontName="Helvetica", fontSize=9.4, leading=13, textColor=INK, spaceAfter=5,
                           alignment=TA_LEFT),
    "bullet": ParagraphStyle("bullet", fontName="Helvetica", fontSize=9.4, leading=12.8, textColor=INK,
                             leftIndent=13, bulletIndent=3, spaceAfter=2.5),
    "cell": ParagraphStyle("cell", fontName="Helvetica", fontSize=8, leading=10.2, textColor=INK),
    "cellb": ParagraphStyle("cellb", fontName="Helvetica-Bold", fontSize=8, leading=10.2, textColor=INK),
    "code": ParagraphStyle("code", fontName="Courier", fontSize=7.6, leading=9.6, textColor=INK),
}


def inline(text):
    """Markdown inline -> reportlab mini-HTML."""
    text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    codes = []

    def keep_code(m):
        codes.append(m.group(1))
        return f"\x00{len(codes) - 1}\x00"
    text = re.sub(r"`([^`]+)`", keep_code, text)
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", lambda m: m.group(1) if not m.group(2).startswith("http")
                  else f'<link href="{m.group(2)}" color="#1c5cab">{m.group(1)}</link>', text)
    text = re.sub(r"\*\*([^*]+)\*\*", r"<b>\1</b>", text)
    text = re.sub(r"(?<![\w*])\*([^*]+)\*(?![\w*])", r"<i>\1</i>", text)
    text = text.replace("→", "-&gt;").replace("…", "...").replace("≥", "&gt;=")
    return re.sub(r"\x00(\d+)\x00", lambda m: f'<font name="Courier" size="8.2">{codes[int(m.group(1))]}</font>',
                  text)


def table(rows, width):
    ncol = len(rows[0])
    data = [[Paragraph(inline(c), S["cellb" if i == 0 else "cell"]) for c in r + [""] * (ncol - len(r))]
            for i, r in enumerate(rows)]
    lens = [max(len(r[j]) if j < len(r) else 0 for r in rows) for j in range(ncol)]
    weights = [min(max(l, 6), 60) for l in lens]
    total = sum(weights)
    t = Table(data, colWidths=[width * w / total for w in weights], repeatRows=1, hAlign="LEFT")
    t.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP"), ("BACKGROUND", (0, 0), (-1, 0), HEAD),
                           ("LINEBELOW", (0, 0), (-1, -1), 0.4, RULE), ("LINEABOVE", (0, 0), (-1, 0), 0.4, RULE),
                           ("TOPPADDING", (0, 0), (-1, -1), 2.2), ("BOTTOMPADDING", (0, 0), (-1, -1), 2.6),
                           ("LEFTPADDING", (0, 0), (-1, -1), 4), ("RIGHTPADDING", (0, 0), (-1, -1), 4)]))
    return t


def code_block(lines, width):
    pre = Preformatted("\n".join(lines), S["code"])
    t = Table([[pre]], colWidths=[width], hAlign="LEFT")
    t.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), CODE_BG), ("BOX", (0, 0), (-1, -1), 0.4, RULE),
                           ("LEFTPADDING", (0, 0), (-1, -1), 6), ("TOPPADDING", (0, 0), (-1, -1), 4),
                           ("BOTTOMPADDING", (0, 0), (-1, -1), 4)]))
    return t


def parse(md, width):
    story, lines, i = [], md.splitlines(), 0
    while i < len(lines):
        line = lines[i]
        if line.lstrip().startswith("```"):
            pad = len(line) - len(line.lstrip())
            block = []
            i += 1
            while i < len(lines) and not lines[i].lstrip().startswith("```"):
                block.append(lines[i][pad:] if lines[i][:pad].strip() == "" else lines[i])
                i += 1
            story += [code_block(block, width), Spacer(1, 5)]
        elif line.startswith("<!--") or not line.strip():
            pass
        elif line.strip() == "---":
            story.append(HRFlowable(width="100%", thickness=0.5, color=RULE, spaceBefore=4, spaceAfter=4))
        elif line.startswith("#"):
            level = len(line) - len(line.lstrip("#"))
            story.append(Paragraph(inline(line.lstrip("#").strip()), S[f"h{min(level, 3)}"]))
        elif line.startswith("|"):
            rows = []
            while i < len(lines) and lines[i].startswith("|"):
                cells = [c.strip() for c in lines[i].strip().strip("|").split("|")]
                if not all(re.fullmatch(r":?-+:?", c) for c in cells if c):
                    rows.append(cells)
                i += 1
            story += [table(rows, width), Spacer(1, 6)]
            continue
        elif re.match(r"^\s*(-|\d+\.)\s", line):
            m = re.match(r"^(\s*)(-|\d+\.)\s+(.*)$", line)
            indent = len(m.group(1)) // 2
            bullet = "•" if m.group(2) == "-" else m.group(2)
            text = m.group(3)
            while i + 1 < len(lines) and lines[i + 1].startswith("   ") and not re.match(r"^\s*(-|\d+\.)\s",
                                                                                         lines[i + 1]) \
                    and not lines[i + 1].strip().startswith("```"):
                i += 1
                text += " " + lines[i].strip()
            style = ParagraphStyle("b", parent=S["bullet"], leftIndent=13 + 12 * indent, bulletIndent=3 + 12 * indent)
            story.append(Paragraph(inline(text), style, bulletText=bullet))
        else:
            para = [line.strip()]
            while i + 1 < len(lines) and lines[i + 1].strip() and not re.match(r"^(#|\||```|\s*-\s|\s*\d+\.\s|---)",
                                                                               lines[i + 1]):
                i += 1
                para.append(lines[i].strip())
            story.append(Paragraph(inline(" ".join(para)), S["body"]))
        i += 1
    return story


def on_page(canv, doc):
    canv.saveState()
    canv.setFont("Helvetica", 7.2)
    canv.setFillColor(MUTED)
    canv.drawString(18 * mm, 10 * mm, "Stack Overflow Developer Q&A Lakehouse  |  Phase 2 Learning Guide")
    canv.drawRightString(A4[0] - 18 * mm, 10 * mm, f"Page {doc.page}")
    canv.restoreState()


def main(src=SRC, out=OUT):
    doc = BaseDocTemplate(out, pagesize=A4, leftMargin=18 * mm, rightMargin=18 * mm, topMargin=16 * mm,
                          bottomMargin=17 * mm, title="Phase 2 Learning Guide: Bronze and Silver Layers",
                          author="Muhammad Sufyan, Muaaz Fahad")
    frame = Frame(doc.leftMargin, doc.bottomMargin, doc.width, doc.height, leftPadding=0, rightPadding=0,
                  topPadding=0, bottomPadding=0)
    doc.addPageTemplates([PageTemplate(id="p", frames=[frame], onPage=on_page)])
    with open(src) as f:
        doc.build(parse(f.read(), doc.width))
    print("wrote", out)


if __name__ == "__main__":
    main(*sys.argv[1:])
