"""Generate the Bronze/Silver/Ops data dictionary from src/so_lakehouse/schemas.py.

Writes docs/data_dictionary.md and refreshes the block between the
DATA-DICTIONARY markers in README.md, so documentation always matches code.

    python scripts/generate_data_dictionary.py
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from so_lakehouse.schemas import TABLES  # noqa: E402

START, END = "<!-- DATA-DICTIONARY:START -->", "<!-- DATA-DICTIONARY:END -->"
LAYER_TITLES = {"bronze": "Bronze layer (`so_bronze`)", "silver": "Silver layer (`so_silver`)",
                "ops": "Operational tables (`so_ops`)"}


from pyspark.sql.types import StructType  # noqa: E402


def type_label(dtype):
    return "struct" if isinstance(dtype, StructType) else dtype.simpleString()


def rows_for(field, pk, prefix=""):
    """One row per column; nested struct fields get their own dotted rows."""
    name = prefix + field.name
    key = "PK" if field.name in pk and not prefix else ""
    nullable = "no" if (key or not field.nullable) else "yes"
    yield (f"| `{name}` | `{type_label(field.dataType)}` | {key} | {nullable} | "
           f"{field.metadata.get('comment', '').replace('|', '/')} |")
    if isinstance(field.dataType, StructType):
        for sub in field.dataType.fields:
            yield from rows_for(sub, pk, prefix=name + ".")


def render():
    out = []
    for layer in ("bronze", "silver", "ops"):
        out.append(f"### {LAYER_TITLES[layer]}\n")
        for logical, (lyr, name, schema, pk, description) in TABLES.items():
            if lyr != layer:
                continue
            out.append(f"#### `{logical}`\n\n{description}. **Primary key:** `{', '.join(pk)}`\n")
            out.append("| Column | Type | Key | Nullable | Description |\n|---|---|---|---|---|")
            for f in schema.fields:
                out.extend(rows_for(f, pk))
            out.append("")
    return "\n".join(out)


def main():
    body = render()
    with open(os.path.join(ROOT, "docs", "data_dictionary.md"), "w") as f:
        f.write("# Data dictionary\n\nGenerated from `src/so_lakehouse/schemas.py` by "
                "`scripts/generate_data_dictionary.py`. Do not edit by hand.\n\n" + body)
    readme = os.path.join(ROOT, "README.md")
    text = open(readme).read()
    if START in text and END in text:
        pre, rest = text.split(START, 1)
        _, post = rest.split(END, 1)
        with open(readme, "w") as f:
            f.write(pre + START + "\n" + body + "\n" + END + post)
    print("data dictionary written")


if __name__ == "__main__":
    main()
