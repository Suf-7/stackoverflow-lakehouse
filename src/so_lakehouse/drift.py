"""Schema drift detection.

Spark's schema-on-read silently DROPS fields that are not in the schema and
turns type mismatches into nulls. That hides drift. So, next to the strict
`from_json(value, contract)` parse, every raw line also goes through
`contract_check`, which compares the record with the contract and reports:

  * unexpected fields   -> new columns (top-level or nested, e.g. owner.badges)
  * type errors         -> e.g. score arrived as "five" instead of an integer
  * malformed records   -> the line is not a JSON object

The Bronze loader then decides per record:
  * new columns     -> evolve the table (mergeSchema) or keep them in _rescued_data
  * type errors     -> quarantine the record (the batch keeps running)
  * malformed lines -> quarantine the record
"""
import json

from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, BooleanType, DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType)

INT32 = (-2**31, 2**31 - 1)
INT64 = (-2**63, 2**63 - 1)

CHECK_RESULT = StructType([
    StructField("is_json", BooleanType()),
    StructField("unexpected", ArrayType(StructType([StructField("path", StringType()),
                                                    StructField("value", StringType())]))),
    StructField("type_errors", ArrayType(StructType([StructField("path", StringType()),
                                                     StructField("expected", StringType()),
                                                     StructField("actual", StringType())]))),
])


def _type_name(value):
    return "null" if value is None else type(value).__name__


def _check(value, dtype, path, unexpected, errors):
    """Recursively compare one JSON value with a Spark type."""
    if value is None or isinstance(dtype, StringType):
        return  # Spark stores any JSON token (even objects) as text in a STRING field
    if isinstance(dtype, BooleanType):
        ok = isinstance(value, bool)
    elif isinstance(dtype, IntegerType):
        ok = isinstance(value, int) and not isinstance(value, bool) and INT32[0] <= value <= INT32[1]
    elif isinstance(dtype, LongType):
        ok = isinstance(value, int) and not isinstance(value, bool) and INT64[0] <= value <= INT64[1]
    elif isinstance(dtype, DoubleType):
        ok = isinstance(value, (int, float)) and not isinstance(value, bool)
    elif isinstance(dtype, ArrayType):
        ok = isinstance(value, list)
        if ok:
            for i, element in enumerate(value):
                _check(element, dtype.elementType, f"{path}[]", unexpected, errors)
    elif isinstance(dtype, StructType):
        ok = isinstance(value, dict)
        if ok:
            _check_object(value, dtype, path + ".", unexpected, errors)
    else:
        ok = True
    if not ok:
        errors.append((path, dtype.simpleString(), f"{_type_name(value)}: {json.dumps(value)[:60]}"))


def _check_object(obj, struct, prefix, unexpected, errors):
    fields = {f.name: f.dataType for f in struct.fields}
    for key, value in obj.items():
        if key not in fields:
            unexpected.append((prefix + key, json.dumps(value)[:4000]))
        else:
            _check(value, fields[key], prefix + key, unexpected, errors)


def make_contract_check(contract):
    """Return a Python UDF: raw JSON line -> CHECK_RESULT struct."""

    def check(line):
        try:
            record = json.loads(line)
        except Exception:
            return (False, [], [])
        if not isinstance(record, dict):
            return (False, [], [])
        unexpected, errors = [], []
        _check_object(record, contract, "", unexpected, errors)
        # de-duplicate array element paths such as tags[]
        seen, uniq_errors = set(), []
        for e in errors:
            if e[0] not in seen:
                seen.add(e[0])
                uniq_errors.append(e)
        return (True, unexpected, uniq_errors)

    return F.udf(check, CHECK_RESULT)


def sanitize_column(name):
    """Turn an unexpected JSON key into a safe Delta column name."""
    clean = "".join(ch if (ch.isalnum() or ch == "_") else "_" for ch in name).strip("_").lower()
    return clean or "unnamed_column"
