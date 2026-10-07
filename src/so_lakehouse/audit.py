"""Audit logging: one row in ops.pipeline_execution_logs per file or table processed.

Usage
    audit = AuditLogger(spark, cfg, pipeline_name="raw_to_bronze", run_mode="incremental")
    with audit.step(layer="Raw-to-Bronze", entity="questions", source_parameter=path,
                    target_table=t, batch_id=b, load_type="full") as rec:
        ...do work...
        rec.rows_inserted = 123

The entry is written in a `finally` block, so a failure is logged with its
error message and timings before the exception is handled. With
reraise=False the caller continues with the next file instead of crashing.
"""
import traceback
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from .schemas import EXECUTION_LOGS


def utc_now():
    return datetime.now(timezone.utc)


@dataclass
class StepRecord:
    layer: str
    entity: str
    source_parameter: str
    target_table: str
    batch_id: Optional[str]
    load_type: Optional[str]
    start_time: datetime = field(default_factory=utc_now)
    end_time: Optional[datetime] = None
    status: Optional[str] = None
    rows_read: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0
    rows_quarantined: int = 0
    message: Optional[str] = None
    log_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def skip(self, message):
        self.status = "SKIPPED"
        self.message = message


class AuditLogger:
    def __init__(self, spark, cfg, pipeline_name, run_mode, run_id=None, echo=print):
        self.spark = spark
        self.cfg = cfg
        self.pipeline_name = pipeline_name
        self.run_mode = run_mode
        self.run_id = run_id or f"{pipeline_name}-{utc_now():%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}"
        self.table = cfg.table("ops.pipeline_execution_logs")
        self.records: List[StepRecord] = []
        self.echo = echo

    @contextmanager
    def step(self, layer, entity, source_parameter, target_table, batch_id=None, load_type=None, reraise=False):
        rec = StepRecord(layer, entity, str(source_parameter), target_table, batch_id, load_type)
        try:
            yield rec
            if rec.status is None:
                rec.status = "SUCCESS"
        except Exception as exc:  # noqa: BLE001 - we log every failure
            rec.status = "FAILED"
            rec.message = f"{type(exc).__name__}: {exc}"[:2000]
            self.echo(traceback.format_exc(limit=3))
            if reraise:
                raise
        finally:
            rec.end_time = utc_now()
            self.records.append(rec)
            self._write(rec)
            self.echo(f"[{rec.status:<7}] {layer} | {entity:<34} | read={rec.rows_read} ins={rec.rows_inserted} "
                      f"upd={rec.rows_updated} del={rec.rows_deleted} quar={rec.rows_quarantined} "
                      f"| {(rec.end_time - rec.start_time).total_seconds():.1f}s"
                      + (f" | {rec.message[:160]}" if rec.message else ""))

    def _write(self, rec):
        row = (rec.log_id, self.run_id, self.pipeline_name, rec.layer, self.run_mode, rec.load_type, rec.entity,
               rec.batch_id, rec.source_parameter, rec.target_table, rec.start_time, rec.end_time,
               (rec.end_time - rec.start_time).total_seconds(), rec.status, int(rec.rows_read),
               int(rec.rows_inserted), int(rec.rows_updated), int(rec.rows_deleted), int(rec.rows_quarantined),
               rec.message, utc_now())
        try:
            (self.spark.createDataFrame([row], EXECUTION_LOGS)
             .write.format("delta").mode("append").saveAsTable(self.table))
        except Exception as exc:  # logging must never hide the real outcome
            self.echo(f"WARNING: could not write audit log entry: {exc}")

    # ------------------------------------------------------------- summaries
    @property
    def failures(self):
        return [r for r in self.records if r.status == "FAILED"]

    def summary(self):
        by = {}
        for r in self.records:
            by[r.status] = by.get(r.status, 0) + 1
        return {"run_id": self.run_id, "steps": len(self.records), "by_status": by,
                "rows_inserted": sum(r.rows_inserted for r in self.records),
                "rows_updated": sum(r.rows_updated for r in self.records),
                "rows_quarantined": sum(r.rows_quarantined for r in self.records)}
