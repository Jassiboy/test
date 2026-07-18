# ─── pipeline/ingestion.py ────────────────────────────────────────────────────

def run_ingestion(
    cfg,                   # TableConfig
    opco:            str,
    windows:         list, # list of YYYYMM strings e.g. ["202501","202502","202503"]
    run_batch_id:    str,
    stage_table:     str,
    attempt:         int = 1,
    watermark_start: Optional[datetime] = None,
) -> bool:
    """
    DQ + promote all months in `windows` for one table.
    Writes exactly ONE row to pipeline_run_log for the full window.

    - Months that pass DQ → promoted to target
    - Months that fail DQ → skipped, logged in dq_message, target untouched
    - Returns True if ALL months promoted, False if any month failed
    """
    load_mnth_start = windows[0]
    load_mnth_end   = windows[-1]
    started_at      = None

    try:
        started_at = log_start(
            run_batch_id    = run_batch_id,
            cfg             = cfg,
            opco            = opco,
            load_mnth_start = load_mnth_start,
            load_mnth_end   = load_mnth_end,
            attempt         = attempt,
            watermark_start = watermark_start,
        )

        target         = f"{DATA_DB}.{cfg.target_table_name}"
        month_summaries = []
        total_read     = 0
        total_written  = 0

        for mnth in windows:
            scope = f"load_mnth = '{mnth}'"

            # Run DQ for this month against stage
            dq_passed, dq_rule_results = run_dq_on_stage(
                cfg, opco, mnth, stage_table,
                source_table=f"{cfg.source_system}.{cfg.source_table_name}",
            )

            summary = MonthDQSummary(
                month        = mnth,
                passed       = dq_passed,
                failed_rules = [r for r in dq_rule_results if not r.passed],
            )
            month_summaries.append(summary)

            if dq_passed:
                rows = _promote_to_target(cfg, target, stage_table, scope)
                total_written += rows
                total_read    += rows

        # All months processed — write final log row
        watermark_end = datetime.now(timezone.utc)
        log_end_success(
            run_batch_id    = run_batch_id,
            cfg             = cfg,
            opco            = opco,
            load_mnth_start = load_mnth_start,
            load_mnth_end   = load_mnth_end,
            attempt         = attempt,
            started_at      = started_at,
            rows_read       = total_read,
            rows_written    = total_written,
            month_summaries = month_summaries,
            watermark_end   = watermark_end,
        )

        # Return True only if every month passed
        return all(m.passed for m in month_summaries)

    except Exception as exc:
        is_final = (attempt >= cfg.retry_max_attempts)
        log_end_failure(
            run_batch_id    = run_batch_id,
            cfg             = cfg,
            opco            = opco,
            load_mnth_start = load_mnth_start,
            load_mnth_end   = load_mnth_end,
            attempt         = attempt,
            started_at      = started_at,
            error_code      = type(exc).__name__,
            error_message   = f"{exc}\n{traceback.format_exc()}",
            is_final        = is_final,
        )
        if not is_final:
            wait = min(
                cfg.retry_initial_wait_sec * (cfg.retry_backoff_multiplier ** (attempt - 1)),
                cfg.retry_max_wait_sec,
            )
            time.sleep(wait)
            return run_ingestion(
                cfg=cfg, opco=opco, windows=windows,
                run_batch_id=run_batch_id, stage_table=stage_table,
                attempt=attempt + 1, watermark_start=watermark_start,
            )
        return False



# ─── logging/run_logger.py ────────────────────────────────────────────────────
import json, os, time, traceback
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional

from delta import DeltaTable
from pyspark.sql import SparkSession

METADATA_DB   = os.getenv("METADATA_DB", "pipeline_metadata")
RUN_LOG_TABLE = f"{METADATA_DB}.pipeline_run_log"


# ─── Data containers ──────────────────────────────────────────────────────────

@dataclass
class DQRuleResult:
    """Result of one DQ rule for one month."""
    month:            str
    rule_id:          str
    rule_type:        str
    target_column:    Optional[str]
    rule_expression:  str
    severity:         str          # CRITICAL | WARNING | INFO
    passed:           bool
    records_failed:   int = 0
    actual_error_pct: float = 0.0
    detail:           str = ""


@dataclass
class MonthDQSummary:
    """Aggregated DQ outcome for one month."""
    month:        str
    passed:       bool
    failed_rules: list = field(default_factory=list)   # list[DQRuleResult]


# ─── Core logger functions ─────────────────────────────────────────────────────

def log_start(
    run_batch_id:    str,
    cfg,                           # TableConfig
    opco:            str,
    load_mnth_start: str,
    load_mnth_end:   str,
    attempt:         int = 1,
    watermark_start: Optional[datetime] = None,
) -> datetime:
    """
    Write the initial RUNNING row.
    Returns started_at so the caller can pass it back to log_end_*.
    """
    started_at = datetime.now(timezone.utc)

    row = {
        "run_batch_id":    run_batch_id,
        "log_time":        started_at.isoformat(),
        "opco":            opco,
        "dataset_layer":   cfg.target_dataset_layer,
        "table_name":      cfg.target_table_name,
        "load_process":    cfg.load_process,
        "load_mnth_start": load_mnth_start,
        "load_mnth_end":   load_mnth_end,
        "status":          "RUNNING",
        "message":         None,
        "attempt_number":  attempt,
        "watermark_start": watermark_start.isoformat() if watermark_start else None,
        "watermark_end":   None,
        "started_at":      started_at.isoformat(),
        "completed_at":    None,
        "duration_seconds": None,
        "rows_read":       None,
        "rows_written":    None,
        "dq_result":       None,
        "dq_message":      None,
    }
    _upsert_log(run_batch_id, cfg.target_table_name, load_mnth_start, attempt, row)
    return started_at


def log_end_success(
    run_batch_id:     str,
    cfg,
    opco:             str,
    load_mnth_start:  str,
    load_mnth_end:    str,
    attempt:          int,
    started_at:       datetime,
    rows_read:        int,
    rows_written:     int,
    month_summaries:  list,        # list[MonthDQSummary]
    watermark_end:    Optional[datetime] = None,
) -> None:
    """
    Update row to SUCCESS (or PARTIAL if some months failed DQ).
    All DQ detail is collapsed into dq_result + dq_message.
    """
    now      = datetime.now(timezone.utc)
    duration = int((now - started_at).total_seconds())

    dq_result, dq_message, status = _build_dq_summary(month_summaries)

    row = {
        "run_batch_id":    run_batch_id,
        "log_time":        now.isoformat(),
        "opco":            opco,
        "dataset_layer":   cfg.target_dataset_layer,
        "table_name":      cfg.target_table_name,
        "load_process":    cfg.load_process,
        "load_mnth_start": load_mnth_start,
        "load_mnth_end":   load_mnth_end,
        "status":          status,
        "message":         f"Completed. {dq_message}",
        "attempt_number":  attempt,
        "watermark_end":   watermark_end.isoformat() if watermark_end else None,
        "started_at":      started_at.isoformat(),
        "completed_at":    now.isoformat(),
        "duration_seconds": duration,
        "rows_read":       rows_read,
        "rows_written":    rows_written,
        "dq_result":       dq_result,
        "dq_message":      dq_message,
    }
    _upsert_log(run_batch_id, cfg.target_table_name, load_mnth_start, attempt, row)


def log_end_failure(
    run_batch_id:    str,
    cfg,
    opco:            str,
    load_mnth_start: str,
    load_mnth_end:   str,
    attempt:         int,
    started_at:      Optional[datetime],
    error_code:      str,
    error_message:   str,
    is_final:        bool = True,
    month_summaries: list = None,  # list[MonthDQSummary] — pass if DQ ran before failure
) -> None:
    """
    Update row to FAILED or RETRYING.
    Optionally includes partial DQ summary if DQ had already run.
    """
    now      = datetime.now(timezone.utc)
    duration = int((now - started_at).total_seconds()) if started_at else 0
    status   = "FAILED" if is_final else "RETRYING"

    dq_result, dq_message = "SKIPPED", "DQ did not complete"
    if month_summaries:
        dq_result, dq_message, _ = _build_dq_summary(month_summaries)

    row = {
        "run_batch_id":    run_batch_id,
        "log_time":        now.isoformat(),
        "opco":            opco,
        "dataset_layer":   cfg.target_dataset_layer,
        "table_name":      cfg.target_table_name,
        "load_process":    cfg.load_process,
        "load_mnth_start": load_mnth_start,
        "load_mnth_end":   load_mnth_end,
        "status":          status,
        "message":         f"{error_code}: {error_message[:500]}",
        "attempt_number":  attempt,
        "started_at":      started_at.isoformat() if started_at else None,
        "completed_at":    now.isoformat(),
        "duration_seconds": duration,
        "rows_read":       None,
        "rows_written":    None,
        "dq_result":       dq_result,
        "dq_message":      dq_message,
    }
    _upsert_log(run_batch_id, cfg.target_table_name, load_mnth_start, attempt, row)


# ─── DQ summary builder ────────────────────────────────────────────────────────

def _build_dq_summary(month_summaries: list):
    """
    Collapse all MonthDQSummary objects into three values:
      dq_result  → PASS | FAIL | WARN | PARTIAL
      dq_message → human-readable string
      status     → SUCCESS | PARTIAL | DQ_FAILED

    Examples:
      All passed  → dq_result=PASS,
                    dq_message="All months passed DQ: [202501, 202502, 202503]"

      All failed  → dq_result=FAIL,
                    dq_message="All months failed DQ: [202501] — rule_a(CRITICAL); [202502] — rule_b(WARNING)"

      Mixed       → dq_result=PARTIAL,
                    dq_message="Months passed: [202501, 202502]. Months failed: [202503] — rule_a(CRITICAL), rule_b(WARNING)"
    """
    if not month_summaries:
        return "SKIPPED", "No months processed", "SUCCESS"

    passed_months = [m for m in month_summaries if m.passed]
    failed_months = [m for m in month_summaries if not m.passed]

    passed_labels = [m.month for m in passed_months]
    failed_labels = [
        f"[{m.month}] — " + ", ".join(
            f"{r.rule_id}({r.severity})"
            + (f" [{r.records_failed} records]" if r.records_failed else "")
            for r in m.failed_rules
        )
        for m in failed_months
    ]

    # Determine overall DQ result
    has_critical = any(
        r.severity == "CRITICAL"
        for m in failed_months
        for r in m.failed_rules
    )

    if not failed_months:
        dq_result  = "PASS"
        dq_message = f"All months passed DQ: {passed_labels}"
        status     = "SUCCESS"

    elif not passed_months:
        dq_result  = "FAIL"
        dq_message = "All months failed DQ. " + " | ".join(failed_labels)
        status     = "DQ_FAILED"

    else:
        dq_result  = "FAIL" if has_critical else "WARN"
        dq_message = (
            f"Months passed: {passed_labels}. "
            f"Months failed: " + " | ".join(failed_labels)
        )
        status = "PARTIAL"

    return dq_result, dq_message, status


# ─── Upsert helper ────────────────────────────────────────────────────────────

def _upsert_log(
    run_batch_id:    str,
    table_name:      str,
    load_mnth_start: str,
    attempt:         int,
    row:             dict,
) -> None:
    """
    MERGE on (run_batch_id, table_name, load_mnth_start, attempt_number).
    First call inserts (RUNNING). Subsequent calls update in place.
    Sink failures are non-fatal — logged to stdout only.
    """
    spark = SparkSession.getActiveSession()
    try:
        df = spark.createDataFrame([row])
        (
            DeltaTable
            .forName(spark, RUN_LOG_TABLE)
            .alias("t")
            .merge(
                df.alias("s"),
                """t.run_batch_id    = s.run_batch_id
               AND t.table_name      = s.table_name
               AND t.load_mnth_start = s.load_mnth_start
               AND t.attempt_number  = s.attempt_number""",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    except Exception as e:
        print(f"[WARN] _upsert_log failed (non-fatal): {e}")
