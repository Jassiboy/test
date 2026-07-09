"""
notebooks/02_ingestion.py
===========================
Writes a transformed DataFrame to a Delta Lake target table for one
(opco, load_mnth) scope:

    1. delete any existing rows for this opco + load_mnth
    2. append the new rows

No MERGE, no merge_keys, no FULL/INCR/CDC branching, no watermark —
every run for a given month fully replaces that month's data, so a plain
delete + append is all that's needed.

Public function:
    run_ingestion(df, cfg, load_mnth, dag_id, run_date) -> bool
"""

import random
import time
import traceback
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

import importlib

from config.env_config import get_data_db

# Dynamic import to handle module names starting with digits
_framework_core = importlib.import_module('notebooks.01_framework_core')
TableConfig  = _framework_core.TableConfig
log          = _framework_core.log
log_start    = _framework_core.log_start
log_success  = _framework_core.log_success
log_failure  = _framework_core.log_failure
make_run_id  = _framework_core.make_run_id
run_dq       = _framework_core.run_dq

from utils.alert_manager import AlertManager

DATA_DB  = get_data_db()
_alerter = AlertManager()


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────
def run_ingestion(
    df:        DataFrame,
    cfg:       TableConfig,
    load_mnth: str,
    dag_id:    str,
    run_date:  str,
) -> bool:
    """
    Delete existing rows for (cfg.opco, load_mnth) in the target table, then
    append df. Retries cfg.retry_max_attempts times with exponential backoff
    (each retry re-does the delete + append, so it's safe to repeat).
    """
    target = f"{DATA_DB}.{cfg.target_table_name}"

    for attempt in range(1, cfg.retry_max_attempts + 1):
        started_at = datetime.now(timezone.utc)
        run_id = make_run_id(dag_id, cfg.table_id, run_date, attempt)

        log("INFO", f"Ingestion [{load_mnth}] attempt {attempt}/{cfg.retry_max_attempts}",
            table=cfg.table_id, run_id=run_id)
        log_start(run_id, cfg, run_date, dag_id, attempt)

        try:
            rows_read = df.count()
            log("INFO", f"Rows to load: {rows_read}", table=cfg.table_id)

            if rows_read == 0:
                log("INFO", "No rows — skipping delete/append", table=cfg.table_id)
                log_success(run_id, started_at, 0, 0, 0, [], True)
                return True

            df = _add_meta(df, run_id, cfg)
            df, dq_results, dq_passed = run_dq(df, cfg)

            clean_df    = df.filter("dq_passed = true")
            rejected_ct = df.filter("dq_passed = false").count()
            log("INFO", f"DQ {'PASS' if dq_passed else 'FAIL'} | "
                        f"clean={rows_read - rejected_ct}  rejected={rejected_ct}",
                table=cfg.table_id)

            rows_written = _delete_and_append(clean_df, cfg, target, load_mnth)
            log_success(run_id, started_at, rows_read, rows_written, rejected_ct,
                        dq_results, dq_passed)

            if dq_passed:
                return True
            _alerter.send(cfg, run_id, "FAILED",
                          f"DQ threshold not met for {cfg.table_id}", "CRITICAL")

        except Exception as exc:
            msg     = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
            is_last = attempt == cfg.retry_max_attempts
            log("ERROR", f"Attempt {attempt} failed: {exc}", table=cfg.table_id)
            log_failure(run_id, started_at, "UNHANDLED_EXCEPTION",
                        msg, status="FAILED" if is_last else "RUNNING")
            if is_last:
                _alerter.send(cfg, run_id, "FAILED", msg, "CRITICAL")
                return False

        if attempt == cfg.retry_max_attempts:
            return False
        wait = min(cfg.retry_wait_sec * (cfg.retry_backoff ** (attempt - 1)), cfg.retry_max_wait)
        wait += random.uniform(0, wait * 0.1)
        log("INFO", f"Retrying in {wait:.0f}s …")
        time.sleep(wait)

    return False


# ─────────────────────────────────────────────────────────────────────────────
# Delete + append
# ─────────────────────────────────────────────────────────────────────────────
def _delete_and_append(df: DataFrame, cfg: TableConfig, target: str, load_mnth: str) -> int:
    """
    Delete rows WHERE opco = cfg.opco AND load_mnth = load_mnth, then append
    df. Returns the number of rows appended.
    """
    spark = SparkSession.getActiveSession()
    delete_cond = f"opco = '{cfg.opco}' AND load_mnth = '{load_mnth}'"

    if DeltaTable.isDeltaTable(spark, target):
        DeltaTable.forName(spark, target).delete(delete_cond)
        log("INFO", f"Deleted existing rows → {target} WHERE {delete_cond}")
    else:
        log("INFO", f"{target} doesn't exist yet — first load, nothing to delete")

    df.write.format("delta").mode("append").saveAsTable(target)
    ct = df.count()
    log("INFO", f"Appended {ct} rows → {target}")
    return ct


# ─────────────────────────────────────────────────────────────────────────────
# Metadata columns
# ─────────────────────────────────────────────────────────────────────────────
def _add_meta(df: DataFrame, run_id: str, cfg: TableConfig) -> DataFrame:
    now = datetime.now(timezone.utc).isoformat()
    return (df
        .withColumn("run_id",        F.lit(run_id))
        .withColumn("source_system", F.lit(cfg.source_system))
        .withColumn("created_at",
            F.coalesce(F.col("created_at"), F.lit(now).cast("timestamp")))
        .withColumn("updated_at",    F.lit(now).cast("timestamp"))
    )
