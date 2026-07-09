import random
import time
import traceback
from datetime import date, datetime, timezone
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

import importlib

from config.env_config import get_data_db

# Dynamic import to handle module names starting with digits
_framework_core = importlib.import_module('notebooks.01_framework_core')
TableConfig = _framework_core.TableConfig
log = _framework_core.log
log_start = _framework_core.log_start
log_success = _framework_core.log_success
log_failure = _framework_core.log_failure
make_run_id = _framework_core.make_run_id
run_dq = _framework_core.run_dq
update_watermark = _framework_core.update_watermark

from utils.alert_manager import AlertManager

DATA_DB  = get_data_db()
_alerter = AlertManager()


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────
def run_ingestion(
    df:         DataFrame,
    cfg:        TableConfig,
    load_mnth:   str,
    dag_id:     str,
    run_date:   str
    # start_date: Optional[date] = None,
    # end_date:   Optional[date] = None,
) -> bool:
    """
    Write df to the target Delta table with retry.

    Incremental mode : start_date=None, end_date=None
                       → applies watermark filter, updates watermark after success
    Range mode       : start_date + end_date both provided
                       → loads full range in one job, no watermark update
    """
    spark = SparkSession.getActiveSession()


    for attempt in range(1, 3):
        started_at = datetime.now(timezone.utc)
        run_id     = make_run_id(dag_id, cfg.table_id, run_date, attempt)

        log("INFO",
            f"Ingestion [] attempt {attempt}/3",
            table=cfg.table_id, run_id=run_id)

        log_start(run_id, cfg, run_date, dag_id, attempt,
                  watermark_start=cfg.last_watermark_value)

        try:
            ok = _execute(df, cfg, run_date, run_id, started_at,
                          )
            if ok:
                return True

        except Exception as exc:
            msg     = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
            is_last = attempt == cfg.retry_max_attempts
            log("ERROR", f"Attempt {attempt} failed: {exc}", table=cfg.table_id)
            log_failure(run_id, started_at, "UNHANDLED_EXCEPTION",
                        msg, status="FAILED" if is_last else "RUNNING")
            if is_last:
                _alerter.send(cfg, run_id, "FAILED", msg, "CRITICAL")
                return False

        wait = _backoff(attempt, cfg)
        log("INFO", f"Retrying in {wait:.0f}s …")
        time.sleep(wait)

    return False


# ─────────────────────────────────────────────────────────────────────────────
# Execute one attempt
# ─────────────────────────────────────────────────────────────────────────────
def _execute(
    df:         DataFrame,
    cfg:        TableConfig,
    run_date:   date,
    run_id:     str,
    started_at: datetime,
    start_date: Optional[date],
    end_date:   Optional[date],
) -> bool:
    spark = SparkSession.getActiveSession()
    is_range = start_date is not None and end_date is not None

    # ── Apply source filter ───────────────────────────────────────────────
    if is_range:
        # Range mode: filter by load_date window
        if cfg.watermark_column:
            df = df.filter(
                (F.col(cfg.watermark_column).cast("date") >= F.lit(str(start_date))) &
                (F.col(cfg.watermark_column).cast("date") <= F.lit(str(end_date)))
            )
            log("INFO",
                f"Range filter: {cfg.watermark_column} BETWEEN {start_date} AND {end_date}",
                table=cfg.table_id)
    else:
        # Incremental mode: filter by watermark value
        if cfg.ingestion_mode == "INCR" and cfg.watermark_column and cfg.last_watermark_value:
            df = df.filter(
                F.col(cfg.watermark_column) > F.lit(cfg.last_watermark_value)
            )
            log("INFO",
                f"Watermark filter: {cfg.watermark_column} > {cfg.last_watermark_value}",
                table=cfg.table_id)

    rows_read = df.count()
    log("INFO", f"Rows after filter: {rows_read}", table=cfg.table_id)

    # Nothing to write for incremental with no new rows
    if rows_read == 0 and cfg.ingestion_mode != "FULL":
        log("INFO", "No new rows — skipping write", table=cfg.table_id)
        log_success(run_id, started_at, 0, 0, 0, [], True)
        return True

    # ── Add metadata cols ─────────────────────────────────────────────────
    df = _add_meta(df, run_id, cfg)

    # ── DQ: validate + tag rows ───────────────────────────────────────────
    df, dq_results, dq_passed = run_dq(df, cfg)

    clean_df    = df.filter("dq_passed = true")
    rejected_ct = df.filter("dq_passed = false").count()
    log("INFO",
        f"DQ {'PASS' if dq_passed else 'FAIL'} | "
        f"clean={rows_read - rejected_ct}  rejected={rejected_ct}",
        table=cfg.table_id)

    # ── Write to Delta ────────────────────────────────────────────────────
    target = f"{DATA_DB}.{cfg.target_table_name}"
    rows_written = _write(clean_df, cfg, target, run_date, start_date, end_date)

    # ── Update watermark (incremental only) ───────────────────────────────
    wm_end = None
    if not is_range and cfg.ingestion_mode == "INCR" and cfg.watermark_column:
        wm_val = clean_df.agg(F.max(cfg.watermark_column)).collect()[0][0]
        if wm_val:
            wm_end = wm_val
            update_watermark(cfg.table_id, wm_val, run_date)
            log("INFO", f"Watermark updated → {wm_val}", table=cfg.table_id)

    log_success(run_id, started_at,
                rows_read, rows_written, rejected_ct,
                dq_results, dq_passed, wm_end)

    if not dq_passed:
        _alerter.send(cfg, run_id, "FAILED",
                      f"DQ threshold not met for {cfg.table_id}", "CRITICAL")
    return dq_passed


# ─────────────────────────────────────────────────────────────────────────────
# Delta write
# ─────────────────────────────────────────────────────────────────────────────
def _write(
    df:         DataFrame,
    cfg:        TableConfig,
    target:     str,
    run_date:   date,
    start_date: Optional[date],
    end_date:   Optional[date],
) -> int:
    """
    Write df to target table. Returns row count written.

    replaceWhere scope:
      Incremental : load_date = '{run_date}'                AND opco = '{opco}'
      Range       : load_date BETWEEN '{start}' AND '{end}' AND opco = '{opco}'

    This ensures only affected partitions are rewritten;
    all other partitions remain untouched.
    """
    spark = SparkSession.getActiveSession()
    is_range = start_date is not None and end_date is not None
    opco     = cfg.opco
    mode     = cfg.ingestion_mode.upper()

    if is_range:
        replace_where = (
            f"load_date BETWEEN '{start_date}' AND '{end_date}' "
            f"AND opco = '{opco}'"
        )
    else:
        replace_where = f"load_date = '{run_date}' AND opco = '{opco}'"

    # ── FULL: atomic partition overwrite ──────────────────────────────────
    if mode == "FULL":
        (df.write
           .format("delta")
           .mode("overwrite")
           .option("replaceWhere", replace_where)
           .saveAsTable(target))
        ct = df.count()
        log("INFO", f"FULL write → {target}: {ct} rows | scope: {replace_where}")
        return ct

    # ── INCR / CDC: MERGE ─────────────────────────────────────────────────
    if not cfg.merge_keys:
        raise ValueError(f"merge_keys required for {mode} on {cfg.table_id}")

    merge_cond = " AND ".join(
        f"t.{k.strip()} = s.{k.strip()}" for k in cfg.merge_keys.split(",")
    )

    # First-ever load — plain append
    if not DeltaTable.isDeltaTable(spark, target):
        df.write.format("delta").mode("append").saveAsTable(target)
        ct = df.count()
        log("INFO", f"First load → {target}: {ct} rows")
        return ct

    dt = DeltaTable.forName(spark, target)

    if mode == "CDC":
        (dt.alias("t").merge(df.alias("s"), merge_cond)
           .whenMatchedDelete(condition="s.cdc_op = 'D'")
           .whenMatchedUpdateAll(condition="s.cdc_op != 'D'")
           .whenNotMatchedInsertAll(condition="s.cdc_op != 'D'")
           .execute())
    else:
        (dt.alias("t").merge(df.alias("s"), merge_cond)
           .whenMatchedUpdateAll()
           .whenNotMatchedInsertAll()
           .execute())

    ops = dt.history(1).select("operationMetrics").collect()[0][0]
    ins = int(ops.get("numTargetRowsInserted", 0))
    upd = int(ops.get("numTargetRowsUpdated",  0))
    dlt = int(ops.get("numTargetRowsDeleted",  0))
    log("INFO", f"{mode} merge → {target}: ins={ins} upd={upd} del={dlt}")
    return ins + upd


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
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


def _backoff(attempt: int, cfg: TableConfig) -> float:
    wait = min(
        cfg.retry_wait_sec * (cfg.retry_backoff ** (attempt - 1)),
        cfg.retry_max_wait,
    )
    return wait + random.uniform(0, wait * 0.1)
