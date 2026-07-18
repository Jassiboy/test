run_batch_id:string
log_timet:timestamp
opco:string
dataset_layer:string
table_name:string
load_process:string
load_mnth_start:string
load_mnth_end:string
status:string
message:string
attempt_number:integer
watermark_start:timestamp
watermark_end:timestamp
started_at:timestamp
completed_at:timestamp
duration_seconds:integer,
rows_read:long
rows_written:long
dq_result:String
dq_message:String


def run_layer(
    run_batch_id: str, opco: str, layer: str, run_date: date, skip_gate: bool,
    table_id: list[str],
    start_date: Optional[date] = None, end_date: Optional[date] = None, stage: Optional[str] = "False"
) -> bool:
    """
    For every table in table_id:
      1. Load ALL months in `windows` into that table's stage table
         (no DQ gate yet — just get everything loaded).
      2. Run DQ (cfg.dq_rules, stage vs source) for EACH month.
      3. Promote to target ONLY the months that pass — a bad month is
         skipped, not a reason to fail the whole table or the whole run.
    """
    all_ok = True
    configs = read_table_configs(table_id=table_id, schedule_group=None)
    if not configs:
        log("WARN", "No configs found", layer=layer, opco=opco)
        return all_ok

    if not skip_gate:
        gate_ok, blocked_by = True, []  # check_dq_gate(configs, run_date)
        if not gate_ok:
            log("ERROR", f"DQ gate blocked {layer}: upstream not ready {blocked_by}")
            return False

    for cfg in configs:
        windows = [] if start_date is None else month_chunks(int(start_date), int(end_date))
        mode = "incremental" if start_date is None else "monthly_backfill"

        run_date = date.today()
        log("INFO", "═" * 55)
        # log("INFO", f"OPCO={opco} | {mode} | tables={table_names or 'ALL'} | groups={groups}")
        
        if mode == 'incremental':
            #incremental logic
            current_month_str = run_date.strftime("%Y%m")
            if run_date.day <= cfg.incremental_cutoff_day:
                prev_month_date = run_date - relativedelta(months=1)
                prev_month_str = prev_month_date.strftime("%Y%m")
                windows = [prev_month_str, current_month_str]
            else:
                windows = [current_month_str]

        temp_table  = f"{DATA_DB}.{cfg.target_table_name}_temp"
        source_table = f"{cfg.source_system}.{cfg.source_table}"

        _reset_stage(stage_table)

        # ── 1. Load every month into stage ────────────────────────────────
        for mnth in windows:
            tdf = get_transformed_df(opco, layer, cfg.target_table_name, cfg.load_process, run_date, mnth)
            tdf.write.format("delta").mode("append").saveAsTable(stage_table)
            log("INFO", f"Loaded {mnth} → stage", table=cfg.table_id)

        # ── 2 & 3. DQ each month, promote only the ones that pass ─────────
        good_months, bad_months = [], []
        for mnth in windows:
            ok = run_ingestion(cfg, opco, mnth, dag_id, run_date, stage_table)
            (good_months if ok else bad_months).append(mnth)
            all_ok = all_ok and ok

        log("INFO", f"{cfg.table_id}: {len(good_months)} promoted, {len(bad_months)} failed",
            good=good_months, bad=bad_months)

    return all_ok


def run_ingestion(
    cfg:         "TableConfig",
    opco:        str,
    load_mnth:   str,
    dag_id:      str,
    run_date:    str,
    stage_table: str,
) -> bool:
    """
    DQ-gate one month, then promote it from stage to target.

      1. Run cfg.dq_rules (from data_quality_rules, dispatched to dq_rules.py)
         against stage_table vs the source table, for this month only.
      2. If ALL rules pass: atomically overwrite that month's partition in
         target from stage (replaceWhere — one transaction, no separate
         delete + append).
      3. If ANY rule fails: log it, alert, return False — target is left
         untouched for this month, whatever was already there stays.

    Retries retry_max_attempts times on unhandled exceptions only (a DQ
    failure is a verdict, not a transient error, so it does NOT retry).
    """
    target = f"{DATA_DB}.{cfg.target_table_name}"
    scope  = f"ratemnth = '{load_mnth}'"

    for attempt in range(1, retry_max_attempts + 1):
        started_at = datetime.now(timezone.utc)
        run_id = make_run_id(dag_id, cfg.table_id, load_mnth, attempt)
        log("INFO", f"DQ + promote [{load_mnth}] attempt {attempt}/{retry_max_attempts}",
            table=cfg.table_id, run_id=run_id)
        log_start(run_id, cfg, run_date, dag_id, attempt)

        try:
            dq_passed, dq_results = run_dq_on_stage(
                cfg, opco, load_mnth, stage_table,
                f"{cfg.source_system}.{cfg.source_table_name}")
            log("INFO" if dq_passed else "ERROR",
                f"DQ {'PASS' if dq_passed else 'FAIL'} — {load_mnth}",
                table=cfg.table_id, results=dq_results)

            if not dq_passed:
                log_success(run_id, started_at, 0, 0, 0, dq_results, False)
                # _alerter.send(cfg, run_id, "FAILED",
                #               f"DQ failed for {cfg.table_id} {load_mnth}: {dq_results}", "CRITICAL")
                return False   # verdict, not an error — no retry, target untouched

            rows_written = _promote_to_target(cfg, target, stage_table, scope)
            log_success(run_id, started_at, rows_written, rows_written, 0, dq_results, True)
            return True

        except Exception as exc:
            msg     = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
            is_last = attempt == retry_max_attempts
            log("ERROR", f"Attempt {attempt} failed: {exc}", table=cfg.table_id)
            log_failure(run_id, started_at, "UNHANDLED_EXCEPTION",
                        msg, status="FAILED" if is_last else "RUNNING")
            if is_last:
                # _alerter.send(cfg, run_id, "FAILED", msg, "CRITICAL")
                return False

        log("INFO", f"Retrying in {10:.0f}s …")
        time.sleep(10)

    return False


    =========================================
    
def log_start(
    run_id: str,
    cfg: TableConfig,
    run_date: date,
    dag_id: str,
    attempt: int,
    watermark_start: Optional[datetime] = None,
):
    spark = SparkSession.getActiveSession()
    now = datetime.now(timezone.utc).isoformat()
    row = {
        "run_id": run_id, "dataset_id": cfg.table_id,
        "dag_run_date": str(run_date), "dag_id": dag_id,
        "task_id": cfg.table_id, "dataset_layer": cfg.target_dataset_layer,
        "opco": cfg.opco, "table_name": cfg.target_table_name,
        "status": "RUNNING", "attempt_number": attempt,
        "rows_read": None, "rows_written": None, "rows_rejected": None,
        "rows_merged": None, "rows_inserted": None, "rows_updated": None,
        "rows_deleted": None,
        "watermark_start": watermark_start.isoformat() if watermark_start else None,
        "watermark_end": None, "started_at": now, "completed_at": None,
        "duration_seconds": None, "dq_results": None, "dq_passed": None,
        "error_code": None, "error_message": None,
        "spark_app_id": spark.sparkContext.applicationId,
        "cluster_id": os.getenv("CLUSTER_ID", "unknown"),
        "created_at": now, "updated_at": now,
    }
    _upsert_run_log(row)


def log_success(
    run_id: str,
    started_at: datetime,
    rows_read: int,
    rows_written: int,
    rows_rejected: int,
    dq_results: list,
    dq_passed: bool,
    watermark_end: Optional[datetime] = None,
):
    now      = datetime.now(timezone.utc)
    duration = int((now - started_at).total_seconds())
    spark = SparkSession.getActiveSession()
    safe_dq  = json.dumps(dq_results).replace("'", "\\'")
    spark.sql(f"""
        UPDATE {METADATA_DB}.cdl_table_run_log
        SET status='SUCCESS', rows_read={rows_read}, rows_written={rows_written},
            rows_rejected={rows_rejected}, completed_at='{now.isoformat()}',
            duration_seconds={duration},
            dq_results='{safe_dq}',
            dq_passed={str(dq_passed)},
            watermark_end={'NULL' if not watermark_end else f"'{watermark_end.isoformat()}'"},
            updated_at='{now.isoformat()}'
        WHERE run_id='{run_id}'
    """)


def log_failure(
    run_id: str,
    started_at: Optional[datetime],
    error_code: str,
    error_message: str,
    status: str = "FAILED",
):
    now      = datetime.now(timezone.utc)
    duration = int((now - started_at).total_seconds()) if started_at else 0
    spark = SparkSession.getActiveSession()
    safe_msg = error_message[:2000].replace("'", "\\'")
    spark.sql(f"""
        UPDATE {METADATA_DB}.cdl_table_run_log
        SET status='{status}', completed_at='{now.isoformat()}',
            duration_seconds={duration}, dq_passed=false,
            error_code='{error_code}', error_message='{safe_msg}',
            updated_at='{now.isoformat()}'
        WHERE run_id='{run_id}'
    """)


def _upsert_run_log(row: dict):
    spark = SparkSession.getActiveSession()
    try:
        df = spark.createDataFrame([row])
        DeltaTable.forName(spark, f"{METADATA_DB}.cdl_table_run_log").alias("t").merge(
            df.alias("s"),
            "t.run_id = s.run_id AND t.attempt_number = s.attempt_number"
        ).whenNotMatchedInsertAll().execute()
    except Exception as e:
        log("ERROR", f"Failed to write run log: {e}")
