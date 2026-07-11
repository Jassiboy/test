def run_layer(
    dag_id: str, opco: str, layer: str, run_date: date, skip_gate: bool,
    table_id: [list[str]] ,
    windows: list[int] ) -> bool:

    all_ok = True

    configs = read_table_configs(table_id=table_id, schedule_group = None)

    if not table_id:
        log("WARN", "No table_id", layer=layer, opco=opco)
        return all_ok

    if not skip_gate:
        # gate_ok, blocked_by = check_dq_gate(configs, run_date)
        gate_ok,blocked_by = True, []
        if not gate_ok:
            log("ERROR", f"DQ gate blocked {layer}: upstream not ready {blocked_by}")
            return False

    for cfg in configs:
        stage_table = f'{cfg.target_table_name}_stage'
        Truncate stage_table
        dbutlis,rm(stage_loaction)
        for mnth in windows:
            tdf = get_transformed_df(opco, layer, cfg.target_table_name,cfg.load_process, run_date, mnth)
            display(tdf)
            print(cfg)
            tdf.write.format('delta').mode('append').saveAsTable(stage_table)
            # ok = run_ingestion(tdf, cfg, run_date, dag_id, start_date, end_date)
            ok = True
            log(f"INFO loading {mnth}" if ok else "ERROR", f"  {'✓' if ok else '✗ FAILED'} {cfg.table_id}")
            all_ok = all_ok and ok

    return all_ok


def run_ingestion(
    df:        DataFrame,
    cfg:       TableConfig,
    load_mnth: str,
    dag_id:    str,
    run_date:  str,
) -> bool:
    """
    Delete existing rows for (cfg.opco, load_mnth) in the target table, then
    append df. Retries retry_max_attempts times with exponential backoff
    (each retry re-does the delete + append, so it's safe to repeat).
    """
    target = f"{DATA_DB}.{cfg.target_table_name}"

    for attempt in range(1, retry_max_attempts + 1):
        started_at = datetime.now(timezone.utc)
        run_id = make_run_id(dag_id, cfg.table_id, run_date, attempt)

        log("INFO", f"Ingestion [{load_mnth}] attempt {attempt}/{retry_max_attempts}",
            table=cfg.table_id, run_id=run_id)
        log_start(run_id, cfg, run_date, dag_id, attempt)

        try:
            rows_read = df.count()
            log("INFO", f"Rows to load: {rows_read}", table=cfg.table_id)

            if rows_read == 0:
                log("INFO", "No rows — skipping ingestion", table=cfg.table_id)
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
            # _alerter.send(cfg, run_id, "FAILED",
            #               f"DQ threshold not met for {cfg.table_id}", "CRITICAL")


        except Exception as exc:
            msg     = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
            is_last = attempt == retry_max_attempts
            log("ERROR", f"Attempt {attempt} failed: {exc}", table=cfg.table_id)
            log_failure(run_id, started_at, "UNHANDLED_EXCEPTION",
                        msg, status="FAILED" if is_last else "RUNNING")
            if is_last:
                # _alerter.send(cfg, run_id, "FAILED", msg, "CRITICAL")
                return False

        if attempt == retry_max_attempts:
            return False
        # wait = min(5 * (cfg.retry_backoff ** (attempt - 1)), cfg.retry_max_wait)
        # wait += random.uniform(0, wait * 0.1)
        log("INFO", f"Retrying in {10:.0f}s …")
        time.sleep(10)

    return False
