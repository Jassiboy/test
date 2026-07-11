"""
dq_rules.py
=============
One function per `dq_rule` name that shows up in
cdl_table_config.data_quality_rules, e.g.:

    [{ "dq_rule": "dq_net_rev", "error_threshold_pct": 0.0 }]

The framework (run_dq_on_stage, in 01_framework_core.py) looks up each rule
by name in this file and calls it — you don't touch the framework to add a
new rule, you just add a function here with a matching name.

CONTRACT every rule function must follow:
    fn(spark, stage_table: str, source_table: str, opco: str, load_mnth: str) -> float

    Returns a diff_pct: how far apart stage and source are on whatever
    metric this rule checks (0.0 = identical). The framework compares this
    return value to the rule's error_threshold_pct — the rule function
    itself doesn't decide pass/fail, it just measures.
"""


def _pct_diff(expected, actual) -> float:
    """Shared helper: % difference between an expected and actual value."""
    if expected in (None, 0):
        return 0.0 if actual in (None, 0) else 100.0
    return abs(float(expected) - float(actual)) / float(expected) * 100


def _scalar(spark, sql: str):
    return spark.sql(sql).collect()[0][0]


# ─────────────────────────────────────────────────────────────────────────────
# Row count — stage vs source, same (opco, load_mnth) scope
# ─────────────────────────────────────────────────────────────────────────────
def dq_row_count(spark, stage_table, source_table, opco, load_mnth) -> float:
    scope = f"opco = '{opco}' AND load_mnth = '{load_mnth}'"
    stage_cnt  = _scalar(spark, f"SELECT COUNT(*) FROM {stage_table}  WHERE {scope}")
    source_cnt = _scalar(spark, f"SELECT COUNT(*) FROM {source_table} WHERE {scope}")
    return _pct_diff(source_cnt, stage_cnt)


# ─────────────────────────────────────────────────────────────────────────────
# Net revenue — example business-metric rule (matches the config example)
# ─────────────────────────────────────────────────────────────────────────────
def dq_net_rev(spark, stage_table, source_table, opco, load_mnth) -> float:
    scope = f"opco = '{opco}' AND load_mnth = '{load_mnth}'"
    stage_rev  = _scalar(spark, f"SELECT SUM(net_revenue) FROM {stage_table}  WHERE {scope}") or 0
    source_rev = _scalar(spark, f"SELECT SUM(net_revenue) FROM {source_table} WHERE {scope}") or 0
    return _pct_diff(source_rev, stage_rev)


# ─────────────────────────────────────────────────────────────────────────────
# Add more rules the same way, e.g.:
#
# def dq_distinct_accounts(spark, stage_table, source_table, opco, load_mnth) -> float:
#     scope = f"opco = '{opco}' AND load_mnth = '{load_mnth}'"
#     stage_ct  = _scalar(spark, f"SELECT COUNT(DISTINCT account_id) FROM {stage_table}  WHERE {scope}")
#     source_ct = _scalar(spark, f"SELECT COUNT(DISTINCT account_id) FROM {source_table} WHERE {scope}")
#     return _pct_diff(source_ct, stage_ct)
# ─────────────────────────────────────────────────────────────────────────────



==========
"""
Add this to notebooks/01_framework_core.py.

Reads cfg.dq_rules (parsed from cdl_table_config.data_quality_rules, e.g.
[{ "dq_rule": "dq_net_rev", "error_threshold_pct": 0.0 }]) and runs each
named rule — found by name in dq_rules.py — against the stage table vs the
source table, for one month.
"""

import importlib
from pyspark.sql import SparkSession

_dq_rules_module = None  # loaded lazily, cached after first use


def run_dq_on_stage(cfg: "TableConfig", opco: str, load_mnth: str,
                     stage_table: str, source_table: str) -> tuple[bool, list[dict]]:
    """
    Run every rule in cfg.dq_rules against (stage_table, source_table) for
    this (opco, load_mnth). Returns (all_rules_passed, per_rule_results).

    Each rule dict looks like {"dq_rule": "dq_net_rev", "error_threshold_pct": 0.0}.
    `dq_rule` is the function name to look up in dq_rules.py; the framework
    calls it and compares its returned diff_pct to error_threshold_pct — the
    rule function itself only measures, it doesn't decide pass/fail.
    """
    global _dq_rules_module
    if _dq_rules_module is None:
        _dq_rules_module = importlib.import_module("dq_rules")

    if not cfg.dq_rules:
        return True, []

    spark = SparkSession.getActiveSession()
    results = []
    overall_ok = True

    for rule in cfg.dq_rules:
        rule_name = rule["dq_rule"]
        threshold = float(rule.get("error_threshold_pct", 0.0))
        fn = getattr(_dq_rules_module, rule_name, None)

        if fn is None:
            log("ERROR", f"DQ rule '{rule_name}' not found in dq_rules.py", table=cfg.table_id)
            results.append({"dq_rule": rule_name, "threshold_pct": threshold,
                             "diff_pct": None, "passed": False, "error": "rule not found"})
            overall_ok = False
            continue

        try:
            diff_pct = fn(spark, stage_table, source_table, opco, load_mnth)
            passed = diff_pct <= threshold
        except Exception as exc:
            log("ERROR", f"DQ rule '{rule_name}' raised: {exc}", table=cfg.table_id)
            diff_pct, passed = None, False

        results.append({"dq_rule": rule_name, "threshold_pct": threshold,
                         "diff_pct": diff_pct, "passed": passed})
        overall_ok = overall_ok and passed

    return overall_ok, results

========
import time
import traceback
from datetime import date, datetime, timezone

from pyspark.sql import SparkSession
from delta.tables import DeltaTable

# from wherever these already live in your project:
# read_table_configs, get_transformed_df, make_run_id, log, log_start,
# log_success, log_failure, run_dq_on_stage, DATA_DB


def run_layer(
    dag_id: str, opco: str, layer: str, run_date: date, skip_gate: bool,
    table_id: list[str],
    windows: list[int],
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
        stage_table  = f"{DATA_DB}.{cfg.target_table_name}_stage"
        source_table = f"{cfg.source_system}.{cfg.source_table_name}"

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
    scope  = f"opco = '{opco}' AND load_mnth = '{load_mnth}'"

    for attempt in range(1, cfg.retry_max_attempts + 1):
        started_at = datetime.now(timezone.utc)
        run_id = make_run_id(dag_id, cfg.table_id, load_mnth, attempt)
        log("INFO", f"DQ + promote [{load_mnth}] attempt {attempt}/{cfg.retry_max_attempts}",
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
            is_last = attempt == cfg.retry_max_attempts
            log("ERROR", f"Attempt {attempt} failed: {exc}", table=cfg.table_id)
            log_failure(run_id, started_at, "UNHANDLED_EXCEPTION",
                        msg, status="FAILED" if is_last else "RUNNING")
            if is_last:
                # _alerter.send(cfg, run_id, "FAILED", msg, "CRITICAL")
                return False

        wait = min(cfg.retry_wait_sec * (cfg.retry_backoff ** (attempt - 1)), cfg.retry_max_wait)
        log("INFO", f"Retrying in {wait:.0f}s …")
        time.sleep(wait)

    return False


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def _reset_stage(stage_table: str):
    spark = SparkSession.getActiveSession()
    if DeltaTable.isDeltaTable(spark, stage_table):
        spark.sql(f"TRUNCATE TABLE {stage_table}")
        log("INFO", f"Truncated {stage_table}")


def _promote_to_target(cfg: "TableConfig", target: str, stage_table: str, scope: str) -> int:
    """Atomic overwrite of just this month's partition in target, from stage."""
    spark = SparkSession.getActiveSession()
    stage_df = spark.table(stage_table).where(scope)
    (stage_df.write.format("delta").mode("overwrite")
        .option("replaceWhere", scope)
        .saveAsTable(target))
    ct = stage_df.count()
    log("INFO", f"Promoted {scope} → {target}: {ct} rows", table=cfg.table_id)
    return ct
