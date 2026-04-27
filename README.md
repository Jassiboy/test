# Databricks notebook source
# DBTITLE 1, Run base framework
# MAGIC %run ./nb_validation_orchestrator

# COMMAND ----------

# =============================================================================
#  THIS IS THE ONLY FILE YOUR PROJECT NEEDS TO WRITE
#  ─────────────────────────────────────────────────
#  1. Write a plain class with your custom check methods.
#  2. Each method must return a ValidationResult.
#  3. Use  self.spark / self.file_path / self._get_file_dfs()
#     — the framework injects these automatically.
#  4. Pass an instance to  .with_checks(...)  and you're done.
#     All your methods appear in the chain alongside base checks.
# =============================================================================


class SdlChecks:
    """
    SDL-specific validation checks.

    Rules
    -----
    - Every method MUST return a ValidationResult(check_name, status, message).
    - Use self.spark, self.file_path, self._get_file_dfs() freely —
      the framework sets these before any check runs.
    - No __init__ required unless you need project-specific config.
    """

    # ── Internal helpers (not checks) ────────────────────────────────────────

    def _read_raw_csv(self, start_row_idx: int = 0):
        """Read the file without headers, with a monotonic row id."""
        return (
            self.spark.read
            .option("header", "False")
            .option("inferSchema", "False")
            .csv(self.file_path)
            .withColumn("_row_id", monotonically_increasing_id())
            .filter(col("_row_id") >= start_row_idx)
        )

    def _read_cell(self, col_idx: int, row_idx: int):
        """Return the value at (col_idx, row_idx).  row_idx is 1-based."""
        df   = self._read_raw_csv()
        cols = df.columns
        if col_idx >= len(cols):
            return None
        rows = (
            df.filter(col("_row_id") == row_idx - 1)
            .select(cols[col_idx])
            .limit(1)
            .collect()
        )
        return rows[0][0] if rows else None

    # ── Custom checks ─────────────────────────────────────────────────────────

    def find_start_cell(
        self,
        keywords: list,
        max_rows: int = 10,
        max_cols: int = 4,
    ) -> ValidationResult:
        """
        Scan the raw CSV for an 'anchor' cell that:
          • starts with one of `keywords`  (case-insensitive)
          • has an empty cell directly below it
          • has a cell to the right starting with 'rev', 'adv', or 'wpp'
        """
        try:
            if not keywords or not isinstance(keywords, list):
                return ValidationResult(
                    "FIND_START_CELL", "FAIL",
                    "keywords must be a non-empty list of strings"
                )

            normalised = [str(k).strip().lower() for k in keywords]

            for col_idx in range(max_cols):
                for row_idx in range(1, max_rows + 1):
                    curr = self._read_cell(col_idx, row_idx)
                    if curr is None:
                        continue
                    if not any(str(curr).strip().lower().startswith(k) for k in normalised):
                        continue
                    below = self._read_cell(col_idx, row_idx + 1)
                    if below not in (None, "", "null"):
                        continue
                    right = self._read_cell(col_idx + 1, row_idx)
                    if right is None:
                        continue
                    if not str(right).strip().lower().startswith(("rev", "adv", "wpp")):
                        continue
                    return ValidationResult(
                        "FIND_START_CELL", "PASS",
                        f"Start cell found at row={row_idx}, col={col_idx}, value='{curr}'"
                    )

            return ValidationResult(
                "FIND_START_CELL", "FAIL",
                f"No start cell found for keywords={keywords} "
                f"within {max_rows} rows / {max_cols} cols"
            )
        except Exception as e:
            return ValidationResult("FIND_START_CELL", "FAIL", str(e))

    def check_version_cell(
        self,
        expected_prefix: str = "v",
        col_idx: int = 1,
        row_idx: int = 1,
    ) -> ValidationResult:
        """Verify that the version cell starts with `expected_prefix`."""
        try:
            value = self._read_cell(col_idx, row_idx)
            if value is None:
                return ValidationResult(
                    "VERSION_CELL_CHECK", "FAIL",
                    f"Version cell at col={col_idx}, row={row_idx} is empty"
                )
            if not str(value).strip().lower().startswith(expected_prefix.lower()):
                return ValidationResult(
                    "VERSION_CELL_CHECK", "FAIL",
                    f"Expected prefix '{expected_prefix}', got '{value}'"
                )
            return ValidationResult(
                "VERSION_CELL_CHECK", "PASS",
                f"Version cell value '{value}' is valid"
            )
        except Exception as e:
            return ValidationResult("VERSION_CELL_CHECK", "FAIL", str(e))

    # ✏️  Add more SDL checks below — same pattern, always return ValidationResult


# =============================================================================
#  USAGE
# =============================================================================

SOURCE_FILES_PATH = (
    "abfss://datalake@stadslabsdl001.dfs.core.windows.net"
    "/COMN_COE/Private/Inbox/archive/SDL/A4T/ingest_dt=2026-04-19/"
)
regex = r"^ec_model_new\.csv$"

report = (
    ValidationOrchestrator(spark)
    .with_checks(SdlChecks())              # ← register your checks once

    .for_files(SOURCE_FILES_PATH, regex)

    # ── Base checks (inherited) ──────────────────────────────────────────────
    .check_file_count()
    .check_size(min_size=100, max_size=50_000_000)
    .check_required_columns(["model_id", "revenue"])
    .check_null(["model_id"])

    # ── SDL custom checks (auto-discovered from SdlChecks) ───────────────────
    .find_start_cell(keywords=["header", "model"])
    .check_version_cell(expected_prefix="v")

    # ── More base checks ─────────────────────────────────────────────────────
    .check_no_future_date("effective_date", "yyyy-MM-dd")
    .check_duplicate_rows()

    .build_report()
)

report.raise_if_failed()
