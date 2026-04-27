# Databricks notebook source
# DBTITLE 1, Run base framework
# MAGIC %run ./nb_validation_orchestrator

# COMMAND ----------

# =============================================================================
# HOW THIS WORKS — Architecture Overview
# =============================================================================
#
#  ValidationOrchestrator          ← Entry point. Validates inputs, returns a Builder.
#         │
#         └─► ValidationBuilder    ← Fluent builder. Holds all base check methods
#                                    + _steps list + build_report().
#
#  SdlValidationOrchestrator       ← Project-level orchestrator.
#    extends ValidationOrchestrator   Inherits: for_files(), for_dataframes(),
#                                      all input validation helpers.
#    Override: for_files()            Returns SdlValidationBuilder instead of
#                                      the base ValidationBuilder, so SDL custom
#                                      checks are available in the chain.
#
#  SdlValidationBuilder            ← Project-level builder.
#    extends ValidationBuilder        Inherits: ALL base check methods.
#                                     Adds:     SDL-specific check methods.
#                                     Holds:    SdlChecks helper instance.
#
#  SdlChecks                       ← Stateless helper class.
#                                    Contains the actual logic for SDL-specific
#                                    checks. Returns ValidationResult (just like
#                                    FileChecks / SchemaChecks / DataChecks).
#
# Usage:
#   report = (
#       SdlValidationOrchestrator(spark)
#       .for_files(SOURCE_FILES_PATH, regex)   # ← returns SdlValidationBuilder
#       .check_file_count()                    # ← inherited base check
#       .find_start_cell(keywords=[...])       # ← SDL custom check
#       .check_required_columns([...])         # ← inherited base check
#       .build_report()                        # ← inherited, runs all steps
#   )
#
# =============================================================================


# =============================================================================
# SDL CUSTOM CHECKS  (stateless helper — mirrors FileChecks / DataChecks style)
# =============================================================================

class SdlChecks:
    """
    Stateless helper containing SDL-specific validation logic.
    Each method receives what it needs and returns a ValidationResult,
    identical to the pattern used by FileChecks / SchemaChecks / DataChecks.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    # -------------------------------------------------------------------------
    def _read_csv_raw(self, file_path: str, start_row_idx: int):
        """
        Reads a CSV without headers (raw), adds a monotonic row id,
        and filters from start_row_idx onwards.
        Internal helper — not a check.
        """
        try:
            return (
                self.spark.read
                .option("header", "False")
                .option("inferSchema", "False")
                .csv(file_path)
                .withColumn("_row_id", monotonically_increasing_id())
                .filter(col("_row_id") >= start_row_idx)
            )
        except Exception as e:
            raise Exception(f"_read_csv_raw failed: {str(e)}")

    # -------------------------------------------------------------------------
    def _read_cell(self, file_path: str, col_idx: int, row_idx: int):
        """
        Reads the value at (col_idx, row_idx) from the raw CSV.
        row_idx is 1-based (row 1 = first row of file).
        Internal helper — not a check.
        """
        try:
            df = self._read_csv_raw(file_path, start_row_idx=0)
            if df is None:
                return None
            columns = df.columns
            if col_idx >= len(columns):
                return None
            row = (
                df.filter(col("_row_id") == row_idx - 1)   # convert to 0-based
                .select(columns[col_idx])
                .limit(1)
                .collect()
            )
            return row[0][0] if row else None
        except Exception as e:
            raise Exception(f"_read_cell failed at ({col_idx},{row_idx}): {str(e)}")

    # -------------------------------------------------------------------------
    def find_start_cell(
        self,
        file_dfs: dict,
        file_path: str,
        keywords: list,
        max_rows: int = 10,
        max_cols: int = 4,
    ) -> ValidationResult:
        """
        Scans the raw CSV (not the parsed DataFrame) to find the 'header anchor'
        cell matching the given keywords, then validates the structure around it.

        Rules (same as original SDL logic):
          - Cell value starts with one of `keywords` (case-insensitive, stripped)
          - The cell BELOW is empty / null  (section header, not a data column)
          - The cell to the RIGHT starts with 'rev', 'adv', or 'wpp'

        Returns a ValidationResult with the found coordinates in the message,
        or FAIL with a descriptive reason.
        """
        try:
            if not keywords or not isinstance(keywords, list):
                return ValidationResult(
                    "FIND_START_CELL",
                    "FAIL",
                    "keywords must be a non-empty list of strings"
                )

            normalised_keywords = [str(k).strip().lower() for k in keywords]

            for col_idx in range(max_cols):
                for row_idx in range(1, max_rows + 1):

                    curr = self._read_cell(file_path, col_idx, row_idx)
                    if curr is None:
                        continue

                    curr_str = str(curr).strip().lower()
                    if not any(curr_str.startswith(kw) for kw in normalised_keywords):
                        continue

                    # Cell below must be empty / null
                    below = self._read_cell(file_path, col_idx, row_idx + 1)
                    if below not in (None, "", "null"):
                        continue

                    # Cell to the right must start with rev / adv / wpp
                    right = self._read_cell(file_path, col_idx + 1, row_idx)
                    if right is None:
                        continue
                    if not str(right).strip().lower().startswith(("rev", "adv", "wpp")):
                        continue

                    return ValidationResult(
                        "FIND_START_CELL",
                        "PASS",
                        (
                            f"Start cell found at row={row_idx}, col={col_idx} "
                            f"with value='{curr}'"
                        )
                    )

            # Exhausted search space
            return ValidationResult(
                "FIND_START_CELL",
                "FAIL",
                (
                    f"Start cell could not be found using keywords={keywords} "
                    f"within first {max_rows} rows / {max_cols} cols"
                )
            )

        except Exception as e:
            return ValidationResult(
                "FIND_START_CELL",
                "FAIL",
                f"find_start_cell encountered an error: {str(e)}"
            )

    # -------------------------------------------------------------------------
    # ✏️  ADD MORE SDL-SPECIFIC CHECKS BELOW — follow the same pattern:
    #     def check_xyz(self, file_dfs, ...) -> ValidationResult: ...
    # -------------------------------------------------------------------------

    def check_sdl_version_cell(
        self,
        file_path: str,
        expected_prefix: str = "v",
        version_col_idx: int = 1,
        version_row_idx: int = 1,
    ) -> ValidationResult:
        """
        Example SDL-specific check: verifies that the version cell (default B1)
        starts with the expected prefix (default 'v').
        """
        try:
            value = self._read_cell(file_path, version_col_idx, version_row_idx)
            if value is None:
                return ValidationResult(
                    "SDL_VERSION_CELL_CHECK",
                    "FAIL",
                    f"Version cell at col={version_col_idx}, row={version_row_idx} is empty"
                )
            if not str(value).strip().lower().startswith(expected_prefix.lower()):
                return ValidationResult(
                    "SDL_VERSION_CELL_CHECK",
                    "FAIL",
                    f"Version cell value '{value}' does not start with '{expected_prefix}'"
                )
            return ValidationResult(
                "SDL_VERSION_CELL_CHECK",
                "PASS",
                f"Version cell value '{value}' is valid"
            )
        except Exception as e:
            return ValidationResult(
                "SDL_VERSION_CELL_CHECK",
                "FAIL",
                f"check_sdl_version_cell failed: {str(e)}"
            )


# =============================================================================
# SDL BUILDER  — inherits ALL base checks, adds SDL-specific chainable methods
# =============================================================================

class SdlValidationBuilder(ValidationBuilder):
    """
    Extends ValidationBuilder with SDL-specific chainable check methods.

    Inherits (ready to use, no changes needed):
        check_file_count(), check_size(), check_required_columns(),
        check_null(), check_date_format(), check_duplicate_rows(), ...
        build_report()

    Adds:
        find_start_cell(keywords, ...)
        check_sdl_version_cell(...)
        — add more below as the SDL project grows —
    """

    def __init__(self, spark: SparkSession, file_path: str, regex: str, df_list: list):
        # Reuse entire base __init__ (spark, file_path, regex, df_list,
        # _df_cache, file/schema/data helpers, _steps list)
        super().__init__(spark, file_path, regex, df_list)

        # SDL-specific helper (equivalent to self.file / self.schema / self.data)
        self.sdl = SdlChecks(spark)

    # ------------------------------------------------------------------
    # Chainable SDL checks
    # ------------------------------------------------------------------

    def find_start_cell(
        self,
        keywords: list,
        max_rows: int = 10,
        max_cols: int = 4,
    ):
        """
        Chainable wrapper around SdlChecks.find_start_cell().

        Parameters
        ----------
        keywords : list[str]
            Strings that the anchor cell value must start with.
        max_rows : int
            How many rows to scan (default 10).
        max_cols : int
            How many columns to scan (default 4).
        """
        # Capture loop variables immediately to avoid late-binding closure bug
        _keywords = keywords
        _max_rows = max_rows
        _max_cols = max_cols
        _file_path = self.file_path

        self._steps.append(
            lambda report: report.add(
                self.sdl.find_start_cell(
                    file_dfs=self._get_file_dfs(),
                    file_path=_file_path,
                    keywords=_keywords,
                    max_rows=_max_rows,
                    max_cols=_max_cols,
                )
            )
        )
        return self

    def check_sdl_version_cell(
        self,
        expected_prefix: str = "v",
        version_col_idx: int = 1,
        version_row_idx: int = 1,
    ):
        """Chainable wrapper around SdlChecks.check_sdl_version_cell()."""
        _prefix = expected_prefix
        _col = version_col_idx
        _row = version_row_idx
        _file_path = self.file_path

        self._steps.append(
            lambda report: report.add(
                self.sdl.check_sdl_version_cell(
                    file_path=_file_path,
                    expected_prefix=_prefix,
                    version_col_idx=_col,
                    version_row_idx=_row,
                )
            )
        )
        return self

    # ✏️  Add more SDL chainable wrappers here following the same pattern


# =============================================================================
# SDL ORCHESTRATOR  — entry point for the SDL project
# =============================================================================

class SdlValidationOrchestrator(ValidationOrchestrator):
    """
    Project-level orchestrator for SDL pipelines.

    Inherits from ValidationOrchestrator:
        - for_files()           ← OVERRIDDEN to return SdlValidationBuilder
        - for_dataframes()      ← inherited as-is (returns base ValidationBuilder)
        - all _validate_* helpers

    Usage
    -----
        report = (
            SdlValidationOrchestrator(spark)
            .for_files(SOURCE_FILES_PATH, regex)
            .check_file_count()                    # base check
            .find_start_cell(keywords=["header"])  # SDL custom check
            .check_required_columns(["col_a"])     # base check
            .build_report()
        )
    """

    def __init__(self, spark: SparkSession):
        # Only spark is needed at this stage — file_path / regex come from for_files()
        super().__init__(spark)

    # ------------------------------------------------------------------
    def for_files(
        self,
        file_path: str,
        regex: str,
        df_list: Optional[List[DataFrame]] = None,
    ) -> SdlValidationBuilder:
        """
        Validates inputs (same as base) then returns an SdlValidationBuilder
        so that SDL-specific checks are available in the fluent chain.
        """
        try:
            self._validate_required_params(file_path, regex)

            is_valid, message = self._validate_regex(regex)
            if not is_valid:
                raise ValidationError(f"FAIL: {message}")

            if df_list is not None:
                is_valid_df, msg_df = self._validate_df_list(df_list)
                if not is_valid_df:
                    raise ValidationError(f"FAIL: {msg_df}")

            self._validate_file_path(file_path, regex)

            # ← Key difference: return SdlValidationBuilder, not ValidationBuilder
            return SdlValidationBuilder(self.spark, file_path, regex, df_list=df_list)

        except ValidationError:
            raise
        except Exception as e:
            raise ValidationError(f"SdlValidationOrchestrator.for_files failed: {str(e)}")


# =============================================================================
# EXAMPLE USAGE
# =============================================================================

# SOURCE_FILES_PATH = (
#     "abfss://datalake@stadslabsdl001.dfs.core.windows.net"
#     "/COMN_COE/Private/Inbox/archive/SDL/A4T/ingest_dt=2026-04-19/"
# )
# regex = r"^ec_model_new\.csv$"
#
# report = (
#     SdlValidationOrchestrator(spark)
#     .for_files(SOURCE_FILES_PATH, regex)
#
#     # ── Inherited base checks ──────────────────────────────────────────────
#     .check_file_count()
#     .check_size(min_size=100, max_size=50_000_000)
#
#     # ── SDL custom checks ──────────────────────────────────────────────────
#     .find_start_cell(keywords=["header", "model"])
#     .check_sdl_version_cell(expected_prefix="v")
#
#     # ── More inherited base checks ─────────────────────────────────────────
#     .check_required_columns(["model_id", "revenue"])
#     .check_null(["model_id"])
#     .check_no_future_date("effective_date", "yyyy-MM-dd")
#
#     .build_report()
# )
#
# report.raise_if_failed()   # stops notebook on any FAIL
