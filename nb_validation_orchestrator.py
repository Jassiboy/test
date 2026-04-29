# Databricks notebook source
# DBTITLE 1,Run nb_all_checks
# MAGIC %run ./nb_all_checks

# COMMAND ----------

# ============================================================
# CHANGE LOG
# ============================================================
# [FIX-01]  Added `Tuple` to typing imports (was missing).
# [FIX-02]  raise_if_failed now raises ValidationError instead of bare Exception.
# [FIX-03]  print_summary: `details` variable was computed but never used —
#           r.message was printed raw, causing long messages to overflow the
#           table.  Now uses the truncated `details` variable.
# [FIX-04]  build_report: moved df.unpersist() into a `finally` block so Spark
#           memory is always released even when a check raises mid-run.
# [FIX-05]  _validate_file_path + _get_file_dfs: both now use pattern.match()
#           (was pattern.search() in orchestrator, pattern.match() in builder —
#           inconsistency caused files to pass orchestrator validation but be
#           silently skipped by the builder).
# [FIX-06]  ValidationBuilderChild: replaced copy-pasted field declarations with
#           a proper super().__init__() call so any future field added to
#           ValidationBuilder is automatically inherited.
# [NEW-01]  base_transform support in for_files():
#             - df_list parameter removed from for_files() (use for_dataframes()
#               if you already have DataFrames).
#             - New optional `base_transform: Callable[[DataFrame, str], DataFrame]`
#               parameter.  When supplied, every file DataFrame is passed through
#               the function after reading and before caching.
#             - Plugin's _get_file_dfs is automatically the transformed version,
#               so custom check methods see transformed data with no extra wiring.
# [NEW-02]  base_transform support in for_dataframes():
#             - New optional `base_transform` parameter.  Applied to each
#               DataFrame in df_list before caching.  Useful when the caller
#               passes raw DataFrames but still wants a common transform applied
#               centrally.
# [NEW-03]  base_transform support in plugin (register_checks):
#             - Plugin methods call self._get_file_dfs() which already returns
#               the transformed cache — no extra wiring needed.
#             - Added `plugin.base_transform` attribute so plugin methods can
#               inspect or re-apply the transform if needed.
# [NEW-04]  ValidationReport.to_dataframe(spark) — exports results as a Spark
#           DataFrame for audit logging / Delta table writes.
# [NEW-05]  _validate_df_list: checks df.rdd.isEmpty() replaced with
#           df.limit(1).count() == 0 to avoid a full RDD materialisation.
# ============================================================

import re
import os
from dataclasses import dataclass, field
from typing import List, Optional, Callable, Dict, Tuple   # [FIX-01] Tuple added

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

# COMMAND ----------

# DBTITLE 1,ValidationError
class ValidationError(Exception):
    """Custom exception — shows only the message, no traceback in Databricks."""
    def _render_traceback_(self):
        return [f'\033[0;31mValidationError: {self}\033[0m']


# COMMAND ----------

# DBTITLE 1,ValidationReport
@dataclass
class ValidationReport:
    results: List[ValidationResult] = field(default_factory=list)

    def add(self, result: ValidationResult):
        self.results.append(result)

    def is_ok(self) -> bool:
        return all(r.status == "PASS" for r in self.results)

    def print_summary(self):
        TABLE_WIDTH = 86
        CHECK_COL   = 28
        STATUS_COL  = 8
        DETAILS_COL = 40

        def truncate(text, width):
            return text if len(text) <= width else text[:width - 3] + "..."

        print("=" * TABLE_WIDTH)
        print(" " * 22 + "VALIDATION REPORT")
        print("=" * TABLE_WIDTH)
        print()
        print(f"| {'Check Name'.ljust(CHECK_COL)} | "
              f"{'Status'.ljust(STATUS_COL)} | "
              f"{'Details'.ljust(DETAILS_COL)} |")
        print(f"|{'-' * (CHECK_COL + 2)}|"
              f"{'-' * (STATUS_COL + 2)}|"
              f"{'-' * (DETAILS_COL + 2)}|")

        for r in self.results:
            details = truncate(r.message, DETAILS_COL)    # [FIX-03] now used
            print(f"| {r.check_name.ljust(CHECK_COL)} | "
                  f"{r.status.ljust(STATUS_COL)} | "
                  f"{details.ljust(DETAILS_COL)} |")       # [FIX-03] details not r.message

        print()
        print("-" * TABLE_WIDTH)
        if self.is_ok():
            print("OVERALL RESULT : PASS (All checks successful)")
        else:
            print("OVERALL RESULT : FAIL (One or more checks failed)")
        print("=" * TABLE_WIDTH)

    def raise_if_failed(self):
        """Raise ValidationError if any check failed."""  # [FIX-02]
        if not self.is_ok():
            failed = [r for r in self.results if r.status == "FAIL"]
            msg = "; ".join(f"{r.check_name}: {r.message}" for r in failed)
            raise ValidationError(f"Validation Failed: {msg}")   # [FIX-02] was bare Exception

    # [NEW-04] Export results as a Spark DataFrame for audit logging.
    def to_dataframe(self, spark: SparkSession) -> DataFrame:
        """
        Returns a Spark DataFrame with columns (check_name, status, message).
        Useful for writing validation results to a Delta audit table.

        Example
        -------
            report.to_dataframe(spark).write.format("delta").mode("append").save(audit_path)
        """
        rows = [(r.check_name, r.status, r.message) for r in self.results]
        return spark.createDataFrame(rows, ["check_name", "status", "message"])


# COMMAND ----------

# =========================================================
# BUILDER (METHOD CHAINING)
# =========================================================

class ValidationBuilder:
    """
    Fluent builder returned by ValidationOrchestrator.for_files().

    Parameters
    ----------
    spark          : active SparkSession
    file_path      : ABFSS / DBFS landing path
    regex          : filename filter regex
    base_transform : optional (df, path) -> df applied after reading each file  [NEW-01]
    """

    def __init__(
        self,
        spark: SparkSession,
        file_path: str,
        regex: str,
        base_transform: Optional[Callable[[DataFrame, str], DataFrame]] = None,  # [NEW-01]
    ):
        self.spark          = spark
        self.file_path      = file_path
        self.regex          = regex
        self.base_transform = base_transform   # [NEW-01]
        self._df_cache: Dict[str, DataFrame] = {}

        self.file   = FileChecks(spark)
        self.schema = SchemaChecks(spark)
        self.data   = DataChecks(spark)

        self._steps: List[Callable[[ValidationReport], None]] = []
        self._df: Optional[DataFrame] = None
        self._plugin = None

    # -------------------------------------------------------------------------
    # Plugin registration
    # -------------------------------------------------------------------------
    def register_checks(self, plugin_instance):
        """
        Attach a project-level checks object.

        After registration, every method on the plugin becomes directly callable
        on the builder via __getattr__ — no boilerplate wiring required.

        The builder injects context into the plugin so its methods can reach:
            plugin.spark           → SparkSession
            plugin.file_path       → resolved file path
            plugin.regex           → regex string
            plugin.base_transform  → the active base transform (or None)  [NEW-03]
            plugin._get_file_dfs   → callable returning {path: DataFrame}
        """
        self._plugin = plugin_instance
        plugin_instance.spark          = self.spark
        plugin_instance.file_path      = self.file_path
        plugin_instance.regex          = self.regex
        plugin_instance.base_transform = self.base_transform   # [NEW-03]
        plugin_instance._get_file_dfs  = self._get_file_dfs
        return self

    # -------------------------------------------------------------------------
    # Magic routing: unknown method calls → plugin
    # -------------------------------------------------------------------------
    def __getattr__(self, name: str):
        """
        Forwards any unknown method call to the registered plugin and wraps
        the returned ValidationResult as a builder step, keeping the chain alive.
        """
        plugin = self.__dict__.get("_plugin")
        if plugin is not None and hasattr(plugin, name):
            def _chainable(*args, **kwargs):
                _name, _args, _kwargs = name, args, kwargs
                self._steps.append(
                    lambda report: report.add(
                        getattr(self._plugin, _name)(*_args, **_kwargs)
                    )
                )
                return self
            return _chainable

        raise AttributeError(
            f"'ValidationBuilder' has no check '{name}'. "
            f"Define it in your custom checks class and pass it to with_checks()."
        )

    # -------------------------------------------------------------------------
    # File reading
    # -------------------------------------------------------------------------
    def _read_file(self, path: str, file_ext: str) -> DataFrame:
        """Reads a single file into a Spark DataFrame based on its extension."""
        try:
            if file_ext == "csv":
                return (
                    self.spark.read
                    .option("header", True)
                    .option("inferSchema", True)
                    .option("quote", '"')
                    .option("escape", '"')
                    .option("multiLine", "true")
                    .csv(path)
                )
            elif file_ext == "parquet":
                return self.spark.read.parquet(path)
            elif file_ext == "json":
                return self.spark.read.option("multiLine", "true").json(path)
            elif file_ext == "text":
                return self.spark.read.text(path)
            elif file_ext in ("xls", "xlsx"):
                return (
                    self.spark.read.format("com.crealytics.spark.excel")
                    .option("header", True)
                    .option("inferSchema", True)
                    .option("dataAddress", "'Sheet1'!")
                    .load(path)
                )
            else:
                raise ValueError(f"Unsupported file format: '{file_ext}'")
        except Exception as e:
            raise ValidationError(f"_read_file failed for '{path}': {str(e)}")

    # -------------------------------------------------------------------------
    # DataFrame cache builder
    # -------------------------------------------------------------------------
    def _get_file_dfs(self) -> Dict[str, DataFrame]:
        """
        Reads and caches all matched files.  If base_transform is set, it is
        applied to every DataFrame immediately after reading and before caching.
        Subsequent calls return the already-cached dict.
        """
        try:
            if self._df_cache:
                return self._df_cache

            pattern = re.compile(self.regex)
            matched = [f for f in dbutils.fs.ls(self.file_path) if pattern.match(f.name)]  # [FIX-05]

            for f in matched:
                _, ext = os.path.splitext(f.name)
                file_ext = ext[1:].lower() if ext else "csv"

                df = self._read_file(f.path, file_ext)

                # [NEW-01] Apply base transform if provided
                if self.base_transform is not None:
                    df = self.base_transform(df, f.path)

                df = df.cache()
                df.count()                          # materialise the cache
                self._df_cache[f.path] = df

            return self._df_cache

        except Exception as e:
            raise ValidationError(f"_get_file_dfs failed: {str(e)}")

    # -------------------------------------------------------------------------
    # Chainable FILE checks
    # -------------------------------------------------------------------------
    def check_file_count(self):
        self._steps.append(lambda r: r.add(self.file.check_file_count(self.file_path, self.regex)))
        return self

    def check_expected_file_count(self, expected: int):
        self._steps.append(lambda r: r.add(self.file.check_expected_file_count(self.file_path, self.regex, expected)))
        return self

    def check_size(self, min_size: int, max_size: int):
        self._steps.append(lambda r: r.add(self.file.check_size(self.file_path, self.regex, min_size, max_size)))
        return self

    def check_duplicate_file(self):
        self._steps.append(lambda r: r.add(self.file.check_duplicate_file(self.file_path, self.regex)))
        return self

    def check_file_already_archived(self, archive_path: str):
        self._steps.append(lambda r: r.add(self.file.check_file_already_archived(self.file_path, self.regex, archive_path)))
        return self

    def check_file_freshness(self, max_age_hours: int):
        self._steps.append(lambda r: r.add(self.file.check_file_freshness(self.file_path, self.regex, max_age_hours)))
        return self

    def check_file_extension(self, allowed_ext: list):
        self._steps.append(lambda r: r.add(self.file.check_file_extension(self.file_path, self.regex, allowed_ext)))
        return self

    def check_file_compression(self, allowed_compressions: list):
        self._steps.append(lambda r: r.add(self.file.check_file_compression(self.file_path, self.regex, allowed_compressions)))
        return self

    # -------------------------------------------------------------------------
    # Chainable SCHEMA checks
    # -------------------------------------------------------------------------
    def check_required_columns(self, required_cols: List[str], allow_extra: bool = False):
        self._steps.append(lambda r: r.add(self.schema.check_required_columns(self._get_file_dfs(), required_cols, allow_extra)))
        return self

    def check_forbidden_columns(self, forbidden_cols: List[str]):
        self._steps.append(lambda r: r.add(self.schema.check_forbidden_columns(self._get_file_dfs(), forbidden_cols)))
        return self

    def check_data_dtype(self, type_map: dict, allow_extra: bool = False):
        self._steps.append(lambda r: r.add(self.schema.check_data_dtype(self._get_file_dfs(), type_map, allow_extra)))
        return self

    def check_column_order(self, expected_order: List[str]):
        self._steps.append(lambda r: r.add(self.schema.check_column_order(self._get_file_dfs(), expected_order)))
        return self

    def check_column_count(self, min_count: int = None, max_count: int = None):
        self._steps.append(lambda r: r.add(self.schema.check_column_count(self._get_file_dfs(), min_count, max_count)))
        return self

    def check_column_sensitivity(self, cols: List[str]):
        self._steps.append(lambda r: r.add(self.schema.check_column_sensitivity(self._get_file_dfs(), cols)))
        return self

    def check_null(self, cols: List[str]):
        self._steps.append(lambda r: r.add(self.schema.check_null(self._get_file_dfs(), cols)))
        return self

    def check_file_header_presence(self):
        self._steps.append(lambda r: r.add(self.schema.check_file_header_presence(self._get_file_dfs())))
        return self

    # -------------------------------------------------------------------------
    # Chainable DATA checks
    # -------------------------------------------------------------------------
    def check_invalid_values(self, column: str, invalid_values: list, case_insensitive: bool = True):
        self._steps.append(lambda r: r.add(self.data.check_invalid_values(self._get_file_dfs(), column, invalid_values, case_insensitive)))
        return self

    def check_expected_values(self, column: str, expected_values: list, case_insensitive: bool = True):
        self._steps.append(lambda r: r.add(self.data.check_expected_values(self._get_file_dfs(), column, expected_values, case_insensitive)))
        return self

    def check_date_format(self, column: str, date_format: str):
        self._steps.append(lambda r: r.add(self.data.check_date_format(self._get_file_dfs(), column, date_format)))
        return self

    def check_no_negative_values(self, column: str):
        self._steps.append(lambda r: r.add(self.data.check_no_negative_values(self._get_file_dfs(), column)))
        return self

    def check_no_whitespace(self, column: str):
        self._steps.append(lambda r: r.add(self.data.check_no_whitespace(self._get_file_dfs(), column)))
        return self

    def check_string_length(self, column: str, min_len: int, max_len: int):
        self._steps.append(lambda r: r.add(self.data.check_string_length(self._get_file_dfs(), column, min_len, max_len)))
        return self

    def check_row_count(self, min_count: int = None, max_count: int = None):
        self._steps.append(lambda r: r.add(self.data.check_row_count(self._get_file_dfs(), min_count, max_count)))
        return self

    def check_completeness_ratio(self, threshold_pct: float):
        self._steps.append(lambda r: r.add(self.data.check_completeness_ratio(self._get_file_dfs(), threshold_pct)))
        return self

    def check_unique(self, key_columns: list):
        self._steps.append(lambda r: r.add(self.data.check_unique(self._get_file_dfs(), key_columns)))
        return self

    def check_kpi_aggregate(self, expr: str, min_val, max_val):
        self._steps.append(lambda r: r.add(self.data.check_kpi_aggregate(self._get_file_dfs(), expr, min_val, max_val)))
        return self

    def check_value_range(self, column_name: str, min_val, max_val):
        self._steps.append(lambda r: r.add(self.data.check_value_range(self._get_file_dfs(), column_name, min_val, max_val)))
        return self

    def check_regex_format(self, column_name: str, pattern: str):
        self._steps.append(lambda r: r.add(self.data.check_regex_format(self._get_file_dfs(), column_name, pattern)))
        return self

    def check_no_future_date(self, date_column: str, date_format: str):
        self._steps.append(lambda r: r.add(self.data.check_no_future_date(self._get_file_dfs(), date_column, date_format)))
        return self

    def check_duplicate_rows(self):
        self._steps.append(lambda r: r.add(self.data.check_duplicate_rows(self._get_file_dfs())))
        return self

    def add_custom_check(self, fn: Callable[[ValidationReport], None]):
        """
        Append a one-off check step.  `fn` receives the ValidationReport and
        should call report.add(ValidationResult(...)) directly.
        """
        self._steps.append(fn)
        return self

    # -------------------------------------------------------------------------
    # Terminal: build report
    # -------------------------------------------------------------------------
    def build_report(self) -> ValidationReport:
        """
        Execute all queued steps and return the completed ValidationReport.
        The DataFrame cache is always unpersisted in the finally block — even
        if a check raises mid-run — to prevent Spark memory leaks.  [FIX-04]
        """
        report = ValidationReport()
        try:
            for step in self._steps:
                step(report)
            report.print_summary()
            return report
        except Exception as e:
            raise ValidationError(f"build_report failed: {str(e)}")
        finally:                                   # [FIX-04] always unpersist
            for df in self._df_cache.values():
                df.unpersist()
            self._df_cache.clear()


# =========================================================
# CHILD BUILDER — for_dataframes() entry point
# =========================================================

class ValidationBuilderChild(ValidationBuilder):
    """
    Variant of ValidationBuilder for use when the caller supplies DataFrames
    directly (via for_dataframes()) rather than pointing at a file path.

    Each DataFrame must contain a `_source_path_` column that identifies its
    origin; this is used as the cache key so all checks remain path-keyed.

    [FIX-06] Now calls super().__init__() instead of re-declaring every field
             manually, so future fields added to ValidationBuilder are
             automatically inherited.

    [NEW-02] Accepts an optional base_transform applied to each DataFrame
             before caching.
    """

    def __init__(
        self,
        spark: SparkSession,
        df_list: list,
        base_transform: Optional[Callable[[DataFrame, str], DataFrame]] = None,  # [NEW-02]
    ):
        # [FIX-06] Delegate all field initialisation to the parent.
        # file_path and regex are not relevant for the DataFrame path, so
        # they are passed as None.  FileCheck methods are not wired on this
        # builder (they need a real path), but SchemaChecks and DataChecks
        # work fine via _get_file_dfs().
        super().__init__(
            spark=spark,
            file_path=None,
            regex=None,
            base_transform=base_transform,
        )
        self.df_list = df_list

    def _get_file_dfs(self) -> Dict[str, DataFrame]:
        """
        Builds the {path: DataFrame} cache from the caller-supplied df_list.
        Each DataFrame must have a `_source_path_` column (dropped before caching).
        If base_transform is set it is applied before caching.  [NEW-02]
        """
        try:
            if self.df_list is None:
                raise ValueError("No DataFrames available in df_list.")

            if self._df_cache:
                return self._df_cache

            for df in self.df_list:
                if "_source_path_" not in df.columns:
                    raise ValueError(
                        "Each DataFrame must have a '_source_path_' column. "
                        "Add it before calling for_dataframes(): "
                        "df.withColumn('_source_path_', lit(file_path))"
                    )
                paths = df.select("_source_path_").distinct().collect()
                if len(paths) != 1:
                    raise ValueError("Each DataFrame must map to exactly one source path.")

                file_path = paths[0][0]
                df = df.drop("_source_path_")

                # [NEW-02] Apply base transform if provided
                if self.base_transform is not None:
                    df = self.base_transform(df, file_path)

                df = df.cache()
                df.count()
                self._df_cache[file_path] = df

            return self._df_cache

        except Exception as e:
            raise ValidationError(f"_get_file_dfs (child) failed: {str(e)}")


# =========================================================
# MAIN ORCHESTRATOR
# =========================================================

class ValidationOrchestrator:
    """
    Entry point for the validation framework.

    Usage
    -----
    # File-based, with optional transform
    report = (
        ValidationOrchestrator(spark)
        .with_checks(MyProjectChecks())
        .for_files(landing_path, regex, base_transform=my_transform)
        .check_file_count()
        .check_required_columns(["id", "amount"])
        .my_custom_check()
        .build_report()
    )

    # DataFrame-based
    report = (
        ValidationOrchestrator(spark)
        .for_dataframes([df1, df2])
        .check_null(["id"])
        .build_report()
    )
    """

    def __init__(self, spark: SparkSession):
        self.spark   = spark
        self._plugin = None

    # -------------------------------------------------------------------------
    # Plugin attachment
    # -------------------------------------------------------------------------
    def with_checks(self, plugin_instance):
        """
        Register a project-level custom checks object once on the orchestrator.
        It is automatically forwarded to every builder returned by for_files()
        / for_dataframes(), including the base_transform context.
        """
        self._plugin = plugin_instance
        return self

    # -------------------------------------------------------------------------
    # Validators (static helpers)
    # -------------------------------------------------------------------------
    @staticmethod
    def _validate_regex(regex: str) -> Tuple[bool, str]:
        if not regex or not regex.strip():
            return False, "Regex cannot be empty"
        if regex.strip() in {"*", "+", "?", "{", "}"}:
            return False, f"Invalid standalone regex operator: '{regex}'"
        if regex.strip() in {".*", "^.*$", ".+", "^.+$"}:
            return False, "Overly broad regex that matches everything is not allowed"
        if not re.search(r"[A-Za-z0-9]", regex):
            return False, "Regex must contain at least one literal character"
        try:
            re.compile(regex)
        except re.error as e:
            return False, f"Invalid regex syntax: {str(e)}"
        return True, ""

    @staticmethod
    def _validate_required_params(file_path: str, regex: str) -> None:
        missing = [n for n, v in {"file_path": file_path, "regex": regex}.items() if v is None]
        if missing:
            raise ValidationError(f"Required parameter(s) missing: {', '.join(missing)}")
        empty = [n for n, v in {"file_path": file_path, "regex": regex}.items() if not str(v).strip()]
        if empty:
            raise ValidationError(f"Parameter(s) cannot be empty: {', '.join(empty)}")

    @staticmethod
    def _validate_file_path(file_path: str, regex: str) -> None:
        try:
            files = dbutils.fs.ls(file_path)
            pattern = re.compile(regex)
            matched = [f for f in files if pattern.match(f.name)]   # [FIX-05] was pattern.search
        except Exception as e:
            raise ValidationError(f"Unable to access location '{file_path}': {e}")
        if not files:
            raise ValidationError(f"Location '{file_path}' is empty.")
        if not matched:
            raise ValidationError(f"No files match regex '{regex}' at path '{file_path}'.")

    @staticmethod
    def _validate_df_list(df_list: List[DataFrame]) -> Tuple[bool, str]:
        try:
            if df_list is None:
                return False, "df_list is None"
            if not isinstance(df_list, list):
                return False, f"Expected df_list to be list, got {type(df_list).__name__}"
            if len(df_list) == 0:
                return False, "df_list is empty"
            for idx, df in enumerate(df_list):
                if df is None:
                    return False, f"DataFrame at index {idx} is None"
                if not isinstance(df, DataFrame):
                    return False, f"Item at index {idx} is not a PySpark DataFrame"
                if df.limit(1).count() == 0:          # [NEW-05] avoids full RDD materialisation
                    return False, f"DataFrame at index {idx} has zero rows"
                if not df.columns:
                    return False, f"DataFrame at index {idx} has zero columns"
                if len(df.columns) != len(set(df.columns)):
                    return False, f"Duplicate column names in DataFrame at index {idx}"
            return True, "All DataFrames are valid"
        except Exception as e:
            return False, f"DataFrame validation failed: {str(e)}"

    # -------------------------------------------------------------------------
    # Entry points
    # -------------------------------------------------------------------------
    def for_files(
        self,
        file_path: str,
        regex: str,
        base_transform: Optional[Callable[[DataFrame, str], DataFrame]] = None,  # [NEW-01]
    ) -> ValidationBuilder:
        """
        File-based entry point.

        Parameters
        ----------
        file_path       : ABFSS / DBFS path to the landing directory.
        regex           : Filename filter regex (must match at least one file).
        base_transform  : Optional (df, path) -> df applied after reading each
                          matched file, before caching.  All subsequent checks
                          (including plugin checks) operate on the transformed
                          DataFrame.

        Example — strip deleted rows and cast amounts before any check:
            def my_transform(df, path):
                return (
                    df.filter(col("status") != "DELETED")
                      .withColumn("amount", col("amount").cast("double"))
                )

            report = (
                ValidationOrchestrator(spark)
                .for_files(landing_path, regex, base_transform=my_transform)
                .check_null(["amount"])
                .build_report()
            )
        """
        try:
            self._validate_required_params(file_path, regex)
            ok, msg = self._validate_regex(regex)
            if not ok:
                raise ValidationError(msg)
            self._validate_file_path(file_path, regex)

            builder = ValidationBuilder(
                spark=self.spark,
                file_path=file_path,
                regex=regex,
                base_transform=base_transform,   # [NEW-01]
            )

            if self._plugin is not None:
                builder.register_checks(self._plugin)   # [NEW-03] injects base_transform too

            return builder

        except ValidationError:
            raise
        except Exception as e:
            raise ValidationError(f"for_files failed: {str(e)}")

    def for_dataframes(
        self,
        df_list: List[DataFrame],
        base_transform: Optional[Callable[[DataFrame, str], DataFrame]] = None,  # [NEW-02]
    ) -> ValidationBuilderChild:
        """
        DataFrame-based entry point.  Use this when DataFrames arrive from an
        upstream pipeline you don't control.

        Each DataFrame must contain a `_source_path_` column identifying its
        origin path — this is the contract between the caller and the framework.

        Parameters
        ----------
        df_list         : List of DataFrames, each with a `_source_path_` column.
        base_transform  : Optional (df, path) -> df applied before caching.
                          Useful for applying common transforms centrally when
                          the caller passes raw DataFrames.  [NEW-02]
        """
        try:
            ok, msg = self._validate_df_list(df_list)
            if not ok:
                raise ValidationError(msg)

            builder = ValidationBuilderChild(
                spark=self.spark,
                df_list=df_list,
                base_transform=base_transform,   # [NEW-02]
            )

            if self._plugin is not None:
                builder.register_checks(self._plugin)

            return builder

        except ValidationError:
            raise
        except Exception as e:
            raise ValidationError(f"for_dataframes failed: {str(e)}")
