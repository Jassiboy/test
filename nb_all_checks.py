# Databricks notebook source

# ============================================================
# CHANGE LOG
# ============================================================
# [FIX-01]  Added `Tuple` to typing imports (was missing, caused NameError).
# [FIX-02]  Removed `_validate_col_list` duplication — moved to a shared
#           module-level mixin `_ColListValidatorMixin` used by both
#           SchemaChecks and DataChecks.
# [FIX-03]  check_size: wrong check_name "STRING_LENGTH_CHECK" replaced
#           with "FILE_SIZE_CHECK" in both parameter-validation returns.
# [FIX-04]  check_expected_file_count: `expected = int` (assigns the type
#           object as default) fixed to `expected: int` (correct annotation,
#           no default — caller must always supply it explicitly).
# [FIX-05]  check_duplicate_file: was using pattern.search() while every
#           other FileCheck uses pattern.match().  Standardised to
#           pattern.match() so regex anchor behaviour is consistent.
# [FIX-06]  "FAIL:" / "PASS:" prefixes stripped from message strings — the
#           `status` field already carries that information; duplication
#           broke the report table Details column and made programmatic
#           parsing harder.
# [FIX-07]  check_null: replaced bare `display(e)` with proper re-raise so
#           the exception is visible and the check_name appears in the report.
# [FIX-08]  check_duplicate_rows: replaced double full-scan (count +
#           distinct().count()) with a groupBy approach that also surfaces
#           how many *distinct* duplicate groups exist — more informative.
# [FIX-09]  check_string_length: removed stray print() debug statements.
# [NEW-01]  `_ColListValidatorMixin` — single source of truth for column-list
#           validation shared by SchemaChecks and DataChecks.
# ============================================================

import re
import builtins
import os
import hashlib

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional, Callable, Dict, Tuple   # [FIX-01] added Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, count, when, trim, length, lower, to_date,
    current_date
)
from pyspark.sql.types import (
    StringType, IntegerType, LongType, ShortType,
    FloatType, DoubleType, DecimalType,
    BooleanType, DateType, TimestampType, DataType
)
from pyspark.sql.functions import expr as spark_expr

# COMMAND ----------

@dataclass
class ValidationResult:
    check_name: str
    status: str        # "PASS" | "FAIL"
    message: str
    expected: Optional[str] = None
    actual: Optional[str] = None

    def __str__(self):
        return f"{self.check_name}: {self.message}"

# COMMAND ----------

# ============================================================
# [NEW-01] Shared mixin — eliminates _validate_col_list
#          duplication between SchemaChecks and DataChecks.
# ============================================================
class _ColListValidatorMixin:
    """
    Provides _validate_col_list() as a static utility.
    Inherit this in any check class that validates column-name lists.
    """

    @staticmethod
    def _validate_col_list(required_cols, check_name: str) -> Optional[ValidationResult]:
        """
        Validates that `required_cols` is a non-empty list of unique strings.
        Returns a FAIL ValidationResult on the first problem found, else None.
        """
        if not isinstance(required_cols, list):
            return ValidationResult(
                check_name, "FAIL",
                f"Column list must be a list, got {type(required_cols).__name__}"
            )
        if not required_cols:
            return ValidationResult(check_name, "FAIL", "Column list cannot be empty")

        non_strings = [c for c in required_cols if not isinstance(c, str)]
        if non_strings:
            return ValidationResult(
                check_name, "FAIL",
                f"All column names must be strings. Invalid entries: {non_strings}"
            )

        lowered = [c.lower() for c in required_cols]
        if len(lowered) != len(set(lowered)):
            return ValidationResult(
                check_name, "FAIL",
                f"Duplicate column names found (case-insensitive): {required_cols}"
            )

        return None


# COMMAND ----------

# DBTITLE 1,FileChecks
class FileChecks:
    def __init__(self, spark):
        self.spark = spark

# ── check_file_count ──────────────────────────────────────────────────────────
    def check_file_count(self, path: str, regex: str) -> ValidationResult:
        """
        Counts files matching the regex.  FAIL if zero found.
        """
        try:
            pattern = re.compile(regex)
            files = dbutils.fs.ls(path)
            matched = [f.name for f in files if pattern.match(f.name)]  # [FIX-05 context] consistent match()
            count_ = len(matched)

            if count_ == 0:
                return ValidationResult(
                    "FILE_COUNT_CHECK", "FAIL",
                    f"No files found matching regex '{regex}' in path '{path}'"  # [FIX-06]
                )
            return ValidationResult(
                "FILE_COUNT_CHECK", "PASS",
                f"{count_} file(s) found matching regex '{regex}'"               # [FIX-06]
            )
        except Exception as e:
            return ValidationResult(
                "FILE_COUNT_CHECK", "FAIL",
                f"Error while counting files in '{path}': {str(e)}"
            )

# ── check_expected_file_count ─────────────────────────────────────────────────
    def check_expected_file_count(self, path: str, regex: str, expected: int) -> ValidationResult:  # [FIX-04]
        """
        FAIL if the number of matching files differs from `expected`.
        """
        try:
            if not isinstance(expected, int) or expected < 0:
                return ValidationResult(
                    "EXPECTED_FILE_COUNT_CHECK", "FAIL",
                    f"Invalid expected value '{expected}'. Must be a non-negative integer."
                )

            pattern = re.compile(regex)
            files = dbutils.fs.ls(path)
            matched = [f.name for f in files if pattern.match(f.name)]
            count_ = len(matched)

            if count_ != expected:
                return ValidationResult(
                    "EXPECTED_FILE_COUNT_CHECK", "FAIL",
                    f"{expected} file(s) expected but {count_} found in path '{path}'"  # [FIX-06]
                )
            return ValidationResult(
                "EXPECTED_FILE_COUNT_CHECK", "PASS",
                f"{count_} file(s) found matching regex '{regex}'"
            )
        except Exception as e:
            return ValidationResult(
                "EXPECTED_FILE_COUNT_CHECK", "FAIL",
                f"Error while counting files in '{path}': {str(e)}"
            )

# ── check_size ────────────────────────────────────────────────────────────────
    def check_size(self, path: str, regex: str, min_size: int, max_size: int) -> ValidationResult:
        """
        PASS if ALL matched files have size within [min_size, max_size] bytes.
        """
        try:
            # [FIX-03]  parameter guards now use the correct check_name
            if not isinstance(min_size, int) or not isinstance(max_size, int):
                return ValidationResult(
                    "FILE_SIZE_CHECK", "FAIL",          # [FIX-03]
                    "min_size and max_size must be integers"
                )
            if min_size < 0 or max_size < min_size:
                return ValidationResult(
                    "FILE_SIZE_CHECK", "FAIL",          # [FIX-03]
                    "Invalid size bounds: min_size must be >= 0 and <= max_size"
                )

            pattern = re.compile(regex)
            files = dbutils.fs.ls(path)
            matched = [f for f in files if pattern.match(f.name)]

            if not matched:
                return ValidationResult(
                    "FILE_SIZE_CHECK", "FAIL",
                    f"No files found matching regex '{regex}' in path '{path}'"
                )

            for f in matched:
                if not hasattr(f, "size"):
                    return ValidationResult(
                        "FILE_SIZE_CHECK", "FAIL",
                        f"File '{f.name}' has no size attribute"
                    )

            invalid = [(f.name, f.size) for f in matched if f.size < min_size or f.size > max_size]
            if invalid:
                details = ", ".join(f"{n}({s} bytes)" for n, s in invalid)
                return ValidationResult(
                    "FILE_SIZE_CHECK", "FAIL",
                    f"Files outside size range [{min_size}, {max_size}] bytes: {details}"  # [FIX-06]
                )
            return ValidationResult(
                "FILE_SIZE_CHECK", "PASS",
                f"All {len(matched)} file(s) are within [{min_size}, {max_size}] bytes"
            )
        except Exception as e:
            return ValidationResult(
                "FILE_SIZE_CHECK", "FAIL",
                f"Error while validating file sizes in '{path}': {str(e)}"
            )

# ── check_duplicate_file ──────────────────────────────────────────────────────
    def check_duplicate_file(self, path: str, regex: str) -> ValidationResult:
        """
        Detects duplicate files (same name + same size).
        """
        try:
            pattern = re.compile(regex)
            files = dbutils.fs.ls(path)
            matched = [f for f in files if pattern.match(f.name)]  # [FIX-05] was pattern.search

            if not matched:
                return ValidationResult(
                    "DUPLICATE_FILE_CHECK", "FAIL",
                    f"No files found matching regex '{regex}' in path '{path}'"
                )

            tracker = {}
            duplicates = []
            for f in matched:
                if not hasattr(f, "size"):
                    return ValidationResult(
                        "DUPLICATE_FILE_CHECK", "FAIL",
                        f"File '{f.name}' has no size attribute"
                    )
                key = (f.name, f.size)
                if key in tracker:
                    duplicates.append(f"{f.name} ({f.size} bytes)")
                else:
                    tracker[key] = f.path

            if duplicates:
                total = len(duplicates)
                detail = ", ".join(duplicates[:5])
                if total > 5:
                    detail += f", ... and {total - 5} more"
                return ValidationResult(
                    "DUPLICATE_FILE_CHECK", "FAIL",
                    f"Duplicate files detected in '{path}': {detail}"  # [FIX-06]
                )
            return ValidationResult(
                "DUPLICATE_FILE_CHECK", "PASS",
                f"No duplicate files found matching regex '{regex}'"
            )
        except Exception as e:
            return ValidationResult(
                "DUPLICATE_FILE_CHECK", "FAIL",
                f"Error while checking duplicate files in '{path}': {str(e)}"
            )

# ── check_file_already_archived ───────────────────────────────────────────────
    def check_file_already_archived(self, source_path: str, regex: str, archive_path: str) -> ValidationResult:
        """
        FAIL if any source file matching the regex already exists in archive_path.
        """
        try:
            if not archive_path or not str(archive_path).strip():
                return ValidationResult(
                    "ARCHIVE_DUPLICATE_CHECK", "FAIL",
                    f"Unable to access archive location '{archive_path}'"
                )

            pattern = re.compile(regex)
            source_names = {f.name for f in dbutils.fs.ls(source_path) if pattern.match(f.name)}
            archive_names = {f.name for f in dbutils.fs.ls(archive_path) if pattern.match(f.name)}

            already_archived = sorted(source_names & archive_names)
            if already_archived:
                detail = ", ".join(already_archived[:5])
                if len(already_archived) > 5:
                    detail += "..."
                return ValidationResult(
                    "ARCHIVE_DUPLICATE_CHECK", "FAIL",
                    f"File(s) already exist in archive '{archive_path}': {detail}"
                )
            return ValidationResult(
                "ARCHIVE_DUPLICATE_CHECK", "PASS",
                f"No source files found in archive path '{archive_path}'"
            )
        except Exception as e:
            return ValidationResult(
                "ARCHIVE_DUPLICATE_CHECK", "FAIL",
                f"Error while checking archive path '{archive_path}': {str(e)}"
            )

# ── check_file_freshness ──────────────────────────────────────────────────────
    def check_file_freshness(self, path: str, regex: str, max_age_hours: int) -> ValidationResult:
        """
        FAIL if ANY matched file is older than max_age_hours.
        """
        try:
            pattern = re.compile(regex)
            files = dbutils.fs.ls(path)
            matched = [f for f in files if pattern.match(f.name)]

            if not matched:
                return ValidationResult(
                    "FILE_FRESHNESS_CHECK", "FAIL",
                    f"No files found matching regex '{regex}' in path '{path}'"
                )

            current_time = datetime.now(timezone.utc)
            stale = []
            for f in matched:
                if not hasattr(f, "modificationTime"):
                    return ValidationResult(
                        "FILE_FRESHNESS_CHECK", "FAIL",
                        f"File '{f.name}' has no modificationTime attribute"
                    )
                mod_time = datetime.fromtimestamp(f.modificationTime / 1000, tz=timezone.utc)
                age_hours = (current_time - mod_time).total_seconds() / 3600
                if age_hours > max_age_hours:
                    stale.append(f"{f.name} ({age_hours:.2f}h old)")

            if stale:
                return ValidationResult(
                    "FILE_FRESHNESS_CHECK", "FAIL",
                    f"Files older than {max_age_hours}h in '{path}': {', '.join(stale)}"
                )
            return ValidationResult(
                "FILE_FRESHNESS_CHECK", "PASS",
                f"All {len(matched)} file(s) are within the {max_age_hours}h freshness threshold"
            )
        except Exception as e:
            return ValidationResult(
                "FILE_FRESHNESS_CHECK", "FAIL",
                f"Error while checking file freshness in '{path}': {str(e)}"
            )

# ── check_file_extension ──────────────────────────────────────────────────────
    def check_file_extension(self, path: str, regex: str, allowed_ext: list) -> ValidationResult:
        """
        FAIL if ANY matched file has an extension not in allowed_ext.
        """
        try:
            pattern = re.compile(regex)
            matched = [f for f in dbutils.fs.ls(path) if pattern.match(f.name)]
            allowed_norm = [e.lower().lstrip(".") for e in allowed_ext]

            invalid = []
            for f in matched:
                ext = f.name.rsplit(".", 1)[-1].lower() if "." in f.name else ""
                if ext not in allowed_norm:
                    invalid.append(f.name)

            if invalid:
                return ValidationResult(
                    "FILE_EXTENSION_CHECK", "FAIL",
                    f"Files with disallowed extensions {allowed_ext}: {', '.join(invalid)}"
                )
            return ValidationResult(
                "FILE_EXTENSION_CHECK", "PASS",
                f"All {len(matched)} file(s) have allowed extensions {allowed_ext}"
            )
        except Exception as e:
            return ValidationResult(
                "FILE_EXTENSION_CHECK", "FAIL",
                f"Error while validating file extensions in '{path}': {str(e)}"
            )

# ── check_file_compression ────────────────────────────────────────────────────
    def check_file_compression(self, path: str, regex: str, allowed_compressions: list) -> ValidationResult:
        """
        FAIL if ANY matched file's last extension is not in allowed_compressions.
        """
        try:
            pattern = re.compile(regex)
            matched = [f for f in dbutils.fs.ls(path) if pattern.match(f.name)]
            allowed_norm = [c.lower().lstrip(".") for c in allowed_compressions]

            invalid = []
            for f in matched:
                parts = f.name.lower().split(".")
                comp = parts[-1] if len(parts) > 1 else None
                if comp not in allowed_norm:
                    invalid.append(f.name)

            if invalid:
                return ValidationResult(
                    "FILE_COMPRESSION_CHECK", "FAIL",
                    f"Files with disallowed compression {allowed_compressions}: {', '.join(invalid)}"
                )
            return ValidationResult(
                "FILE_COMPRESSION_CHECK", "PASS",
                f"All {len(matched)} file(s) use allowed compression formats {allowed_compressions}"
            )
        except Exception as e:
            return ValidationResult(
                "FILE_COMPRESSION_CHECK", "FAIL",
                f"Error while validating file compression in '{path}': {str(e)}"
            )


# COMMAND ----------

# DBTITLE 1,SchemaChecks
class SchemaChecks(_ColListValidatorMixin):   # [FIX-02] inherits shared mixin
    def __init__(self, spark):
        self.spark = spark

    @staticmethod
    def normalize_spark_dtype(dtype: DataType) -> str:
        if isinstance(dtype, StringType):
            return "string"
        if isinstance(dtype, (IntegerType, LongType, ShortType)):
            return "integer"
        if isinstance(dtype, (FloatType, DoubleType, DecimalType)):
            return "float"
        if isinstance(dtype, BooleanType):
            return "boolean"
        if isinstance(dtype, DateType):
            return "date"
        if isinstance(dtype, TimestampType):
            return "timestamp"
        return type(dtype).__name__.lower()

    def check_required_columns(self, file_dfs: dict, required_cols: list, allow_extra: bool = False) -> ValidationResult:
        """
        PASS if ALL files contain every column in required_cols.
        When allow_extra=False, extra columns also cause FAIL.
        """
        try:
            error = self._validate_col_list(required_cols, "REQUIRED_COLUMNS_CHECK")
            if error:
                return error

            if not isinstance(allow_extra, bool):
                return ValidationResult(
                    "REQUIRED_COLUMNS_CHECK", "FAIL",
                    f"allow_extra must be boolean, got {type(allow_extra).__name__}"
                )

            required_norm = {c.lower(): c for c in required_cols}
            failures = {}

            for file_path, df in file_dfs.items():
                df_norm = {c.lower(): c for c in df.columns}
                missing = [orig for norm, orig in required_norm.items() if norm not in df_norm]
                extra = (
                    [actual for norm, actual in df_norm.items() if norm not in required_norm]
                    if not allow_extra else []
                )
                if missing or extra:
                    failures[file_path] = {"missing": missing, "extra": extra}

            if failures:
                details = "; ".join(
                    f"{f}: missing={i['missing']}, extra={i['extra']}"
                    for f, i in failures.items()
                )
                return ValidationResult(
                    "REQUIRED_COLUMNS_CHECK", "FAIL",
                    f"Column validation failed: {details}"
                )
            return ValidationResult(
                "REQUIRED_COLUMNS_CHECK", "PASS",
                f"All files have required columns {required_cols} (allow_extra={allow_extra})"
            )
        except Exception as e:
            return ValidationResult(
                "REQUIRED_COLUMNS_CHECK", "FAIL",
                f"Unable to validate required columns: {str(e)}"
            )

    def check_forbidden_columns(self, file_dfs: dict, forbidden_cols: list) -> ValidationResult:
        """
        FAIL if ANY forbidden column is found in ANY file.
        """
        try:
            error = self._validate_col_list(forbidden_cols, "FORBIDDEN_COLUMN_CHECK")
            if error:
                return error

            forbidden_map = {c.casefold(): c for c in forbidden_cols}
            failures = {}

            for file_path, df in file_dfs.items():
                df_casefold = {c.casefold(): c for c in df.columns}
                present = [forbidden_map[c] for c in forbidden_map if c in df_casefold]
                if present:
                    failures[file_path] = present

            if failures:
                details = "; ".join(f"{f}: {cols}" for f, cols in failures.items())
                return ValidationResult(
                    "FORBIDDEN_COLUMN_CHECK", "FAIL",
                    f"Forbidden columns found: {details}"
                )
            return ValidationResult(
                "FORBIDDEN_COLUMN_CHECK", "PASS",
                "No forbidden columns found across all files"
            )
        except Exception as e:
            return ValidationResult(
                "FORBIDDEN_COLUMN_CHECK", "FAIL",
                f"Unable to validate forbidden columns: {str(e)}"
            )

    def check_data_dtype(self, file_dfs: dict, type_map: dict, allowed_extra: bool = False) -> ValidationResult:
        """
        PASS if ALL files have columns matching the logical types in type_map.
        Spark types are normalised before comparison (e.g. LongType → "integer").

        NOTE: type_map normalisation is done once before the loop to avoid the
        original bug where the outer `type_map` variable was overwritten on the
        first iteration, causing wrong comparisons for subsequent files.
        """
        try:
            if not isinstance(allowed_extra, bool):
                return ValidationResult(
                    "DATA_DTYPE_CHECK", "FAIL",
                    f"allowed_extra must be boolean, got {type(allowed_extra).__name__}"
                )
            if not isinstance(type_map, dict) or not type_map:
                return ValidationResult(
                    "DATA_DTYPE_CHECK", "FAIL",
                    "type_map must be a non-empty dict of {column: datatype_string}"
                )
            for k, v in type_map.items():
                if not isinstance(k, str) or not isinstance(v, str):
                    return ValidationResult(
                        "DATA_DTYPE_CHECK", "FAIL",
                        "type_map must contain only string:string mappings"
                    )

            # Normalise once — original code re-normalised inside the loop,
            # which mutated `type_map` and broke comparisons for file 2+.
            normalised_map = {col_.lower(): dtype.lower() for col_, dtype in type_map.items()}
            failures = {}

            for file_path, df in file_dfs.items():
                schema_fields = {f.name.lower(): f.dataType for f in df.schema.fields}
                df_cols = set(schema_fields.keys())
                expected_cols = set(normalised_map.keys())
                file_failures = {}

                if not allowed_extra:
                    extra = df_cols - expected_cols
                    if extra:
                        file_failures["EXTRA_COLUMNS"] = sorted(extra)

                for col_, expected_type in normalised_map.items():
                    if col_ not in schema_fields:
                        file_failures[col_] = "Column not found"
                        continue
                    actual_type = self.normalize_spark_dtype(schema_fields[col_])
                    if actual_type != expected_type:
                        file_failures[col_] = f"Expected '{expected_type}', found '{actual_type}'"

                if file_failures:
                    failures[file_path] = file_failures

            if failures:
                details = "; ".join(f"{f}: {issues}" for f, issues in failures.items())
                return ValidationResult(
                    "DATA_DTYPE_CHECK", "FAIL",
                    f"Data type validation failed: {details}"
                )
            return ValidationResult(
                "DATA_DTYPE_CHECK", "PASS",
                f"All schemas match expected types (allowed_extra={allowed_extra})"
            )
        except Exception as e:
            return ValidationResult(
                "DATA_DTYPE_CHECK", "FAIL",
                f"Unable to validate data types: {str(e)}"
            )

    def check_column_order(self, file_dfs: dict, expected_order: list) -> ValidationResult:
        """
        FAIL if ANY file's columns are not in the exact expected order (case-insensitive).
        """
        try:
            error = self._validate_col_list(expected_order, "COLUMN_ORDER_CHECK")
            if error:
                return error

            expected_norm = [c.lower() for c in expected_order]
            failures = {}

            for file_path, df in file_dfs.items():
                actual_cols = list(df.columns)
                actual_norm = [c.lower() for c in actual_cols]
                if actual_norm != expected_norm:
                    failures[file_path] = {
                        "expected": expected_order,
                        "actual": actual_cols,
                        "missing": [c for c in expected_order if c.lower() not in actual_norm],
                        "extra": [c for c in actual_cols if c.lower() not in expected_norm],
                    }

            if failures:
                details = "; ".join(
                    f"{f}: missing={i['missing']}, extra={i['extra']}, "
                    f"expected={i['expected']}, actual={i['actual']}"
                    for f, i in failures.items()
                )
                return ValidationResult(
                    "COLUMN_ORDER_CHECK", "FAIL",
                    f"Column order validation failed: {details}"
                )
            return ValidationResult(
                "COLUMN_ORDER_CHECK", "PASS",
                "All files match expected column order (case-insensitive)"
            )
        except Exception as e:
            return ValidationResult(
                "COLUMN_ORDER_CHECK", "FAIL",
                f"Unable to validate column order: {str(e)}"
            )

    def check_column_count(self, file_dfs: dict, min_count: int = None, max_count: int = None) -> ValidationResult:
        """
        FAIL if ANY file has a column count outside [min_count, max_count].
        """
        try:
            for name, val in {"min_count": min_count, "max_count": max_count}.items():
                if val is None or not isinstance(val, int):
                    return ValidationResult(
                        "COLUMN_COUNT_CHECK", "FAIL",
                        f"{name} must be an integer, got {type(val).__name__}"
                    )
            if min_count < 0 or max_count < min_count:
                return ValidationResult(
                    "COLUMN_COUNT_CHECK", "FAIL",
                    "Invalid bounds: min_count must be >= 0 and <= max_count"
                )

            failures = {}
            for file_path, df in file_dfs.items():
                n = len(df.columns)
                errors = []
                if n < min_count:
                    errors.append(f"column count {n} < min_count {min_count}")
                if n > max_count:
                    errors.append(f"column count {n} > max_count {max_count}")
                if errors:
                    failures[file_path] = errors

            if failures:
                details = "; ".join(f"{f}: {e}" for f, e in failures.items())
                return ValidationResult(
                    "COLUMN_COUNT_CHECK", "FAIL",
                    f"Column count validation failed: {details}"
                )
            return ValidationResult(
                "COLUMN_COUNT_CHECK", "PASS",
                "Column count within expected range for all files"
            )
        except Exception as e:
            return ValidationResult(
                "COLUMN_COUNT_CHECK", "FAIL",
                f"Unable to validate column count: {str(e)}"
            )

    def check_null(self, file_dfs: dict, non_null_cols: list) -> ValidationResult:
        """
        FAIL if ANY specified column contains NULL values in ANY file.
        """
        try:
            error = self._validate_col_list(non_null_cols, "NULL_CHECK")
            if error:
                return error

            non_null_norm = {c.lower(): c for c in non_null_cols}
            failures = {}

            for file_path, df in file_dfs.items():
                df_norm = {c.lower(): c for c in df.columns}
                violated = {}

                for norm_name, orig_name in non_null_norm.items():
                    if norm_name not in df_norm:
                        violated[orig_name] = "column missing"
                    else:
                        actual_col = df_norm[norm_name]
                        null_count = (
                            df.select(count(when(col(actual_col).isNull(), 1)).alias("nulls"))
                            .collect()[0]["nulls"]
                        )
                        if null_count > 0:
                            violated[actual_col] = f"{null_count} NULL value(s)"

                if violated:
                    failures[file_path] = violated

            if failures:
                details = "; ".join(f"{f}: {info}" for f, info in failures.items())
                return ValidationResult(
                    "NULL_CHECK", "FAIL",
                    f"NOT NULL constraint violated: {details}"
                )
            return ValidationResult(
                "NULL_CHECK", "PASS",
                "All NOT NULL constraints satisfied (case-insensitive)"
            )

        except Exception as e:  # [FIX-07] was display(e) — now properly raises
            return ValidationResult(
                "NULL_CHECK", "FAIL",
                f"Unable to validate nullable constraints: {str(e)}"
            )

    def check_column_sensitivity(self, file_dfs: dict, cols: list) -> ValidationResult:
        """
        Case-SENSITIVE column presence check (unlike all other column checks).
        """
        try:
            error = self._validate_col_list(cols, "COLUMN_SENSITIVITY")
            if error:
                return error

            failures = {}
            for file_path, df in file_dfs.items():
                actual = set(df.columns)
                missing = [c for c in cols if c not in actual]
                if missing:
                    failures[file_path] = {"missing_case_sensitive": missing, "available": list(df.columns)}

            if failures:
                details = "; ".join(
                    f"{f}: missing={i['missing_case_sensitive']} (available={i['available']})"
                    for f, i in failures.items()
                )
                return ValidationResult(
                    "COLUMN_SENSITIVITY", "FAIL",
                    f"Case-sensitive column validation failed: {details}"
                )
            return ValidationResult(
                "COLUMN_SENSITIVITY", "PASS",
                f"All sensitive columns present with exact casing: {cols}"
            )
        except Exception as e:
            return ValidationResult(
                "COLUMN_SENSITIVITY", "FAIL",
                f"Unable to validate column sensitivity: {str(e)}"
            )

    def check_file_header_presence(self, file_dfs: dict) -> ValidationResult:
        """
        FAIL if ANY file has no columns or only Spark auto-generated column names (_c0, _c1…).
        """
        failures = {}
        for file_path, df in file_dfs.items():
            if not df.columns:
                failures[file_path] = "No columns found (empty or malformed file)"
                continue
            if all(c.lower().startswith("_c") for c in df.columns):
                failures[file_path] = "Header row missing (auto-generated columns detected)"

        if failures:
            return ValidationResult(
                "FILE_HEADER_PRESENCE_CHECK", "FAIL",
                "; ".join(f"{f}: {r}" for f, r in failures.items())
            )
        return ValidationResult(
            "FILE_HEADER_PRESENCE_CHECK", "PASS",
            "File headers present and valid for all files"
        )


# COMMAND ----------

# DBTITLE 1,DataChecks
class DataChecks(_ColListValidatorMixin):   # [FIX-02] inherits shared mixin
    def __init__(self, spark):
        self.spark = spark

    def check_invalid_values(self, file_dfs: dict, column: str, invalid_values: list,
                              case_insensitive: bool = True) -> ValidationResult:
        """
        FAIL if the column contains ANY value listed in invalid_values.
        """
        try:
            error = self._validate_col_list(invalid_values, "INVALID_VALUES_CHECK")
            if error:
                return error
            if not isinstance(case_insensitive, bool):
                return ValidationResult("INVALID_VALUES_CHECK", "FAIL", "case_insensitive must be boolean")
            if not isinstance(column, str):
                return ValidationResult("INVALID_VALUES_CHECK", "FAIL", "column must be a string")

            invalid_set = {
                v.lower() if case_insensitive and isinstance(v, str) else v
                for v in invalid_values
            }
            failures = {}

            for file_path, df in file_dfs.items():
                df_cols = {c.lower(): c for c in df.columns}
                if column.lower() not in df_cols:
                    failures[file_path] = f"Column '{column}' not found"
                    continue
                actual_col = df_cols[column.lower()]
                val_col = lower(col(actual_col)) if case_insensitive else col(actual_col)
                if df.filter(val_col.isin(invalid_set)).limit(1).count() > 0:
                    failures[file_path] = f"Invalid values found in '{actual_col}'. Banned: {invalid_values}"

            if failures:
                return ValidationResult("INVALID_VALUES_CHECK", "FAIL",
                                        "; ".join(f"{k}: {v}" for k, v in failures.items()))
            return ValidationResult("INVALID_VALUES_CHECK", "PASS",
                                    f"No invalid values found in '{column}'")
        except Exception as e:
            return ValidationResult("INVALID_VALUES_CHECK", "FAIL",
                                    f"Unable to validate invalid values: {str(e)}")

    def check_expected_values(self, file_dfs: dict, column: str, expected_values: list,
                               case_insensitive: bool = True) -> ValidationResult:
        """
        FAIL if the column contains ANY value NOT in expected_values.
        """
        try:
            if not isinstance(case_insensitive, bool):
                return ValidationResult("EXPECTED_VALUES_CHECK", "FAIL", "case_insensitive must be boolean")
            if not isinstance(column, str):
                return ValidationResult("EXPECTED_VALUES_CHECK", "FAIL", "column must be a string")
            error = self._validate_col_list(expected_values, "EXPECTED_VALUES_CHECK")
            if error:
                return error

            expected_set = {
                v.lower() if case_insensitive and isinstance(v, str) else v
                for v in expected_values
            }
            failures = {}

            for file_path, df in file_dfs.items():
                df_cols = {c.lower(): c for c in df.columns}
                if column.lower() not in df_cols:
                    failures[file_path] = f"Column '{column}' not found"
                    continue
                actual_col = df_cols[column.lower()]
                val_col = lower(col(actual_col)) if case_insensitive else col(actual_col)
                if df.filter(~val_col.isin(expected_set)).limit(1).count() > 0:
                    failures[file_path] = f"Unexpected values in '{actual_col}'. Allowed: {expected_values}"

            if failures:
                return ValidationResult("EXPECTED_VALUES_CHECK", "FAIL",
                                        "; ".join(f"{k}: {v}" for k, v in failures.items()))
            return ValidationResult("EXPECTED_VALUES_CHECK", "PASS",
                                    f"All files contain only expected values in '{column}'")
        except Exception as e:
            return ValidationResult("EXPECTED_VALUES_CHECK", "FAIL",
                                    f"Unable to validate expected values: {str(e)}")

    def check_date_format(self, file_dfs: dict, column: str, date_format: str) -> ValidationResult:
        """
        FAIL if ANY value in the column cannot be parsed with date_format.
        """
        try:
            if not isinstance(date_format, str) or not date_format.strip():
                return ValidationResult("DATE_FORMAT_CHECK", "FAIL", "date_format must be a non-empty string")
            if not isinstance(column, str):
                return ValidationResult("DATE_FORMAT_CHECK", "FAIL", "column must be a string")

            failures = {}
            for file_path, df in file_dfs.items():
                df_cols = {c.lower(): c for c in df.columns}
                if column.lower() not in df_cols:
                    failures[file_path] = f"Column '{column}' not found"
                    continue
                actual_col = df_cols[column.lower()]
                parsed = df.withColumn("__parsed", to_date(col(actual_col), date_format))
                invalid = parsed.filter(
                    col(actual_col).isNotNull() & col("__parsed").isNull()
                ).limit(1).count()
                if invalid > 0:
                    failures[file_path] = f"Invalid date format in '{actual_col}', expected: {date_format}"

            if failures:
                return ValidationResult("DATE_FORMAT_CHECK", "FAIL",
                                        "; ".join(f"{k}: {v}" for k, v in failures.items()))
            return ValidationResult("DATE_FORMAT_CHECK", "PASS",
                                    f"All files have valid '{column}' dates in format '{date_format}'")
        except Exception as e:
            return ValidationResult("DATE_FORMAT_CHECK", "FAIL",
                                    f"Unable to validate date format: {str(e)}")

    def check_no_negative_values(self, file_dfs: dict, column: str) -> ValidationResult:
        """
        FAIL if ANY non-null value in the column is negative.
        """
        try:
            if not isinstance(column, str):
                return ValidationResult("NO_NEGATIVE_VALUES_CHECK", "FAIL", "column must be a string")

            failures = {}
            for file_path, df in file_dfs.items():
                df_cols = {c.lower(): c for c in df.columns}
                if column.lower() not in df_cols:
                    failures[file_path] = f"Column '{column}' not found"
                    continue
                actual_col = df_cols[column.lower()]
                if df.filter(col(actual_col).isNotNull() & ~col(actual_col).rlike(r'^-?\d+(\.\d+)?$')).limit(1).count() > 0:
                    failures[file_path] = "Non-numeric values found"
                    continue
                if df.filter(col(actual_col) < 0).limit(1).count() > 0:
                    failures[file_path] = "Negative values found"

            if failures:
                return ValidationResult("NO_NEGATIVE_VALUES_CHECK", "FAIL",
                                        "; ".join(f"{f}: {r}" for f, r in failures.items()))
            return ValidationResult("NO_NEGATIVE_VALUES_CHECK", "PASS",
                                    f"No negative values found in column '{column}'")
        except Exception as e:
            return ValidationResult("NO_NEGATIVE_VALUES_CHECK", "FAIL",
                                    f"Unable to validate negative values: {str(e)}")

    def check_no_whitespace(self, file_dfs: dict, column: str) -> ValidationResult:
        """
        FAIL if ANY string value in the column contains whitespace.
        """
        try:
            if not isinstance(column, str):
                return ValidationResult("NO_WHITESPACE_CHECK", "FAIL", "column must be a string")

            failures = {}
            for file_path, df in file_dfs.items():
                df_cols = {c.lower(): c for c in df.columns}
                if column.lower() not in df_cols:
                    failures[file_path] = f"Column '{column}' not found"
                    continue
                actual_col = df_cols[column.lower()]
                dtype = df.schema[actual_col].dataType
                if not isinstance(dtype, StringType):
                    failures[file_path] = "Non-string column"
                    continue
                if df.filter(col(actual_col).isNotNull() & col(actual_col).rlike(r"\s")).limit(1).count() > 0:
                    failures[file_path] = "Whitespace detected"

            if failures:
                return ValidationResult("NO_WHITESPACE_CHECK", "FAIL",
                                        "; ".join(f"{k}: {v}" for k, v in failures.items()))
            return ValidationResult("NO_WHITESPACE_CHECK", "PASS",
                                    f"No whitespace found in column '{column}'")
        except Exception as e:
            return ValidationResult("NO_WHITESPACE_CHECK", "FAIL",
                                    f"Unable to validate whitespace: {str(e)}")

    def check_string_length(self, file_dfs: dict, column: str, min_len: int, max_len: int) -> ValidationResult:
        """
        FAIL if ANY non-null trimmed string value falls outside [min_len, max_len].
        """
        try:
            if not isinstance(column, str):
                return ValidationResult("STRING_LENGTH_CHECK", "FAIL", "column must be a string")
            if not isinstance(min_len, int) or not isinstance(max_len, int):
                return ValidationResult("STRING_LENGTH_CHECK", "FAIL", "min_len and max_len must be integers")
            if min_len < 0 or max_len < min_len:
                return ValidationResult("STRING_LENGTH_CHECK", "FAIL",
                                        "Invalid bounds: min_len must be >= 0 and <= max_len")

            failures = {}
            for file_path, df in file_dfs.items():
                df_cols = {c.lower(): c for c in df.columns}
                if column.lower() not in df_cols:
                    failures[file_path] = "Column not found"
                    continue
                actual_col = df_cols[column.lower()]  # [FIX-09] removed stray print()
                dtype = df.schema[actual_col].dataType
                if not isinstance(dtype, StringType):
                    failures[file_path] = "Non-string column"
                    continue
                trimmed_len = length(trim(col(actual_col)))
                if df.filter(
                    col(actual_col).isNotNull() &
                    ((trimmed_len < min_len) | (trimmed_len > max_len))
                ).limit(1).count() > 0:
                    failures[file_path] = f"String length out of bounds [{min_len}, {max_len}]"

            if failures:
                return ValidationResult("STRING_LENGTH_CHECK", "FAIL",
                                        "; ".join(f"{k}: {v}" for k, v in failures.items()))
            return ValidationResult("STRING_LENGTH_CHECK", "PASS",
                                    f"All values in '{column}' meet length constraints [{min_len}, {max_len}]")
        except Exception as e:
            return ValidationResult("STRING_LENGTH_CHECK", "FAIL",
                                    f"Unable to validate string length: {str(e)}")

    def check_row_count(self, file_dfs: dict, min_count: int = None, max_count: int = None) -> ValidationResult:
        """
        FAIL if ANY file's row count falls outside [min_count, max_count].
        """
        try:
            if not isinstance(min_count, int) or not isinstance(max_count, int):
                return ValidationResult("ROW_COUNT_CHECK", "FAIL",
                                        "min_count and max_count must be integers")
            if min_count < 0 or max_count < min_count:
                return ValidationResult("ROW_COUNT_CHECK", "FAIL",
                                        "Invalid bounds: min_count must be >= 0 and <= max_count")

            failures = {}
            for file_path, df in file_dfs.items():
                rc = df.count()
                if rc < min_count:
                    failures[file_path] = f"Row count {rc} < min {min_count}"
                elif rc > max_count:
                    failures[file_path] = f"Row count {rc} > max {max_count}"

            if failures:
                return ValidationResult("ROW_COUNT_CHECK", "FAIL",
                                        "; ".join(f"{f}: {r}" for f, r in failures.items()))
            return ValidationResult("ROW_COUNT_CHECK", "PASS", "Row count within expected limits")
        except Exception as e:
            return ValidationResult("ROW_COUNT_CHECK", "FAIL",
                                    f"Unable to validate row count: {str(e)}")

    def check_completeness_ratio(self, file_dfs: dict, threshold_pct: float) -> ValidationResult:
        """
        FAIL if overall non-null cell ratio falls below threshold_pct (0–100).
        """
        try:
            if not isinstance(threshold_pct, (int, float)):
                return ValidationResult("COMPLETENESS_RATIO_CHECK", "FAIL",
                                        "threshold_pct must be a number")
            if not (0 <= threshold_pct <= 100):
                return ValidationResult("COMPLETENESS_RATIO_CHECK", "FAIL",
                                        "threshold_pct must be between 0 and 100")

            failures = {}
            for file_path, df in file_dfs.items():
                rows = df.count()
                cols = len(df.columns)
                if rows == 0 or cols == 0:
                    failures[file_path] = "Empty dataset; completeness undefined"
                    continue
                non_null_counts = (
                    df.select([count(col(c)).alias(c) for c in df.columns])
                    .collect()[0].asDict()
                )
                completeness = (builtins.sum(non_null_counts.values()) / (rows * cols)) * 100
                if completeness < threshold_pct:
                    failures[file_path] = f"Completeness {completeness:.2f}% < threshold {threshold_pct}%"

            if failures:
                return ValidationResult("COMPLETENESS_RATIO_CHECK", "FAIL",
                                        "; ".join(f"{f}: {r}" for f, r in failures.items()))
            return ValidationResult("COMPLETENESS_RATIO_CHECK", "PASS",
                                    "Completeness ratio meets the threshold for all files")
        except Exception as e:
            return ValidationResult("COMPLETENESS_RATIO_CHECK", "FAIL",
                                    f"Unable to validate completeness ratio: {str(e)}")

    def check_unique(self, file_dfs: dict, key_columns: list) -> ValidationResult:
        """
        FAIL if ANY combination of key_columns is not unique within a file.
        """
        try:
            error = self._validate_col_list(key_columns, "UNIQUE_CHECK")
            if error:
                return error

            key_norm = {c.lower(): c for c in key_columns}
            failures = {}

            for file_path, df in file_dfs.items():
                df_norm = {c.lower(): c for c in df.columns}
                missing = [orig for lc, orig in key_norm.items() if lc not in df_norm]
                if missing:
                    failures[file_path] = f"Missing key columns: {missing}"
                    continue
                actual_keys = [df_norm[c.lower()] for c in key_columns]
                total = df.count()
                distinct = df.select(*actual_keys).distinct().count()
                if distinct < total:
                    failures[file_path] = f"{total - distinct} duplicate key combination(s) found"

            if failures:
                return ValidationResult("UNIQUE_CHECK", "FAIL",
                                        "; ".join(f"{f}: {r}" for f, r in failures.items()))
            return ValidationResult("UNIQUE_CHECK", "PASS", "All key column combinations are unique")
        except Exception as e:
            return ValidationResult("UNIQUE_CHECK", "FAIL",
                                    f"Unable to validate uniqueness: {str(e)}")

    def check_kpi_aggregate(self, file_dfs: dict, expr: str, min_val, max_val) -> ValidationResult:
        """
        FAIL if the aggregate expression result falls outside [min_val, max_val].
        """
        try:
            if not isinstance(expr, str) or not expr.strip():
                return ValidationResult("KPI_AGGREGATE_CHECK", "FAIL",
                                        "expr must be a non-empty string")
            if not all(isinstance(v, (int, float)) for v in [min_val, max_val]):
                return ValidationResult("KPI_AGGREGATE_CHECK", "FAIL",
                                        "min_val and max_val must be numeric")
            if max_val < min_val:
                return ValidationResult("KPI_AGGREGATE_CHECK", "FAIL",
                                        "max_val must be >= min_val")

            failures = {}
            for file_path, df in file_dfs.items():
                try:
                    agg_value = (
                        df.select(spark_expr(expr.lower()).alias("kpi_value"))
                        .collect()[0]["kpi_value"]
                    )
                except Exception:
                    failures[file_path] = f"Invalid expression: {expr}"
                    continue
                if agg_value is None:
                    failures[file_path] = "Aggregate value is NULL"
                    continue
                if not (min_val <= agg_value <= max_val):
                    failures[file_path] = f"Aggregate {agg_value} not in [{min_val}, {max_val}]"

            if failures:
                return ValidationResult("KPI_AGGREGATE_CHECK", "FAIL",
                                        "; ".join(f"{f}: {r}" for f, r in failures.items()))
            return ValidationResult("KPI_AGGREGATE_CHECK", "PASS",
                                    "KPI aggregate expression within expected range")
        except Exception as e:
            return ValidationResult("KPI_AGGREGATE_CHECK", "FAIL",
                                    f"Unable to validate KPI aggregate: {str(e)}")

    def check_value_range(self, file_dfs: dict, column_name: str, min_val, max_val) -> ValidationResult:
        """
        FAIL if ANY non-null value in the column falls outside [min_val, max_val].
        """
        try:
            if not isinstance(column_name, str):
                return ValidationResult("VALUE_RANGE_CHECK", "FAIL",
                                        "column_name must be a string")
            if not all(isinstance(v, (int, float)) for v in [min_val, max_val]):
                return ValidationResult("VALUE_RANGE_CHECK", "FAIL",
                                        "min_val and max_val must be numeric")
            if max_val < min_val:
                return ValidationResult("VALUE_RANGE_CHECK", "FAIL",
                                        "max_val must be >= min_val")

            failures = {}
            for file_path, df in file_dfs.items():
                df_cols = {c.lower(): c for c in df.columns}
                if column_name.lower() not in df_cols:
                    failures[file_path] = f"Missing column: {column_name}"
                    continue
                actual_col = df_cols[column_name.lower()]
                cast_col = col(actual_col).cast("double")
                if df.filter(col(actual_col).isNotNull() & cast_col.isNull()).count() > 0:
                    failures[file_path] = "Non-numeric values found"
                    continue
                oor = df.filter(cast_col.isNotNull() & ((cast_col < min_val) | (cast_col > max_val))).count()
                if oor > 0:
                    failures[file_path] = f"{oor} values outside range [{min_val}, {max_val}]"

            if failures:
                return ValidationResult("VALUE_RANGE_CHECK", "FAIL",
                                        "; ".join(f"{f}: {r}" for f, r in failures.items()))
            return ValidationResult("VALUE_RANGE_CHECK", "PASS",
                                    "All values within expected range")
        except Exception as e:
            return ValidationResult("VALUE_RANGE_CHECK", "FAIL",
                                    f"Unable to validate value range: {str(e)}")

    def check_regex_format(self, file_dfs: dict, column_name: str, pattern: str) -> ValidationResult:
        """
        FAIL if ANY non-null value does not match the given regex pattern.
        """
        try:
            if not isinstance(column_name, str):
                return ValidationResult("REGEX_FORMAT_CHECK", "FAIL",
                                        "column_name must be a string")
            if not isinstance(pattern, str) or not pattern:
                return ValidationResult("REGEX_FORMAT_CHECK", "FAIL",
                                        "pattern must be a non-empty string")

            failures = {}
            for file_path, df in file_dfs.items():
                df_cols = {c.lower(): c for c in df.columns}
                if column_name.lower() not in df_cols:
                    failures[file_path] = f"Missing column: {column_name}"
                    continue
                actual_col = df_cols[column_name.lower()]
                invalid = df.filter(
                    col(actual_col).isNotNull() &
                    (lower(trim(col(actual_col))) != "null") &
                    ~trim(col(actual_col)).rlike(pattern)
                ).count()
                if invalid > 0:
                    failures[file_path] = f"{invalid} value(s) do not match pattern '{pattern}'"

            if failures:
                return ValidationResult("REGEX_FORMAT_CHECK", "FAIL",
                                        "; ".join(f"{f}: {r}" for f, r in failures.items()))
            return ValidationResult("REGEX_FORMAT_CHECK", "PASS",
                                    "All values match the expected regex format")
        except Exception as e:
            return ValidationResult("REGEX_FORMAT_CHECK", "FAIL",
                                    f"Unable to validate regex format: {str(e)}")

    def check_no_future_date(self, file_dfs: dict, date_column: str, date_format: str) -> ValidationResult:
        """
        FAIL if ANY parsed date is after today.
        """
        try:
            if not isinstance(date_column, str):
                return ValidationResult("NO_FUTURE_DATE_CHECK", "FAIL",
                                        "date_column must be a string")
            if not isinstance(date_format, str) or not date_format:
                return ValidationResult("NO_FUTURE_DATE_CHECK", "FAIL",
                                        "date_format must be a non-empty string")

            failures = {}
            for file_path, df in file_dfs.items():
                df_cols = {c.lower(): c for c in df.columns}
                if date_column.lower() not in df_cols:
                    failures[file_path] = f"Missing column: {date_column}"
                    continue
                actual_col = df_cols[date_column.lower()]
                parsed = to_date(trim(col(actual_col)), date_format)
                invalid_fmt = df.filter(
                    col(actual_col).isNotNull() &
                    (lower(trim(col(actual_col))) != "null") &
                    parsed.isNull()
                ).count()
                if invalid_fmt > 0:
                    failures[file_path] = f"{invalid_fmt} invalid date value(s)"
                    continue
                future = df.filter(parsed > current_date()).count()
                if future > 0:
                    failures[file_path] = f"{future} future date value(s) found"

            if failures:
                return ValidationResult("NO_FUTURE_DATE_CHECK", "FAIL",
                                        "; ".join(f"{f}: {r}" for f, r in failures.items()))
            return ValidationResult("NO_FUTURE_DATE_CHECK", "PASS",
                                    "No future dates detected")
        except Exception as e:
            return ValidationResult("NO_FUTURE_DATE_CHECK", "FAIL",
                                    f"Unable to validate future dates: {str(e)}")

    def check_duplicate_rows(self, file_dfs: dict) -> ValidationResult:
        """
        FAIL if ANY file contains fully duplicate rows.

        [FIX-08] Replaced double full-scan (count + distinct().count()) with a
        groupBy approach.  This surfaces the number of *distinct* duplicate groups
        (i.e. how many unique rows appear more than once) rather than just the
        surplus row count — more actionable for debugging.
        """
        try:
            failures = {}
            for file_path, df in file_dfs.items():
                dup_groups = (
                    df.groupBy(df.columns)
                    .count()
                    .filter(col("count") > 1)
                    .count()
                )
                if dup_groups > 0:
                    failures[file_path] = f"{dup_groups} distinct duplicate row group(s) found"

            if failures:
                return ValidationResult("DUPLICATE_ROWS_CHECK", "FAIL",
                                        "; ".join(f"{f}: {r}" for f, r in failures.items()))
            return ValidationResult("DUPLICATE_ROWS_CHECK", "PASS",
                                    "No duplicate rows found")
        except Exception as e:
            return ValidationResult("DUPLICATE_ROWS_CHECK", "FAIL",
                                    f"Unable to validate duplicate rows: {str(e)}")
