# Databricks notebook source
import re
import builtins
import os
import hashlib

from dataclasses import dataclass, field
from datetime import datetime,timezone
from typing import List, Optional, Callable,Dict
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import expr as spark_expr

# COMMAND ----------

@dataclass
class ValidationResult:
    check_name: str
    status: str   # PASS / FAIL
    message: str
    expected: Optional[str] = None
    actual: Optional[str] = None

    def __str__(self):
        return f"{self.check_name}: {self.message}"

# COMMAND ----------

# DBTITLE 1,FileChecks
class FileChecks:
    def __init__(self, spark):
        self.spark = spark


# ── name pattern and it's count ──────────────────────────────────────────────────────────
    def check_file_count(self, path: str, regex: str) -> ValidationResult:

        """
        Counts files in a given path whose filenames match the regex.
        FAIL if zero files are found.
        """

        try:
            pattern = re.compile(regex)
            files = dbutils.fs.ls(path)

            matched_files = [
                f.name for f in files
                if pattern.match(f.name)
                # not f.isDir() and
            ]

            count = len(matched_files)

            if count == 0:
                return ValidationResult(
                    "FILE_COUNT_CHECK",
                    "FAIL",
                    f"FAIL: No files found matching regex '{regex}' in path '{path}'"
                )

            return ValidationResult(
                "FILE_COUNT_CHECK",
                "PASS",
                f"PASS: {count} file(s) found matching regex '{regex}'"
            )

        except Exception as e:
            return ValidationResult(
                "FILE_COUNT_CHECK",
                "FAIL",
                f"FAIL: Error while counting files in '{path}': {str(e)}"
            )

# ── name pattern with expected count ──────────────────────────────────────────────────────────        
    def check_expected_file_count(self, path: str, regex: str , expected = int) -> ValidationResult:

        """
        Counts files in a given path whose filenames match the regex.
        FAIL if expected files count are not found.
        """

        try:
            
            if not isinstance(expected, int) or expected < 0:
                return ValidationResult(
                    "EXPECTED_FILE_COUNT_CHECK",
                    "FAIL",
                    f"FAIL: Invalid expected value '{expected}'. Must be a non-negative integer."
                )

            pattern = re.compile(regex)
            files = dbutils.fs.ls(path)

            matched_files = [
                f.name for f in files
                if pattern.match(f.name)
                # not f.isDir() and
            ]

            count = len(matched_files)

            if count != expected:
                return ValidationResult(
                    "EXPECTED_FILE_COUNT_CHECK",
                    "FAIL",
                    f"FAIL: There were {expected} file were expected but received '{count}' in path '{path}'"
                )

            return ValidationResult(
                "EXPECTED_FILE_COUNT_CHECK",
                "PASS",
                f"PASS: {count} file(s) found matching regex '{regex}'"
            )

        except Exception as e:
            return ValidationResult(
                "EXPECTED_FILE_COUNT_CHECK",
                "FAIL",
                f"FAIL: Error while counting files in '{path}': {str(e)}"
            )

# ── check  size  ──────────────────────────────────────────────────────────
    def check_size(self,  path: str,   regex: str,    min_size: int,     max_size: int   ) -> ValidationResult:
        """
        Validates the size (in bytes) of each file in a given path
        whose filenames match the regex.

        PASS if ALL matched files have size within [min_size, max_size].
        FAIL if ANY file is outside the expected size range.
        """

        try:
            pattern = re.compile(regex)
            files = dbutils.fs.ls(path)

            matched_files = [
                f for f in files
                if pattern.match(f.name)
                # and not f.isDir()
            ]

            if not matched_files:
                return ValidationResult(
                    "FILE_SIZE_CHECK",
                    "FAIL",
                    f"FAIL: No files found matching regex '{regex}' in path '{path}'"
                )

            if not isinstance(min_size, int) or not isinstance(max_size, int):
                return ValidationResult(
                    "STRING_LENGTH_CHECK",
                    "FAIL",
                    "min_size and max_size must be integers"
                )

            if min_size < 0 or max_size < min_size:
                return ValidationResult(
                    "STRING_LENGTH_CHECK",
                    "FAIL",
                    "Invalid length bounds"
                )

            for f in matched_files:
                if not hasattr(f, "size"):
                    return ValidationResult(
                        "FILE_SIZE_CHECK",
                        "FAIL",
                        f"FAIL: Files doesn't have size attribute for matching regex '{regex}' in path '{path}'"
                    )

            invalid_files = [
                (f.name, f.size)
                for f in matched_files
                if f.size < min_size or f.size > max_size
            ]

            if invalid_files:
                details = ", ".join(
                    [f"{name}({size} bytes)" for name, size in invalid_files]
                )

                return ValidationResult(
                    "FILE_SIZE_CHECK",
                    "FAIL",
                    (
                        f"FAIL: The following files are outside the expected size range "
                        f"[{min_size}, {max_size}] bytes in path '{path}': {details}"
                    )
                )

            return ValidationResult(
                "FILE_SIZE_CHECK",
                "PASS",
                (
                    f"PASS: All {len(matched_files)} file(s) matching regex '{regex}' "
                    f"are within the expected size range "
                    f"[{min_size}, {max_size}] bytes"
                )
            )

        except Exception as e:
            return ValidationResult(
                "FILE_SIZE_CHECK",
                "FAIL",
                f"FAIL: Error while validating file sizes in '{path}': {str(e)}"
            )

# ── check_duplicate_files  ──────────────────────────────────────────────────────────
    def check_duplicate_file( self, path: str, regex: str ) -> ValidationResult:
        """
        Checks for duplicate files in a given path whose filenames
        match the regex.

        A duplicate is defined as:
        - Same file name (exact match)
        - Same file size (exact match)

        PASS if no duplicates are found.
        FAIL if any duplicates are detected.
        """

        try:
            pattern = re.compile(regex)
            files = dbutils.fs.ls(path)

            matched_files = [
                f for f in files
                if pattern.search(f.name)
                # and not f.isDir()
            ]

            if not matched_files:
                return ValidationResult(
                    "DUPLICATE_FILE_CHECK",
                    "FAIL",
                    f"FAIL: No files found matching regex '{regex}' in path '{path}'"
                )

            
            file_tracker = {}
            duplicates = []

            for f in matched_files:
                
                if not hasattr(f, "size"):
                    return ValidationResult(
                        "DUPLICATE_FILE_CHECK",
                        "FAIL",
                        f"FAIL: Files doesn't have size attribute for matching regex '{regex}' in path '{path}'"
                    )

                key = (f.name, f.size)

                if key in file_tracker:
                    duplicates.append(
                        f"{f.name} ({f.size} bytes)"
                    )
                else:
                    file_tracker[key] = f.path


            if duplicates:
                total_duplicates = len(duplicates)
                shown_duplicates = duplicates[:5]

                duplicate_details = ", ".join(shown_duplicates)

                if total_duplicates > 5:
                    duplicate_details += (
                        f", ... and {total_duplicates - 5} more"
                    )

                return ValidationResult(
                    "DUPLICATE_FILE_CHECK",
                    "FAIL",
                    (
                        f"FAIL: Duplicate files detected in path '{path}': "
                        f"{duplicate_details}"
                    )
                )

            return ValidationResult(
                "DUPLICATE_FILE_CHECK",
                "PASS",
                (
                    f"PASS: No duplicate files found for files matching "
                    f"regex '{regex}' in given path "
                )
            )

        except Exception as e:
            return ValidationResult(
                "DUPLICATE_FILE_CHECK",
                "FAIL",
                f"FAIL: Error while checking duplicate files in '{path}': {str(e)}"
            )

# ── check_file_already_archived  ──────────────────────────────────────────────────────────
    def check_file_already_archived(self, source_path :str, regex: str, archive_path: str) -> ValidationResult:
        """
        Checks whether files matching the given regex already exist
        in the archive location.

        FAIL if any matching file is found.
        PASS if no matching files are present.
        """

        try:
            if archive_path is None or not str(archive_path).strip():
                return ValidationResult(
                    "ARCHIVE_DUPLICATE_CHECK",
                    "FAIL",
                    f"FAIL: Unable to access archive location '{archive_path}'"

                )

            pattern         = re.compile(regex)
            source_files    = dbutils.fs.ls(source_path)
            archive_files   = dbutils.fs.ls(archive_path)
            
            matched_source_files = [
                f.name for f in source_files
                if pattern.search(f.name)

            ]
            matched_archived_files = [
                f.name for f in archive_files
                if pattern.search(f.name)

            ]

            archive_duplicate_file = []
            for f in matched_source_files:
                if f in matched_archived_files:
                    archive_duplicate_file.append(f)
                    
            if archive_duplicate_file:
                return ValidationResult(
                    "ARCHIVE_DUPLICATE_CHECK",
                    "FAIL",
                    (
                        f"FAIL: File(s) already exist in archive path '{archive_path}': "
                        f"{', '.join(archive_duplicate_file[:5])}"
                        + ("..." if len(archive_duplicate_file) > 5 else "")
                    )
                )

            return ValidationResult(
                "ARCHIVE_DUPLICATE_CHECK",
                "PASS",
                (
                    f"PASS: No files found in archive path '{archive_path}' "
                    f"matching regex '{regex}'"
                )
            )

        except Exception as e:
            return ValidationResult(
                "ARCHIVE_DUPLICATE_CHECK",
                "FAIL",
                f"FAIL: Error while checking archive path '{archive_path}': {str(e)}"
            )

# ── check_file_freshness  ──────────────────────────────────────────────────────────
    def check_file_freshness(self, path: str, regex: str, max_age_hours: int ) -> ValidationResult:
        """
        Checks the freshness of files in a given path whose filenames
        match the regex.

        A file is considered fresh if:
        current_time - file_modification_time <= max_age_hours

        PASS if ALL matched files are fresh.
        FAIL if ANY file is older than max_age_hours.
        """

        try:
            pattern = re.compile(regex)
            files = dbutils.fs.ls(path)

            matched_files = [
                f for f in files
                if pattern.match(f.name)
                # and not f.isDir()
            ]

            if not matched_files:
                return ValidationResult(
                    "FILE_FRESHNESS_CHECK",
                    "FAIL",
                    f"FAIL: No files found matching regex '{regex}' in path '{path}'"
                )

            current_time = datetime.now(timezone.utc)
            stale_files = []

            for f in matched_files:
                # dbutils provides modificationTime in milliseconds

                if not hasattr(f, "modificationTime"):
                    return ValidationResult(
                        "FILE_FRESHNESS_CHECK",
                        "FAIL",
                        f"FAIL: Files doesn't have modificationTime attribute for matching regex '{regex}' in path '{path}'"
                    )
                file_mod_time = datetime.fromtimestamp(
                    f.modificationTime / 1000, tz=timezone.utc
                )

                age_in_hours = (current_time - file_mod_time).total_seconds() / 3600

                if age_in_hours > max_age_hours:
                    stale_files.append(
                        f"{f.name} ({age_in_hours:.2f} hours old)"
                    )

            if stale_files:
                details = ", ".join(stale_files)

                return ValidationResult(
                    "FILE_FRESHNESS_CHECK",
                    "FAIL",
                    (
                        f"FAIL: The following files are older than "
                        f"{max_age_hours} hours in path '{path}': {details}"
                    )
                )

            return ValidationResult(
                "FILE_FRESHNESS_CHECK",
                "PASS",
                (
                    f"PASS: All {len(matched_files)} file(s) matching regex '{regex}' "
                    f"are within the freshness threshold of {max_age_hours} hours"
                )
            )

        except Exception as e:
            return ValidationResult(
                "FILE_FRESHNESS_CHECK",
                "FAIL",
                f"FAIL: Error while checking file freshness in '{path}': {str(e)}"
            )

# ── check_file_extension  ──────────────────────────────────────────────────────────
    def check_file_extension( self, path: str, regex: str,  allowed_ext: list ) -> ValidationResult:
        """
        Validates that each file matching the regex has an allowed extension.

        PASS if ALL matched files have extensions in allowed_ext.
        FAIL if ANY file has an invalid extension.
        """

        try:
            pattern = re.compile(regex)
            files = dbutils.fs.ls(path)

            matched_files = [
                f for f in files
                if pattern.match(f.name)
            ]


            allowed_ext_normalized = [ext.lower().lstrip(".") for ext in allowed_ext]

            invalid_files = []
            for f in matched_files:
                if "." not in f.name:
                    invalid_files.append(f.name)
                else:
                    ext = f.name.rsplit(".", 1)[-1].lower()
                    if ext not in allowed_ext_normalized:
                        invalid_files.append(f.name)

            if invalid_files:
                details = ", ".join(invalid_files)
                return ValidationResult(
                    "FILE_EXTENSION_CHECK",
                    "FAIL",
                    (
                        f"FAIL: The following files have extensions not in the allowed list "
                        f"{allowed_ext}: {details}"
                    )
                )

            return ValidationResult(
                "FILE_EXTENSION_CHECK",
                "PASS",
                (
                    f"PASS: All {len(matched_files)} file(s) matching regex '{regex}' "
                    f"have allowed extensions {allowed_ext}"
                )
            )

        except Exception as e:
            return ValidationResult(
                "FILE_EXTENSION_CHECK",
                "FAIL",
                f"FAIL: Error while validating file extensions in '{path}': {str(e)}"
            )

# ── check_file_compression  ──────────────────────────────────────────────────────────
    def check_file_compression( self, path: str, regex: str, allowed_compressions: list ) -> ValidationResult:
        """
        Validates that each file matching the regex uses an allowed compression format.

        PASS if ALL matched files use allowed compression.
        FAIL if ANY file uses a disallowed compression.
        """

        try:
            pattern = re.compile(regex)
            files = dbutils.fs.ls(path)

            matched_files = [
                f for f in files
                if pattern.match(f.name)
            ]


            allowed_comp_normalized = [
                comp.lower().lstrip(".") for comp in allowed_compressions
            ]

            invalid_files = []
            for f in matched_files:
                name_parts = f.name.lower().split(".")
                compression = name_parts[-1] if len(name_parts) > 1 else None

                if compression not in allowed_comp_normalized:
                    invalid_files.append(f.name)

            if invalid_files:
                details = ", ".join(invalid_files)
                return ValidationResult(
                    "FILE_COMPRESSION_CHECK",
                    "FAIL",
                    (
                        f"FAIL: The following files use compression formats not in "
                        f"allowed list {allowed_compressions}: {details}"
                    )
                )

            return ValidationResult(
                "FILE_COMPRESSION_CHECK",
                "PASS",
                (
                    f"PASS: All {len(matched_files)} file(s) matching regex '{regex}' "
                    f"use allowed compression formats {allowed_compressions}"
                )
            )

        except Exception as e:
            return ValidationResult(
                "FILE_COMPRESSION_CHECK",
                "FAIL",
                f"FAIL: Error while validating file compression in '{path}': {str(e)}"
            )

# COMMAND ----------

# DBTITLE 1,SchemaChecks
class SchemaChecks:
    def __init__(self, spark):
        self.spark = spark
        # self._df_cache = {}  # path -> cached DataFrame

    @staticmethod
    def _validate_col_list(required_cols , check_name):
            # Validate required_cols type
            if not isinstance(required_cols, list):
                return ValidationResult(
                    f"{check_name}",
                    "FAIL",
                    f"cols list must be a list of column names, got {type(required_cols).__name__}"
                )

            # Validate required_cols not empty
            if not required_cols:
                return ValidationResult(
                    f"{check_name}",
                    "FAIL",
                    "cols list cannot be empty"
                )

            # Validate all required_cols are strings
            non_string_cols = [c for c in required_cols if not isinstance(c, str)]
            if non_string_cols:
                return ValidationResult(
                    f"{check_name}",
                    "FAIL",
                    f"All cols must be strings. Invalid entries: {non_string_cols}"
                )

            # Validate no duplicates (case-insensitive)
            lowered = [c.lower() for c in required_cols]
            if len(lowered) != len(set(lowered)):
                return ValidationResult(
                    f"{check_name}",
                    "FAIL",
                    f"Duplicate column names found in cols list (case-insensitive): {required_cols}"
                )

            return None
        
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

    def check_required_columns(self, file_dfs: dict,  required_cols: list,  allow_extra: bool = False) -> ValidationResult:
        
        """
        Validates that all input DataFrames contain the required set of columns.

        The check is case-insensitive and ensures:
        - All required columns are present in each DataFrame.
        - No extra columns are present, unless allow_extra is set to True.

        PASS if ALL files:
        - Contain every column listed in required_cols
        - And (when allow_extra=False) contain no additional columns

        FAIL if ANY file:
        - Is missing one or more required columns
        - Or contains extra columns when allow_extra=False
        """

        try:

            error =  self._validate_col_list(required_cols , 'REQUIRED_COLUMNS_CHECK')
            if error: return error

            if not isinstance(allow_extra, bool):
                return ValidationResult(
                    "REQUIRED_COLUMNS_CHECK",
                    "FAIL",
                    f"allow_extra must be a boolean (True/False), got {type(allow_extra).__name__}")

            failures = {}

            # Normalize required columns once (lowercase)
            required_norm = {c.lower(): c for c in required_cols}

            for file_path, df in file_dfs.items():
                # Map normalized -> actual column name from dataframe
                df_norm = {c.lower(): c for c in df.columns}

                # Missing columns (case-insensitive)
                missing_cols = [
                    orig_name
                    for norm_name, orig_name in required_norm.items()
                    if norm_name not in df_norm
                ]

                # Extra columns (case-insensitive) only if allow_extra is False
                extra_cols = []
                if not allow_extra:
                    extra_cols = [
                        actual_name
                        for norm_name, actual_name in df_norm.items()
                        if norm_name not in required_norm
                    ]

                if missing_cols or extra_cols:
                    failures[file_path] = {
                        "missing": missing_cols,
                        "extra": extra_cols
                    }

            if failures:
                details = "; ".join(
                    f"{file}: missing={info['missing']}, extra={info['extra']}"
                    for file, info in failures.items()
                )

                return ValidationResult(
                    "REQUIRED_COLUMNS_CHECK",
                    "FAIL",
                    f"Column validation failed: {details}"
                )

            return ValidationResult(
                "REQUIRED_COLUMNS_CHECK",
                "PASS",
                (
                    f"All files have required columns {required_cols} "
                    f"(case-insensitive). allow_extra={allow_extra}"
                )
            )

        except Exception as e:
            return ValidationResult(
                "REQUIRED_COLUMNS_CHECK",
                "FAIL",
                f"Unable to validate required columns: {str(e)}"
            )

    def check_forbidden_columns(self, file_dfs: dict, forbidden_cols: list) -> ValidationResult:
        
        """
        Validates that none of the forbidden columns are present
        in any of the input DataFrames/files.

        The check is case-insensitive and compares DataFrame column
        names against the provided forbidden column list.

        PASS if ALL files:
        - Do not contain any column listed in forbidden_cols

        FAIL if ANY file:
        - Contains one or more forbidden columns
        """

        try:
            error = self._validate_col_list(forbidden_cols, 'FORBIDDEN_COLUMN_CHECK')
            if error:
                return error

            # Normalize forbidden columns once
            forbidden_map = {c.casefold(): c for c in forbidden_cols}

            failures = {}

            for file_path, df in file_dfs.items():
                # Normalize dataframe columns
                df_cols_casefold = {col.casefold(): col for col in df.columns}

                # Find matches (case-insensitive)
                present_forbidden = [
                    forbidden_map[c]
                    for c in forbidden_map
                    if c in df_cols_casefold
                ]

                if present_forbidden:
                    failures[file_path] = present_forbidden

            if failures:
                details = "; ".join(
                    f"{file}: forbidden columns present {cols}"
                    for file, cols in failures.items()
                )

                return ValidationResult(
                    "FORBIDDEN_COLUMN_CHECK",
                    "FAIL",
                    f"Forbidden columns found: {details}"
                )

            return ValidationResult(
                "FORBIDDEN_COLUMN_CHECK",
                "PASS",
                "No forbidden columns found across all files"
            )

        except Exception as e:
            return ValidationResult(
                "FORBIDDEN_COLUMN_CHECK",
                "FAIL",
                f"Unable to validate forbidden columns: {str(e)}"
            )

    def check_data_dtype(self, file_dfs: dict, type_map: dict,  allowed_extra: bool = False ) -> ValidationResult:
             
        """
        Validates that DataFrame/files columns match the expected logical data types
        defined in the type_map.

        Column name comparison is case-insensitive, and Spark-specific data
        types are normalized to logical types before comparison:
        - StringType           -> "string"
        - Integer/Long/Short   -> "integer"
        - Float/Double/Decimal -> "float"
        - BooleanType          -> "boolean"
        - DateType             -> "date"
        - TimestampType        -> "timestamp"

        The behavior depends on allowed_extra:
        - If allowed_extra is False, columns present in the DataFrame but not
        defined in type_map will cause FAILURE.
        - If allowed_extra is True, extra columns are ignored.

        PASS if ALL files:
        - Contain all columns defined in type_map (case-insensitive)
        - Have data types that match the expected normalized logical types
        - And (when allowed_extra=False) contain no additional columns

        FAIL if ANY file:
        - Is missing an expected column
        - Has a mismatched data type after normalization
        - Or contains extra columns when allowed_extra=False
        """


        try:
            # Validate allowed_extra
            if not isinstance(allowed_extra, bool):
                return ValidationResult(
                    "DATA_DTYPE_CHECK",
                    "FAIL",
                    f"allowed_extra must be a boolean, got {type(allowed_extra).__name__}"
                )

            # Validate type_map
            if not isinstance(type_map, dict) or not type_map:
                return ValidationResult(
                    "DATA_DTYPE_CHECK",
                    "FAIL",
                    "type_map must be a non-empty dict of column:datatype"
                )

            for k, v in type_map.items():
                if not isinstance(k, str) or not isinstance(v, str):
                    return ValidationResult(
                        "DATA_DTYPE_CHECK",
                        "FAIL",
                        "type_map must contain only string:string mappings"
                    )

            failures = {}

            for file_path, df in file_dfs.items():
                file_failures = {}

                # schema_fields = {f.name: f.dataType for f in df.schema.fields}
                
                schema_fields = {
                    f.name.lower(): f.dataType
                    for f in df.schema.fields
                }

                type_map = {
                    col.lower(): dtype
                    for col, dtype in type_map.items()
                }

                df_cols = set(schema_fields.keys())
                expected_cols = set(type_map.keys())

                # Strict mode: no extra columns
                if not allowed_extra:
                    extra_cols = df_cols - expected_cols
                    if extra_cols:
                        file_failures["EXTRA_COLUMNS"] = list(extra_cols)

                #  Validate expected columns
                for col, expected_type in type_map.items():
                    if col not in schema_fields:
                        file_failures[col] = "Column not found"
                        continue

                    actual_type =self.normalize_spark_dtype(schema_fields[col])
                    expected_type_norm = expected_type.lower()

                    if actual_type != expected_type_norm:
                        file_failures[col] = (
                            f"Expected {actual_type}, found {expected_type_norm}"
                        )

                if file_failures:
                    failures[file_path] = file_failures

            if failures:
                details = "; ".join(
                    f"{file}: {issues}"
                    for file, issues in failures.items()
                )

                return ValidationResult(
                    "DATA_DTYPE_CHECK",
                    "FAIL",
                    f"Data type validation failed: {details}"
                )

            return ValidationResult(
                "DATA_DTYPE_CHECK",
                "PASS",
                f"All Spark schemas match expected data types (allowed_extra={allowed_extra})"
            )

        except Exception as e:
            return ValidationResult(
                "DATA_DTYPE_CHECK",
                "FAIL",
                f"Unable to validate data types: {str(e)}"
            )

    def check_column_order( self, file_dfs: dict,  expected_order: list ) -> ValidationResult:
        
        """
        Validates that DataFrame/files columns appear in the exact expected order.

        The comparison is case-insensitive, but the relative ordering of
        columns must match the expected_order exactly.

        PASS if ALL files:
        - Contain the same columns as expected_order
        - And the column sequence matches expected_order (case-insensitive)

        FAIL if ANY file:
        - Has columns in a different order
        - Is missing expected columns
        - Or contains extra columns not listed in expected_order
        """

        try:
            error = self._validate_col_list( expected_order, 'COLUMN_ORDER_CHECK' )
            if error:   return error

            failures = {}
            expected_norm = [c.lower() for c in expected_order]

            for file_path, df in file_dfs.items():
                actual_cols = list(df.columns)
                actual_norm = [c.lower() for c in actual_cols]

                if actual_norm != expected_norm:
                    failures[file_path] = {
                        "expected": expected_order,
                        "actual": actual_cols,
                        "missing": [
                            c for c in expected_order
                            if c.lower() not in actual_norm
                        ],
                        "extra": [
                            c for c in actual_cols
                            if c.lower() not in expected_norm
                        ]
                    }

            if failures:
                details = "; ".join(
                    (
                        f"{file}: missing={info['missing']}, "
                        f"extra={info['extra']}, "
                        f"expected={info['expected']}, "
                        f"actual={info['actual']}"
                    )
                    for file, info in failures.items()
                )

                return ValidationResult(
                    "COLUMN_ORDER_CHECK",
                    "FAIL",
                    f"Column order validation failed: {details}"
                )

            return ValidationResult(
                "COLUMN_ORDER_CHECK",
                "PASS",
                "All files match expected column order (case-insensitive)."
            )

        except Exception as e:
            return ValidationResult(
                "COLUMN_ORDER_CHECK",
                "FAIL",
                f"Unable to validate column order: {str(e)}"
            )

    def check_column_count(self, file_dfs: dict,  min_count: int = None, max_count: int = None ) -> ValidationResult:
        
        """
        Validates that the number of columns in each DataFrame
        falls within the specified minimum and maximum bounds.

        The check enforces column count limits only and does not
        consider column names or order.

        PASS if ALL files:
        - Have a column count >= min_count
        - And have a column count <= max_count

        FAIL if ANY file:
        - Has fewer columns than min_count
        - Or has more columns than max_count
        """

        try:
            for name, val in {
                "min_count": min_count,
                "max_count": max_count
            }.items():
                if val is None or not isinstance(val, int):
                    return ValidationResult(
                        "COLUMN_COUNT_CHECK",
                        "FAIL",
                        f"{name} must be an integer, got {type(val).__name__}"
                    )

            if min_count < 0 or max_count < min_count:
                return ValidationResult(
                "COLUMN_COUNT_CHECK",
                "FAIL",
                "Invalid length bounds"
                )

            failures = {}

            for file_path, df in file_dfs.items():
                col_count = len(df.columns)
                errors = []

                if min_count is not None and col_count < min_count:
                    errors.append(f"below min_count={min_count}")

                if max_count is not None and col_count > max_count:
                    errors.append(f"above max_count={max_count}")

                if errors:
                    failures[file_path] = errors

            if failures:
                details = "; ".join(
                    f"{file}: {errs}" for file, errs in failures.items()
                )

                return ValidationResult(
                    "COLUMN_COUNT_CHECK",
                    "FAIL",
                    f"Column count validation failed: {details}"
                )

            return ValidationResult(
                "COLUMN_COUNT_CHECK",
                "PASS",
                "Column count validation passed for all files."
            )

        except Exception as e:
            return ValidationResult(
                "COLUMN_COUNT_CHECK",
                "FAIL",
                f"Unable to validate column count: {str(e)}"
            )

    def check_null(self, file_dfs: dict, non_null_cols: list ) -> ValidationResult:
        
        """
        Validates that specified columns do not contain NULL values.

        The check is case-insensitive and ensures that each column listed
        in non_null_cols exists in the DataFrame and has zero NULL values.

        PASS if ALL files:
        - Contain all columns listed in non_null_cols
        - And none of those columns contain NULL values

        FAIL if ANY file:
        - Is missing a column listed in non_null_cols
        - Or contains one or more NULL values in those columns
        """

        try:
            error = self._validate_col_list( non_null_cols, 'NULL_CHECK' )
            if error: return error

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
                        
                        null_count = df.select(count(when(col(actual_col).isNull(), 1)).alias("nulls") ).collect()[0]["nulls"]

                        if null_count > 0:
                            violated[actual_col] = f"{null_count} NULL values"

                if violated:
                    failures[file_path] = violated

            if failures:
                details = "; ".join(
                    f"{file}: {info}" for file, info in failures.items()
                )

                return ValidationResult(
                    "NULL_CHECK",
                    "FAIL",
                    f"NOT NULL constraint violated: {details}"
                )

            return ValidationResult(
                "NULL_CHECK",
                "PASS",
                "All NOT NULL constraints satisfied (case-insensitive)."
            )
        
        except Exception as e:
            display(e)
            return ValidationResult(
                "NULL_CHECK",
                "FAIL",
                f"Unable to validate nullable constraints: {str(e)}"
            )

    def check_column_sensitivity( self, file_dfs: dict, cols: list ) -> ValidationResult:
        
        """
        Validates the presence of specified columns with exact case sensitivity.

        This check enforces strict, case-sensitive matching of column names
        against the provided list. Unlike other column checks, no case
        normalization is applied.

        PASS if ALL files:
        - Contain every column listed in cols with the exact same casing

        FAIL if ANY file:
        - Is missing one or more columns due to case mismatch
        - Or does not contain a column with the exact expected name
        """

        try:
            error = self._validate_col_list(cols, 'COLUMN_SENSITIVITY')
            if error: return error

            failures = {}

            for file_path, df in file_dfs.items():
                actual_cols = set(df.columns)  # case-sensitive
                missing = [col for col in cols if col not in actual_cols]

                if missing:
                    failures[file_path] = {
                        "missing_case_sensitive": missing,
                        "available_columns": list(df.columns)
                    }

            if failures:
                details = "; ".join(
                    (
                        f"{file}: missing={info['missing_case_sensitive']} "
                        f"(available={info['available_columns']})"
                    )
                    for file, info in failures.items()
                )

                return ValidationResult(
                    "COLUMN_SENSITIVITY",
                    "FAIL",
                    f"Case-sensitive sensitive column validation failed: {details}"
                )

            return ValidationResult(
                "COLUMN_SENSITIVITY",
                "PASS",
                f"All sensitive columns present with exact case: {cols}"
            )

        except Exception as e:
            return ValidationResult(
                "COLUMN_SENSITIVITY",
                "FAIL",
                f"Unable to validate column sensitivity: {str(e)}"
            )

    def check_file_header_presence(self, file_dfs):
        
        """
        Validates that input files contain a proper header row.

        This check ensures that each DataFrame/File
        - Has at least one column
        - Does not rely on Spark auto-generated column names
        (e.g. _c0, _c1, ...), which indicate a missing header

        PASS if ALL files:
        - Contain one or more columns
        - And do not have only auto-generated Spark column names

        FAIL if ANY file:
        - Has no columns (empty or malformed file)
        - Or has missing header row detected via auto-generated columns
        """

        failures = {}

        for file_path, df in file_dfs.items():

            # 1No columns at all
            if not df.columns or len(df.columns) == 0:
                failures[file_path] = "No columns found (empty or malformed file)"
                continue

            # Spark auto-generated columns (_c0, _c1...)
            auto_generated = all(c.lower().startswith("_c") for c in df.columns)
            if auto_generated:
                failures[file_path] = "Header row missing (auto-generated columns detected)"
                continue

        if failures:
            return ValidationResult(
                "FILE_HEADER_PRESENCE_CHECK",
                "FAIL",
                "; ".join(f"{f}: {r}" for f, r in failures.items())
            )

        return ValidationResult(
            "FILE_HEADER_PRESENCE_CHECK",
            "PASS",
            "File headers are present and valid"
        )


# COMMAND ----------

# DBTITLE 1,DataChecks
class DataChecks:
    def __init__(self, spark):
        self.spark = spark

    @staticmethod
    def _validate_col_list(required_cols , check_name):
            # Validate required_cols type
            if not isinstance(required_cols, list):
                return ValidationResult(
                    f"{check_name}",
                    "FAIL",
                    f"must be a list , got {type(required_cols).__name__}"
                )

            # Validate required_cols not empty
            if not required_cols:
                return ValidationResult(
                    f"{check_name}",
                    "FAIL",
                    "list cannot be empty"
                )

            # Validate all required_cols are strings
            non_string_cols = [c for c in required_cols if not isinstance(c, str)]
            if non_string_cols:
                return ValidationResult(
                    f"{check_name}",
                    "FAIL",
                    f"All values must be strings. Invalid entries: {non_string_cols}"
                )

            # Validate no duplicates (case-insensitive)
            lowered = [c.lower() for c in required_cols]
            if len(lowered) != len(set(lowered)):
                return ValidationResult(
                    f"{check_name}",
                    "FAIL",
                    f"Duplicate valuea found in cols list (case-insensitive): {required_cols}"
                )

            return None

    def check_invalid_values( self, file_dfs: dict, column: str, invalid_values: list, case_insensitive: bool = True) -> ValidationResult:
        try:
            
            """
            Validates that a column does not contain any disallowed (invalid) values.

            The check verifies values in the specified column against a provided
            list of invalid values. Comparison can be performed in a
            case-insensitive or case-sensitive manner.

            PASS if ALL files:
            - Contain the specified column
            - And do not contain any values listed in invalid_values

            FAIL if ANY file:
            - Is missing the specified column
            - Or contains one or more invalid values in that column
            """

            error = self._validate_col_list(invalid_values, 'INVALID_VALUES_CHECK')
            if error: return error

            if not isinstance(case_insensitive, bool):
                return ValidationResult(
                    "INVALID_VALUES_CHECK",
                    "FAIL",
                    "case_insensitive must be boolean"
                )

            if not isinstance(column, str):
                return ValidationResult(
                    "INVALID_VALUES_CHECK",
                    "FAIL",
                    "column must be string"
                )

            invalid_set = set(
                v.lower() if case_insensitive and isinstance(v, str) else v
                for v in invalid_values
            )

            failures = {}

            for file_path, df in file_dfs.items():
                df_cols = {c.lower(): c for c in df.columns}
                if column.lower() not in df_cols:
                    failures[file_path] = f"Column '{column}' not found"
                    continue

                actual_col = df_cols[column.lower()]
                val_col = lower(col(actual_col)) if case_insensitive else col(actual_col)

                invalid_count = df.filter(val_col.isin(invalid_set)).limit(1).count()

                if invalid_count > 0:
                    failures[file_path] = (
                        f"Invalid values found in column '{actual_col}'. "
                        f"Banned: {invalid_values}"
                    )

            if failures:
                return ValidationResult(
                    "INVALID_VALUES_CHECK",
                    "FAIL",
                    "; ".join(f"{k}: {v}" for k, v in failures.items())
                )

            return ValidationResult(
                "INVALID_VALUES_CHECK",
                "PASS",
                f"No invalid values found in '{column}'"
            )

        except Exception as e:
            return ValidationResult(
                "INVALID_VALUES_CHECK",
                "FAIL",
                f"Unable to validate invalid values: {str(e)}"
            )

    def check_expected_values( self, file_dfs: dict, column: str, expected_values: list, case_insensitive: bool = True ) -> ValidationResult:
        
        """
        Validates that a specified column in multiple DataFrames contains
        only values from a given list of expected values.

        This check iterates over each file/DataFrame pair and verifies:
        - The target column exists
        - All values in the column belong to the allowed expected values list
        - Comparison can be performed in a case-insensitive manner (optional)

        Parameters:
            file_dfs (dict):
                Mapping of file paths (or identifiers) to Spark DataFrames.
            column (str):
                Name of the column to validate.
            expected_values (list):
                List of allowed values for the column.
            case_insensitive (bool, optional):
                Whether string comparisons should ignore case (default: True).

        Returns:
            ValidationResult:
                PASS  - If all files contain only expected values
                FAIL  - If validation fails due to:
                        • Invalid input types
                        • Missing column(s)
                        • Unexpected values
                        • Runtime exceptions
        """

        try:
           
            if not isinstance(case_insensitive, bool):
                return ValidationResult(
                    "EXPECTED_VALUES_CHECK",
                    "FAIL",
                    "case_insensitive must be boolean"
                )
            
            if not isinstance(column, str):
                return ValidationResult(
                    "EXPECTED_VALUES_CHECK",
                    "FAIL",
                    "column must be string"
                )

            error = self._validate_col_list(expected_values, 'EXPECTED_VALUES_CHECK')
            if error: return error

            expected_set = set(
                v.lower() if case_insensitive and isinstance(v, str) else v
                for v in expected_values
            )

            failures = {}

            for file_path, df in file_dfs.items():
                df_cols = {c.lower(): c for c in df.columns}
                if column.lower() not in df_cols:
                    failures[file_path] = f"Column '{column}' not found"
                    continue

                actual_col = df_cols[column.lower()]
                val_col = lower(col(actual_col)) if case_insensitive else col(actual_col)

                invalid_cond = ~val_col.isin(expected_set)

                invalid_count = df.filter(invalid_cond).limit(1).count()
                if invalid_count > 0:
                    failures[file_path] = (
                        f"Unexpected values in column '{actual_col}'. "
                        f"Allowed: {expected_values}"
                    )

            if failures:
                return ValidationResult(
                    "EXPECTED_VALUES_CHECK",
                    "FAIL",
                    "; ".join(f"{k}: {v}" for k, v in failures.items())
                )

            return ValidationResult(
                "EXPECTED_VALUES_CHECK",
                "PASS",
                f"All files contain only expected values in '{column}'"
            )

        except Exception as e:
            return ValidationResult(
                "EXPECTED_VALUES_CHECK",
                "FAIL",
                f"Unable to validate expected values: {str(e)}"
            )

    def check_date_format(self, file_dfs: dict, column: str, date_format: str) -> ValidationResult:
        
        """
        Validates that a specified column in multiple DataFrames conforms
        to a given date format.

        This check iterates over each file/DataFrame pair and verifies:
        - The target column exists (case-insensitive match)
        - Invalid or unparsable date values are detected

        Parameters:

            column (str):
                Name of the column containing date values to validate.
            date_format (str):
                Expected date format (Spark-compatible format string).

        Returns:
            ValidationResult:
                PASS  - If all files contain valid dates in the expected format
                FAIL  - If validation fails due to:
                        • Invalid input arguments
                        • Missing column(s)
                        • Date parsing failures
                        • Runtime exceptions
        """

        try:
            # error = self._validate_col(column, 'DATE_FORMAT_CHECK')
            # if error: return error

            if not isinstance(date_format, str) or not date_format.strip():
                return ValidationResult(
                    "DATE_FORMAT_CHECK",
                    "FAIL",
                    "date_format must be a non-empty string"
                )

            if not isinstance(column, str):
                return ValidationResult(
                    "DATE_FORMAT_CHECK",
                    "FAIL",
                    "column must be string"
                )

            failures = {}

            for file_path, df in file_dfs.items():
                # Case-insensitive column resolution
                df_cols = {c.lower(): c for c in df.columns}
                if column.lower() not in df_cols:
                    failures[file_path] = f"Column '{column}' not found"
                    continue

                actual_col = df_cols[column.lower()]

                parsed_df = df.withColumn(
                    "__parsed_date",
                    to_date(col(actual_col), date_format)
                )

                invalid_cond = col("__parsed_date").isNull()
                invalid_cond &= col(actual_col).isNotNull()

                invalid_count = parsed_df.filter(invalid_cond).limit(1).count()

                if invalid_count > 0:
                    failures[file_path] = (
                        f"Invalid date format in column '{actual_col}', "
                        f"expected format: {date_format}"
                    )

            if failures:
                return ValidationResult(
                    "DATE_FORMAT_CHECK",
                    "FAIL",
                    "; ".join(f"{k}: {v}" for k, v in failures.items())
                )

            return ValidationResult(
                "DATE_FORMAT_CHECK",
                "PASS",
                f"All files have valid '{column}' dates in format {date_format}"
            )

        except Exception as e:
            return ValidationResult(
                "DATE_FORMAT_CHECK",
                "FAIL",
                f"Unable to validate date format: {str(e)}"
            )

    def check_no_negative_values( self, file_dfs: dict, column: str ) -> ValidationResult:
        
        """
        Validates that a specified column in multiple DataFrames contains
        only non-negative numeric values.

        This check iterates over each file/DataFrame pair and verifies:
        - The target column exists (case-insensitive match)
        - All non-null values in the column are numeric
        - No negative values are present in the column

        Parameters:
            column (str):
                Name of the column to validate for non-negative values.

        Returns:
            ValidationResult:
                PASS  - If all files contain only non-negative numeric values
                FAIL  - If validation fails due to:
                        • Invalid column argument
                        • Missing column(s)
                        • Non-numeric values
                        • Negative values
                        • Runtime exceptions
        """

        try:
            if not isinstance(column, str):
                return ValidationResult(
                    "NO_NEGATIVE_VALUES_CHECK",
                    "FAIL",
                    "column must be string"
                )

            failures = {}

            for file_path, df in file_dfs.items():
                df_cols = {c.lower(): c for c in df.columns}

                if column.lower() not in df_cols:
                    failures[file_path] = f"Column '{column}' not found"
                    continue

                actual_col = df_cols[column.lower()]
                
                non_numeric_found = (
                    df.filter(
                        col(actual_col).isNotNull() &
                        ~col(actual_col).rlike(r'^-?\d+$')
                    )
                    .limit(1)
                    .count() > 0
                )

                if non_numeric_found:
                    failures[file_path] = "Non-numeric values found"
                    continue


                neg_found = df.filter(col(actual_col) < 0).limit(1).count() > 0
                if neg_found:
                    failures[file_path] = "Negative values found"


            if failures:
                details = "; ".join(
                    f"{file}: {cols}" for file, cols in failures.items()
                )
                return ValidationResult(
                    "NO_NEGATIVE_VALUES_CHECK",
                    "FAIL",
                    f"Negative value check failed: {details}"
                )

            return ValidationResult(
                "NO_NEGATIVE_VALUES_CHECK",
                "PASS",
                f"No negative values found in columns {column}"
            )

        except Exception as e:
            return ValidationResult(
                "NO_NEGATIVE_VALUES_CHECK",
                "FAIL",
                f"Unable to validate negative values: {str(e)}"
            )

    def check_no_whitespace( self, file_dfs: dict, column: str ) -> ValidationResult:
        
        """
        Validates that a specified string column in multiple DataFrames
        does not contain any whitespace characters.

        This check iterates over each file/DataFrame pair and verifies:
        - The target column exists (case-insensitive match)
        - The column data type is a string
        - No non-null values in the column contain whitespace characters
        (spaces, tabs, newlines, etc.)

        Parameters:
            column (str):
                Name of the string column to validate.

        Returns:
            ValidationResult:
                PASS  - If all files have no whitespace in the specified column
                FAIL  - If validation fails due to:
                        • Invalid column argument
                        • Missing column(s)
                        • Non-string column types
                        • Presence of whitespace
                        • Runtime exceptions
        """

        try:
            if not isinstance(column, str):
                return ValidationResult(
                    "NO_WHITESPACE_CHECK",
                    "FAIL",
                    "column must be string"
                )

            failures = {}

            for file_path, df in file_dfs.items():
                df_cols = {c.lower(): c for c in df.columns}


                if column.lower() not in df_cols:
                    failures[file_path] = "Column not found"
                    continue

                actual_col = df_cols[column.lower()]
                dtype = df.schema[actual_col].dataType

                if not isinstance(dtype, StringType):
                    failures[file_path] = "Non-string column"
                    continue

                whitespace_found = df.filter(
                    col(actual_col).isNotNull() &
                    col(actual_col).rlike(r"\s")
                ).limit(1).count() > 0

                if whitespace_found:
                    failures[file_path] = "Whitespace detected"


            if failures:
                return ValidationResult(
                    "NO_WHITESPACE_CHECK",
                    "FAIL",
                    "; ".join(f"{k}: {v}" for k, v in failures.items())
                )

            return ValidationResult(
                "NO_WHITESPACE_CHECK",
                "PASS",
                f"No whitespace found in columns {column}"
            )

        except Exception as e:
            return ValidationResult(
                "NO_WHITESPACE_CHECK",
                "FAIL",
                f"Unable to validate whitespace: {str(e)}"
            )

    def check_string_length( self, file_dfs: dict,  column: str, min_len: int,  max_len: int ) -> ValidationResult:
        
        """
        Validates that a specified string column in multiple DataFrames
        has values whose trimmed length falls within configured bounds.

        This check iterates over each file/DataFrame pair and verifies:
        - The target column exists (case-insensitive match)
        - The column data type is a string
        - All non-null values, after trimming whitespace, have lengths
        within the inclusive range [min_len, max_len]

        Parameters:
            column (str):
                Name of the string column to validate.
            min_len (int):
                Minimum allowed length (inclusive).
            max_len (int):
                Maximum allowed length (inclusive).

        Returns:
            ValidationResult:
                PASS  - If all files satisfy the string length constraints
                FAIL  - If validation fails due to:
                        • Invalid input arguments
                        • Missing column(s)
                        • Non-string column types
                        • String length violations
                        • Runtime exceptions
        """

        try:
            if not isinstance(column, str):
                return ValidationResult(
                    "STRING_LENGTH_CHECK",
                    "FAIL",
                    "column must be string"
                )


            if not isinstance(min_len, int) or not isinstance(max_len, int):
                return ValidationResult(
                    "STRING_LENGTH_CHECK",
                    "FAIL",
                    "min_len and max_len must be integers"
                )

            if min_len < 0 or max_len < min_len:
                return ValidationResult(
                    "STRING_LENGTH_CHECK",
                    "FAIL",
                    "Invalid length bounds"
                )

            failures = {}

            for file_path, df in file_dfs.items():
                df_cols = {c.lower(): c for c in df.columns}
                if column.lower() not in df_cols:
                    failures[file_path] = "Column not found"
                    continue

                actual_col = df_cols[column.lower()]
                print(actual_col)
                dtype = df.schema[actual_col].dataType
                print(dtype)

                if not isinstance(dtype, StringType):
                    failures[file_path] = "Non-string column"
                    continue

                invalid_found = df.filter(
                    col(actual_col).isNotNull() &
                    (
                        (length(trim(col(actual_col))) < min_len) |
                        (length(trim(col(actual_col))) > max_len)
                    )
                ).limit(1).count() > 0

                if invalid_found:
                    failures[file_path] = (
                        f"String length out of bounds [{min_len}, {max_len}]"
                    )

            if failures:
                return ValidationResult(
                    "STRING_LENGTH_CHECK",
                    "FAIL",
                    "; ".join(f"{k}: {v}" for k, v in failures.items())
                )

            return ValidationResult(
                "STRING_LENGTH_CHECK",
                "PASS",
                f"All values in '{column}' meet length constraints [{min_len}, {max_len}]"
            )

        except Exception as e:
            return ValidationResult(
                "STRING_LENGTH_CHECK",
                "FAIL",
                f"Unable to validate string length: {str(e)}"
            )

    def check_row_count( self, file_dfs: dict, min_count: int = None,  max_count: int  = None) -> ValidationResult:

        """
        Validates that each DataFrame contains a number of rows within
        specified minimum and/or maximum bounds.

        This check iterates over each file/DataFrame pair and verifies:
        - Row count is greater than or equal to `min_count` (if provided)
        - Row count is less than or equal to `max_count` (if provided)
        - Bounds are logically valid

        Parameters:

            min_count (int, optional):
                Minimum allowed number of rows (inclusive).
            max_count (int, optional):
                Maximum allowed number of rows (inclusive).

        Returns:
            ValidationResult:
                PASS  - If all files have row counts within the expected limits
                FAIL  - If validation fails due to:
                        • Invalid input arguments
                        • Row count below minimum or above maximum
                        • Runtime exceptions
        """

        try:
            if not isinstance(min_count, int) or not isinstance(max_count, int):
                return ValidationResult(
                    "ROW_COUNT_CHECK",
                    "FAIL",
                    "min_count and max_count must be integers"
                )

            if min_count < 0 or max_count < min_count:
                return ValidationResult(
                    "ROW_COUNT_CHECK",
                    "FAIL",
                    "Invalid length bounds"
                )
            
            failures = {}

            for file_path, df in file_dfs.items():
                row_count = df.count()


                if min_count is not None and row_count < min_count:
                    failures[file_path] = (
                        f"Row count {row_count} < minimum {min_count}"
                    )

                if max_count is not None and row_count > max_count:
                    failures[file_path] = (
                        f"Row count {row_count} > maximum {max_count}"
                    )

            if failures:
                return ValidationResult(
                    "ROW_COUNT_CHECK",
                    "FAIL",
                    "; ".join(f"{f}: {r}" for f, r in failures.items())
                )

            return ValidationResult(
                "ROW_COUNT_CHECK",
                "PASS",
                "Row count within expected limits"
            )

        except Exception as e:
            return ValidationResult(
                "ROW_COUNT_CHECK",
                "FAIL",
                f"Unable to validate row count: {str(e)}"
            )

    def check_completeness_ratio( self, file_dfs: dict, threshold_pct: float ) -> ValidationResult:
        
        """
        Validates that the overall data completeness of each DataFrame meets
        or exceeds a specified percentage threshold.

        Completeness is calculated as:
            (Number of non-null cells / Total number of cells) * 100

        This check iterates over each file/DataFrame pair and verifies:
        - The provided threshold is a valid percentage (0–100)
        - The DataFrame is non-empty
        - The calculated completeness ratio meets or exceeds the threshold

        Parameters:
            threshold_pct (float):
                Minimum required completeness percentage (0–100).

        Returns:
            ValidationResult:
                PASS  - If all files meet or exceed the completeness threshold
                FAIL  - If validation fails due to:
                        • Invalid threshold value
                        • Empty datasets
                        • Completeness below threshold
                        • Runtime exceptions
        """

        try:
            # ---------- PARAMETER VALIDATION ----------
            if not isinstance(threshold_pct, (int, float)):
                return ValidationResult(
                    "COMPLETENESS_RATIO_CHECK",
                    "FAIL",
                    "threshold_pct must be a number"
                )

            if threshold_pct < 0 or threshold_pct > 100:
                return ValidationResult(
                    "COMPLETENESS_RATIO_CHECK",
                    "FAIL",
                    "threshold_pct must be between 0 and 100"
                )

            # ---------- COMPLETENESS CHECK ----------
            failures = {}

            for file_path, df in file_dfs.items():
                total_rows = df.count()
                total_cols = len(df.columns)

                if total_rows == 0 or total_cols == 0:
                    failures[file_path] = "Empty dataset; completeness undefined"
                    continue

                total_cells = total_rows * total_cols

                non_null_exprs = [
                    count(col(c)).alias(c) for c in df.columns
                ]

                non_null_counts = (
                    df.select(*non_null_exprs)
                    .collect()[0]
                    .asDict()
                )


                non_null_cells = builtins.sum(non_null_counts.values())
                completeness_pct = (non_null_cells / total_cells) * 100

                if completeness_pct < threshold_pct:
                    failures[file_path] = (
                        f"Completeness {completeness_pct:.2f}% "
                        f"< threshold {threshold_pct}%"
                    )

            if failures:
                return ValidationResult(
                    "COMPLETENESS_RATIO_CHECK",
                    "FAIL",
                    "; ".join(f"{f}: {r}" for f, r in failures.items())
                )

            return ValidationResult(
                "COMPLETENESS_RATIO_CHECK",
                "PASS",
                "Completeness ratio meets the threshold"
            )

        except Exception as e:
            return ValidationResult(
                "COMPLETENESS_RATIO_CHECK",
                "FAIL",
                f"Unable to validate completeness ratio: {str(e)}"
            )

    def check_unique(self, file_dfs: dict, key_columns: list  ) -> ValidationResult:
        
        """
        Validates that the specified key columns uniquely identify each row
        in multiple DataFrames.

        This check iterates over each file/DataFrame pair and verifies:
        - The list of key columns is valid and non-empty
        - All key columns exist in the DataFrame (case-insensitive match)
        - The combination of key column values is unique for every row

        Parameters:

            key_columns (list):
                List of column names that together form a uniqueness key.

        Returns:
            ValidationResult:
                PASS  - If all files contain unique key combinations
                FAIL  - If validation fails due to:
                        • Invalid key column list
                        • Missing key columns
                        • Duplicate key combinations
                        • Runtime exceptions
        """

        try:
            error = self._validate_col_list(key_columns, 'UNIQUE_CHECK')
            if error: return error

            # ---------- UNIQUENESS CHECK ----------
            failures = {}
            key_cols_norm = {c.lower(): c for c in key_columns}

            for file_path, df in file_dfs.items():
                df_cols_norm = {c.lower(): c for c in df.columns}

                missing = [
                    orig for lc, orig in key_cols_norm.items()
                    if lc not in df_cols_norm
                ]

                if missing:
                    failures[file_path] = f"Missing key columns: {missing}"
                    continue

                actual_keys = [df_cols_norm[c.lower()] for c in key_columns]

                total = df.count()
                distinct = df.select(*actual_keys).distinct().count()

                if distinct < total:
                    failures[file_path] = (
                        f"Duplicate keys found: {total - distinct}"
                    )

            if failures:
                return ValidationResult(
                    "UNIQUE_CHECK",
                    "FAIL",
                    "; ".join(f"{f}: {r}" for f, r in failures.items())
                )

            return ValidationResult(
                "UNIQUE_CHECK",
                "PASS",
                "All key columns are unique"
            )

        except Exception as e:
            return ValidationResult(
                "UNIQUE_CHECK",
                "FAIL",
                f"Unable to validate uniqueness: {str(e)}"
            )

    def check_kpi_aggregate( self, file_dfs: dict, expr: str, min_val, max_val ) -> ValidationResult:
        
        """
        Validates that the result of a KPI aggregate expression falls within
        a specified numeric range for multiple DataFrames.

        This check iterates over each file/DataFrame pair and verifies:
        - The aggregate expression is a valid, non-empty Spark SQL expression
        - The expression can be successfully evaluated on the DataFrame
        - The resulting aggregate value is non-null
        - The aggregate value lies within the inclusive range [min_val, max_val]

        Parameters:

            expr (str):
                Spark SQL aggregate expression to evaluate (e.g. "sum(revenue)",
                "avg(score)", "count(*)").
            min_val (int | float):
                Minimum allowed value for the aggregate result (inclusive).
            max_val (int | float):
                Maximum allowed value for the aggregate result (inclusive).

        Returns:
            ValidationResult:
                PASS  - If all files produce an aggregate value within the expected range
                FAIL  - If validation fails due to:
                        • Invalid expression or parameters
                        • Expression evaluation errors
                        • NULL aggregate results
                        • Aggregate value outside the allowed range
                        • Runtime exceptions
        """

        try:
            # ---------- PARAMETER VALIDATION ----------
            if not isinstance(expr, str) or not expr.strip():
                return ValidationResult(
                    "KPI_AGGREGATE_CHECK",
                    "FAIL",
                    "expr must be a non-empty string"
                )

            if not all(isinstance(v, (int, float)) for v in [min_val, max_val]):
                return ValidationResult(
                    "KPI_AGGREGATE_CHECK",
                    "FAIL",
                    "min_val and max_val must be numeric"
                )

            if max_val < min_val:
                return ValidationResult(
                    "KPI_AGGREGATE_CHECK",
                    "FAIL",
                    "max_val must be >= min_val"
                )

            failures = {}

            for file_path, df in file_dfs.items():
                try:
                    agg_value = (
                        df.select(spark_expr(expr.lower()).alias("kpi_value"))
                        .collect()[0]["kpi_value"]
                    )
                except Exception as ex:
                    failures[file_path] = f"Invalid expression: {expr}"
                    continue

                if agg_value is None:
                    failures[file_path] = "Aggregate value is NULL"
                    continue

                if not (min_val <= agg_value <= max_val):
                    failures[file_path] = (
                        f"Aggregate value {agg_value} "
                        f"not in range [{min_val}, {max_val}]"
                    )

            if failures:
                return ValidationResult(
                    "KPI_AGGREGATE_CHECK",
                    "FAIL",
                    "; ".join(f"{f}: {r}" for f, r in failures.items())
                )

            return ValidationResult(
                "KPI_AGGREGATE_CHECK",
                "PASS",
                "KPI aggregate expression within expected range"
            )

        except Exception as e:
            return ValidationResult(
                "KPI_AGGREGATE_CHECK",
                "FAIL",
                f"Unable to validate KPI aggregate: {str(e)}"
            )

    def check_value_range(self, file_dfs: dict, column_name: str,  min_val,  max_val ) -> ValidationResult:
        
        """
        Validates that all values in a specified column fall within a
        defined numeric range across multiple DataFrames.

        This check iterates over each file/DataFrame pair and verifies:
        - The target column exists (case-insensitive match)
        - All non-null values in the column are numeric
        - All numeric values fall within the inclusive range [min_val, max_val]

        Parameters:

            column_name (str):
                Name of the column to validate.
            min_val (int | float):
                Minimum allowed value for the column (inclusive).
            max_val (int | float):
                Maximum allowed value for the column (inclusive).

        Returns:
            ValidationResult:
                PASS  - If all files contain only numeric values within the range
                FAIL  - If validation fails due to:
                        • Invalid input parameters
                        • Missing column(s)
                        • Presence of non-numeric values
                        • Values outside the allowed range
                        • Runtime exceptions
        """

        try:
            # ---------- PARAMETER VALIDATION ----------
            if not isinstance(column_name, str):
                return ValidationResult(
                    "VALUE_RANGE_CHECK",
                    "FAIL",
                    "column_name must be a string"
                )

            if not all(isinstance(v, (int, float)) for v in [min_val, max_val]):
                return ValidationResult(
                    "VALUE_RANGE_CHECK",
                    "FAIL",
                    "min_val and max_val must be numeric"
                )

            if max_val < min_val:
                return ValidationResult(
                    "VALUE_RANGE_CHECK",
                    "FAIL",
                    "max_val must be >= min_val"
                )

            failures = {}

            for file_path, df in file_dfs.items():
                df_cols = {c.lower(): c for c in df.columns}

                if column_name.lower() not in df_cols:
                    failures[file_path] = f"Missing column: {column_name}"
                    continue


                actual_col = df_cols[column_name.lower()]
                cast_col = col(actual_col).cast("double")

                # ---------- 1️⃣ Non-numeric detection ----------
                non_numeric_count = df.filter(
                    col(actual_col).isNotNull() & cast_col.isNull()
                ).count()

                if non_numeric_count > 0:
                    failures[file_path] = (
                        f"{non_numeric_count} non-numeric values found"
                    )
                    continue

                # ---------- 2️⃣ Range validation ----------
                out_of_range_count = df.filter(
                    cast_col.isNotNull() &
                    ((cast_col < min_val) | (cast_col > max_val))
                ).count()

                if out_of_range_count > 0:
                    failures[file_path] = (
                        f"{out_of_range_count} values outside range "
                        f"[{min_val}, {max_val}]"
                    )


            if failures:
                return ValidationResult(
                    "VALUE_RANGE_CHECK",
                    "FAIL",
                    "; ".join(f"{f}: {r}" for f, r in failures.items())
                )

            return ValidationResult(
                "VALUE_RANGE_CHECK",
                "PASS",
                "All values within expected range"
            )

        except Exception as e:
            return ValidationResult(
                "VALUE_RANGE_CHECK",
                "FAIL",
                f"Unable to validate value range: {str(e)}"
            )

    def check_regex_format(self, file_dfs: dict, column_name: str, pattern: str ) -> ValidationResult:
        
        """
        Validates that values in a specified column conform to a given
        regular expression pattern across multiple DataFrames.

        This check iterates over each file/DataFrame pair and verifies:
        - The target column exists (case-insensitive match)
        - The provided regex pattern is valid and non-empty
        - All relevant non-null, non-literal-'null' values in the column
        match the supplied regular expression after trimming whitespace

        Parameters:
            column_name (str):
                Name of the column to validate.
            pattern (str):
                Regular expression pattern that column values must match.

        Returns:
            ValidationResult:
                PASS  - If all files contain values matching the regex pattern
                FAIL  - If validation fails due to:
                        • Invalid input parameters
                        • Missing column(s)
                        • Values that do not match the regex pattern
                        • Runtime exceptions
        """

        try:
            # ---------- PARAMETER VALIDATION ----------
            if not isinstance(column_name, str):
                return ValidationResult(
                    "REGEX_FORMAT_CHECK",
                    "FAIL",
                    "column_name must be a string"
                )

            if not isinstance(pattern, str) or not pattern:
                return ValidationResult(
                    "REGEX_FORMAT_CHECK",
                    "FAIL",
                    "pattern must be a non-empty string"
                )

            failures = {}

            for file_path, df in file_dfs.items():
                df_cols = {c.lower(): c for c in df.columns}

                if column_name.lower() not in df_cols:
                    failures[file_path] = f"Missing column: {column_name}"
                    continue

                actual_col = df_cols[column_name.lower()]


                invalid_count = df.filter(
                    col(actual_col).isNotNull() &
                    (lower(trim(col(actual_col))) != "null") &
                    (~trim(col(actual_col)).rlike(
                        pattern
                    ))
                ).count()

                if invalid_count > 0:
                    failures[file_path] = (
                        f"{invalid_count} values do not match regex"
                    )

            if failures:
                return ValidationResult(
                    "REGEX_FORMAT_CHECK",
                    "FAIL",
                    "; ".join(f"{f}: {r}" for f, r in failures.items())
                )

            return ValidationResult(
                "REGEX_FORMAT_CHECK",
                "PASS",
                "All values match the expected format"
            )

        except Exception as e:
            return ValidationResult(
                "REGEX_FORMAT_CHECK",
                "FAIL",
                f"Unable to validate regex format: {str(e)}"
            )

    def check_no_future_date( self, file_dfs: dict,  date_column: str,  date_format: str ) -> ValidationResult:
        
        """
        Validates that a specified date column does not contain any future
        dates across multiple DataFrames.

        This check iterates over each file/DataFrame pair and verifies:
        - The target date column exists (case-insensitive match)
        - Date values conform to the specified date format
        - All valid dates are less than or equal to the current system date
        - Invalid or unparsable date values are detected separately

        Parameters:

            date_column (str):
                Name of the column containing date values to validate.
            date_format (str):
                Expected date format for parsing the column values
                (Spark-compatible format string).

        Returns:
            ValidationResult:
                PASS  - If all files contain no future dates
                FAIL  - If validation fails due to:
                        • Invalid input parameters
                        • Missing column(s)
                        • Invalid date formats
                        • Presence of future dates
                        • Runtime exceptions
        """

        try:
            # ---------- PARAMETER VALIDATION ----------
            if not isinstance(date_column, str):
                return ValidationResult(
                    "NO_FUTURE_DATE_CHECK",
                    "FAIL",
                    "date_column must be a string"
                )

            if not isinstance(date_format, str) or not date_format:
                return ValidationResult(
                    "NO_FUTURE_DATE_CHECK",
                    "FAIL",
                    "date_format must be a non-empty string"
                )

            failures = {}

            for file_path, df in file_dfs.items():
                df_cols = {c.lower(): c for c in df.columns}

                if date_column.lower() not in df_cols:
                    failures[file_path] = f"Missing column: {date_column}"
                    continue

                actual_col = df_cols[date_column.lower()]

                parsed_date = to_date(
                    trim(col(actual_col)),
                    date_format
                )

                invalid_format_count = df.filter(
                    col(actual_col).isNotNull() &
                    (lower(trim(col(actual_col))) != "null") &
                    parsed_date.isNull()
                ).count()

                if invalid_format_count > 0:
                    failures[file_path] = (
                        f"{invalid_format_count} invalid date values"
                    )
                    continue

                # ❌ future dates
                future_date_count = df.filter(
                    parsed_date > current_date()
                ).count()

                if future_date_count > 0:
                    failures[file_path] = (
                        f"{future_date_count} future date values found"
                    )

            if failures:
                return ValidationResult(
                    "NO_FUTURE_DATE_CHECK",
                    "FAIL",
                    "; ".join(f"{f}: {r}" for f, r in failures.items())
                )

            return ValidationResult(
                "NO_FUTURE_DATE_CHECK",
                "PASS",
                "No future dates detected"
            )

        except Exception as e:
            return ValidationResult(
                "NO_FUTURE_DATE_CHECK",
                "FAIL",
                f"Unable to validate future dates: {str(e)}"
            )

    def check_duplicate_rows( self, file_dfs: dict ) -> ValidationResult:
        
        """
        Validates that each DataFrame does not contain duplicate rows.

        This check iterates over each file/DataFrame pair and verifies:
        - The total number of rows equals the number of distinct rows
        - Any duplicate rows (entire row duplicates across all columns)
        are detected and reported

        Parameters:
        No parameters

        Returns:
            ValidationResult:
                PASS  - If all files contain only unique rows
                FAIL  - If validation fails due to:
                        • Presence of duplicate rows
                        • Runtime exceptions
        """

        try:
            failures = {}

            for file_path, df in file_dfs.items():
                total_count = df.count()
                distinct_count = df.distinct().count()

                duplicate_rows = total_count - distinct_count

                if duplicate_rows > 0:
                    failures[file_path] = (
                        f"{duplicate_rows} duplicate rows found"
                    )

            if failures:
                return ValidationResult(
                    "DUPLICATE_ROWS_CHECK",
                    "FAIL",
                    "; ".join(f"{f}: {r}" for f, r in failures.items())
                )

            return ValidationResult(
                "DUPLICATE_ROWS_CHECK",
                "PASS",
                "No duplicate rows found"
            )

        except Exception as e:
            return ValidationResult(
                "DUPLICATE_ROWS_CHECK",
                "FAIL",
                f"Unable to validate duplicate rows: {str(e)}"
            )

