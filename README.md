# =============================================================================

class SdlChecks:
    """
    SDL-specific validation checks.

    Rules
    -----
    - Every method MUST return a ValidationResult(check_name, status, message).
    - Use self.spark, self.file_path, self.regex , freely —
      the framework sets these before any check runs.
    - No __init__ required .
    """


    @staticmethod
    def _read_raw_csv_2():
        """Read the file without headers"""
        return (
            spark.read
            .option("header", "False")
            .option("inferSchema", "False")
            .csv(self.path)
            .withColumn("_row_id", monotonically_increasing_id())
            .filter(col("_row_id") >= 7)
        )

    def check_consecutive_values_row(self, values: list, allow = True, transform_fn=None) -> ValidationResult:
        try:

            """
            Validates whether a specific sequence of values (exact or regex-based)
            appears consecutively within any single row across files.

            The check constructs a comma-separated representation of each row
            and applies a regex match to detect consecutive patterns.

            PASS behavior:
            - allow=True  → pattern MUST be present in ALL files
            - allow=False → pattern MUST NOT be present in ALL files

            FAIL behavior:
            - If allow=True  and ANY file does not contain the pattern
            - If allow=False and ANY file contains the pattern
            """

            if not values or not isinstance(values, list):
                return ValidationResult(
                    "check_consecutive_values_row",
                    "FAIL",
                    "values must be a non-empty list"
                )

            
            if not isinstance(allow, bool):
                return ValidationResult(
                    "CONSECUTIVE_VALUES_ROW_CHECK",
                    "FAIL",
                    "allow must be a boolean value"
                )
            
            if transform_fn is not None and not callable(transform_fn):
                return ValidationResult(
                    "CONSECUTIVE_VALUES_ROW_CHECK",
                    "FAIL",
                    "transform_fn must be callable"
                )

            # Build regex for consecutive matching
            # Assume values are already valid regex or literals
            regex_parts = [v.strip().lower() for v in values]
            row_regex = ",".join(regex_parts)   # enforces adjacency
            failures = {}

            file_df_dict = self._get_file_dfs()
            for file_path, df in file_df_dict.items():
                # raw_df = self._read_raw_csv(file.path)

                
                # Apply transformation if provided
                df_tranformed = transform_fn(df) if transform_fn else df


                df_idx = df_tranformed.withColumn(
                    "_row_id", monotonically_increasing_id()
                )

                df_arr = df_idx.select(
                    "_row_id",
                    array(
                        *[
                            lower(trim(col(c).cast("string")))
                            for c in df.columns
                        ]
                    ).alias("row_values")
                )

                df_str = df_arr.withColumn(
                    "row_str",
                    concat_ws(",", col("row_values"))
                )

                found = (
                    df_str
                    .filter(col("row_str").rlike(row_regex))
                    .count() > 0
                )

                # allow = True  → pattern must be found
                if allow and not found :
                    
                    failures[file_path] = (
                                        f"Expected consecutive pattern not found: {values}"
                                    )
               
                # allow = False → pattern must NOT be found
                if not allow and found:
                    failures[file_path] = (f"Expected consecutive pattern not found: {values}" )

            # If all files satisfy the rule

            # ----------------------------
            # Final result
            # ----------------------------
            if failures:
                return ValidationResult(
                    "CONSECUTIVE_VALUES_ROW_CHECK",
                    "FAIL",
                    "; ".join(f"{k}: {v}" for k, v in failures.items())
                )

            return ValidationResult(
                "CONSECUTIVE_VALUES_ROW_CHECK",
                "PASS",
                "Consecutive row pattern validation passed"
            
            )

        except Exception as e:
            return ValidationResult(
                "check_consecutive_values_row",
                "FAIL",
                str(e)
            )



    #   Add more SDL checks below — same pattern, always return ValidationResult





# =============================================================================
#  USAGE
# =============================================================================

SOURCE_FILES_PATH = "abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_COE/Private/Inbox/archive/SDL/A4T/ingest_dt=2026-04-19/"

regex = "^ec_model_new\.csv$"
report = (
    ValidationOrchestrator(spark)
    .with_checks(SdlChecks())              # ← register your checks once

    .for_files(SOURCE_FILES_PATH, regex, base_transfrom = SdlChecks._read_raw_csv_2())

    # ── Base checks (inherited) ──────────────────────────────────────────────
    .check_file_count()
    .check_size(min_size=100, max_size=50_000_000)
    # .check_test()
    # .check_required_columns(["model_id", "revenue"])
    # .check_null(["model_id"])

    # ── SDL custom checks (auto-discovered from SdlChecks) ───────────────────
    # .find_start_cell(keywords = ['fedex','ups','usps','amzn','others'])
    # .check_consecutive_values_row( [ "cy\\d{4}",  "null",  "cy\\d{4}\\s+ratio"],allow = False)
    .check_consecutive_values_row( [ "UPs",  "252"],allow = True)
    # .check_consecutive_values_column(['ADV','ONP EC','ONP Non-EC'])
    # .check_values_with_neighbors(['ADV'] ,up = 'null')


    .build_report()
)
# report.raise_if_failed()

