What changed and why
nb_all_checks.py
[NEW-01] — _ColListValidatorMixin
SchemaChecks and DataChecks each had an identical _validate_col_list static method copy-pasted word-for-word. Any bug fix or new rule had to be applied twice. This is now a single shared mixin class that both inherit from — one source of truth.
[FIX-01] — Tuple in imports
Tuple was missing from the typing import but was used in _validate_df_list's return annotation. Silent NameError waiting to happen at runtime.
[FIX-02] (checks) — check_data_dtype type_map mutation bug
The original code re-normalised type_map (.lower() on keys) inside the per-file loop, which mutated the outer variable on the first iteration. Every subsequent file would compare against the already-lowercased map, so files 2+ would fail with false positives for any column whose expected type string had uppercase characters. Now normalised once before the loop.
[FIX-03] — check_size wrong check_name
Both parameter-guard returns used "STRING_LENGTH_CHECK" as the check name inside check_size. Fixed to "FILE_SIZE_CHECK".
[FIX-04] — check_expected_file_count signature
expected = int assigned the int type object as the default value. Changed to expected: int (type annotation only, no default — caller must always supply it).
[FIX-05] — check_duplicate_file used pattern.search inconsistently
Every other FileChecks method used pattern.match. Standardised to pattern.match.
[FIX-06] — "FAIL:"/"PASS:" prefixes stripped from message strings
The status field already holds "FAIL" or "PASS". Having it in the message too breaks the report table (Details column reads "FAIL: No files found…" while Status already says FAIL) and makes programmatic message parsing messy.
[FIX-07] — check_null used display(e) instead of raising
display() is a Databricks notebook helper that renders the exception visually but swallows it — the check silently returned None on error. Replaced with proper exception handling that returns a ValidationResult.
[FIX-08] — check_duplicate_rows double scan replaced
The original count() + distinct().count() approach tells you the surplus row count but nothing about which rows are duplicated. The new groupBy(...).count().filter(count > 1).count() approach tells you how many distinct duplicate groups exist — far more actionable when debugging.
[FIX-09] — Stray print() statements removed from check_string_length

nb_validation_orchestrator.py
[NEW-01] — base_transform in for_files()
df_list is removed from for_files() (it belonged in for_dataframes() anyway). Replaced with an optional base_transform: Callable[[DataFrame, str], DataFrame]. After reading each file, if a transform is provided it is called as base_transform(df, file_path) before the DataFrame is cached. All subsequent checks — including plugin checks — see the transformed data automatically.
[NEW-02] — base_transform in for_dataframes()
Same parameter added here. Applied to each caller-supplied DataFrame before caching. Useful when raw DataFrames come from an upstream pipeline and you still want one common preparation step applied centrally.
[NEW-03] — base_transform injected into plugin context
register_checks() now sets plugin.base_transform = self.base_transform. Plugin methods can inspect or reference the active transform, and since they call self._get_file_dfs() which already returns the transformed cache, they see transformed data with zero extra wiring.
[NEW-04] — ValidationReport.to_dataframe(spark)
New method that converts all results to a Spark DataFrame with columns (check_name, status, message). Makes it trivial to write audit results to a Delta table: report.to_dataframe(spark).write.format("delta").mode("append").save(audit_path).
[NEW-05] — _validate_df_list uses df.limit(1).count() instead of df.rdd.isEmpty()
rdd.isEmpty() forces full RDD materialisation. limit(1).count() stops at the first row — far cheaper on large DataFrames.
[FIX-02] (orchestrator) — raise_if_failed raises ValidationError
Was raising bare Exception. Changed to ValidationError so callers can catch the framework's own exception type cleanly.
[FIX-03] (orchestrator) — print_summary now uses truncated details
The details = truncate(r.message, DETAILS_COL) variable was computed but then r.message was printed raw. Long messages overflowed the table. Now correctly uses details.
[FIX-04] — build_report unpersists in a finally block
Previously the unpersist only ran if the loop completed successfully. If any check raised mid-run, the cached DataFrames leaked in Spark memory for the lifetime of the cluster. The finally block guarantees cleanup regardless.
[FIX-05] — _validate_file_path uses pattern.match not pattern.search
The orchestrator was using pattern.search to validate file existence, but the builder's _get_file_dfs used pattern.match. A file could pass orchestrator validation and then silently not appear in the builder's cache.
[FIX-06] — ValidationBuilderChild calls super().__init__()
Was manually re-declaring every field (self.spark, self._df_cache, self._steps, etc.) by copy-paste. Any new field added to ValidationBuilder would be silently missing in the child. Now delegates to super().__init__() and only overrides what differs.
