# Databricks notebook source
# MAGIC %md
# MAGIC # Validation Framework Overview
# MAGIC
# MAGIC ##  What is a Validation Framework?
# MAGIC The **Validation Framework** is a structured system designed to validate all ingested files **before any processing begins**.  
# MAGIC Its primary goal is to ensure data quality, consistency, and reliability by performing multiple levels of validation.
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ##  Types of Validation Checks
# MAGIC
# MAGIC The framework is divided into three main categories of validation:
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### 1 File Checks
# MAGIC These checks validate the **integrity and properties of incoming files**.
# MAGIC
# MAGIC **Includes:**
# MAGIC -  File name validation  
# MAGIC -  File count verification  
# MAGIC -  File compression time check  
# MAGIC -  File extension validation  
# MAGIC -  File modification check  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2 Schema Checks
# MAGIC These checks ensure the **structure and data types** of the data are correct.
# MAGIC
# MAGIC **Includes:**
# MAGIC -  Data type validation  
# MAGIC -  Column order validation  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3 Data Checks
# MAGIC These checks focus on the **actual content and quality of the data**.
# MAGIC
# MAGIC **Includes:**
# MAGIC -  Uniqueness validation  
# MAGIC -  Whitespace checks  
# MAGIC -  Distinct value checks  
# MAGIC -  Range validation (e.g., minimum/maximum values)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC
# MAGIC This helps prevent downstream processing errors and improves overall data quality.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Validation Framework Checks
# MAGIC
# MAGIC ## FILE Checks
# MAGIC
# MAGIC | Category | Requirement Name | Function | Explanation | Code Example |
# MAGIC |----------|-----------------|----------|-------------|--------------|
# MAGIC | FILE | FILE_NAME_PATTERN_CHECK | `check_file_count()` | Counts files in a given path whose filenames match the regex. FAIL if zero files are found. This will return Number of files present at landing path with matching regex expression, the more robust regex, the better validation result. If you want uppercase, put it in regex, if you want lower case, then also regex will help. THROWS ERROR if NO FILE FOUND. | `landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'`<br>`regex = '^dq_src_input2.csv.$'`<br>`report = (`<br>`    ValidationOrchestrator(spark)`<br>`    .for_files(landing_path,regex)`<br>`    .check_file_count()`<br>`    .build_report()`<br>`)` |
# MAGIC | FILE | FILE_NAMES_EXACT_CHECK | `check_file_count()` | | |
# MAGIC | FILE | FILE_EXISTENCE_CHECK | `check_file_count()` | | |
# MAGIC | FILE | FILE_COUNT_CHECK | `check_expected_file_count(expected = int)` | Counts files in a given path whose filenames match the regex. FAIL if expected files count are not found. This will check whether the number of files matching the regex present at landing_path is equal to expected count. Throws error if not matched. | `landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'`<br>`regex = '^dq_src_input2.csv.$'`<br>`report = (`<br>`    ValidationOrchestrator(spark)`<br>`    .for_files(landing_path,regex)`<br>`    .check_expected_file_count(expected = 1)`<br>`    .build_report()`<br>`)` |
# MAGIC | FILE | FILE_SIZE_CHECK | `check_size(min_size: int, max_size: int)` | Validates the size (in bytes) of each file in a given path whose filenames match the regex. PASS if ALL matched files have size within [min_size, max_size]. FAIL if ANY file is outside the expected size range. Checks size of all the files present at landing path with given regex. Throw ERROR if any file does not fall within the range. | `landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'`<br>`regex = '^dq_src_input2.csv.$'`<br>`report = (`<br>`    ValidationOrchestrator(spark)`<br>`    .for_files(landing_path,regex)`<br>`    .check_size(min_size=0, max_size=100000)`<br>`    .build_report()`<br>`)` |
# MAGIC | FILE | FILE_FRESHNESS_CHECK | `check_file_freshness(max_age_hours: int)` | Checks the freshness of files in a given path whose filenames match the regex. A file is considered fresh if: `current_time - file_modification_time <= max_age_hours`. PASS if ALL matched files are fresh. FAIL if ANY file is older than max_age_hours. | `landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'`<br>`regex = '^dq_src_input2.csv.$'`<br>`report = (`<br>`    ValidationOrchestrator(spark)`<br>`    .for_files(landing_path,regex)`<br>`    .check_file_freshness(max_age_hours=424)`<br>`    .build_report()`<br>`)` |
# MAGIC | FILE | FILE_ALREADY_ARCHIVED | `check_file_already_archived(archive_path: str)` | Checks whether files matching the given regex already exist in the archive location. FAIL if any matching file is found. PASS if no matching files are present. It takes a list of all the files present at landing path with matching regex, then searches in the archive location for each file in the list. Throws ERROR if any landing file is present in archive. | `landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'`<br>`regex = '^dq_src_input2.csv.$'`<br>`report = (`<br>`    ValidationOrchestrator(spark)`<br>`    .for_files(landing_path,regex)`<br>`    .check_file_already_archived(ARCHIVE_PATH)`<br>`    .build_report()`<br>`)` |
# MAGIC | FILE | DUPLICATE_INGESTION_CHECK | `check_duplicate_file()` | Checks for duplicate files in a given path whose filenames match the regex. A duplicate is defined as: same file name (exact match) and same file size (exact match). PASS if no duplicates are found. FAIL if any duplicates are detected. | `landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'`<br>`regex = '^dq_src_input2.csv.$'`<br>`report = (`<br>`    ValidationOrchestrator(spark)`<br>`    .for_files(landing_path,regex)`<br>`    .check_duplicate_file()`<br>`    .build_report()`<br>`)` |
# MAGIC | FILE | FILE_COMPRESSION_CHECK | `check_file_compression(allowed_compressions: list)` | Validates that each file matching the regex uses an allowed compression format. PASS if ALL matched files use allowed compression. FAIL if ANY file uses a disallowed compression. | `landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'`<br>`regex = '^dq_src_input2.csv.$'`<br>`report = (`<br>`    ValidationOrchestrator(spark)`<br>`    .for_files(landing_path,regex)`<br>`    .check_file_compression(['zip'])`<br>`    .build_report()`<br>`)` |
# MAGIC | FILE | FILE_EXTENSION_CHECK | `check_file_extension(allowed_ext: list)` | Validates that each file matching the regex has an allowed extension. PASS if ALL matched files have extensions in allowed_ext. FAIL if ANY file has an invalid extension. | `landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'`<br>`regex = '^dq_src_input2.csv.$'`<br>`report = (`<br>`    ValidationOrchestrator(spark)`<br>`    .for_files(landing_path,regex)`<br>`    .check_file_extension(['csv'])`<br>`    .build_report()`<br>`)` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SCHEMA Checks
# MAGIC
# MAGIC | Category | Requirement Name | Function | Explanation | Code Example |
# MAGIC |----------|-----------------|----------|-------------|--------------|
# MAGIC | SCHEMA | REQUIRED_COLUMN_CHECK | `check_required_columns(required_cols: list, allow_extra: bool = False)` *(allow_extra is Optional, default False)* | Validates that all input DataFrames/files contain the required set of columns. The check is case-insensitive and ensures: all required columns are present in each DataFrame; no extra columns are present, unless allow_extra is set to True. PASS if ALL files contain every column listed in required_cols and (when allow_extra=False) contain no additional columns. FAIL if ANY file is missing one or more required columns or contains extra columns when allow_extra=False. | `landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'`<br>`regex = '^dq_src_input2.csv.$'`<br>`report = (`<br>`    ValidationOrchestrator(spark)`<br>`    .for_files(landing_path,regex)`<br>`    .check_required_columns(['name'], True)`<br>`    .build_report()`<br>`)` |
# MAGIC | SCHEMA | FORBIDDEN_COLUMN_CHECK | `check_forbidden_columns(forbidden_cols: list)` | Validates that none of the forbidden columns are present in any of the input DataFrames/FILE. The check is case-insensitive and compares DataFrame column names against the provided forbidden column list. PASS if ALL files do not contain any column listed in forbidden_cols. FAIL if ANY file contains one or more forbidden columns. | `landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'`<br>`regex = '^dq_src_input2.csv.$'`<br>`report = (`<br>`    ValidationOrchestrator(spark)`<br>`    .for_files(landing_path,regex)`<br>`    .check_forbidden_columns(['name'], True)`<br>`    .build_report()`<br>`)` |
# MAGIC | SCHEMA | DATA_DTYPE_CHECK | `check_data_dtype(type_map: dict, allowed_extra: bool = False)` *(allow_extra is Optional, default False)* | Validates that DataFrame columns match the expected logical data types defined in the type_map. Column name comparison is case-insensitive, and Spark-specific data types are normalized: StringType→"string", Integer/Long/Short→"integer", Float/Double/Decimal→"float", BooleanType→"boolean", DateType→"date", TimestampType→"timestamp". If allowed_extra is False, extra columns cause FAILURE; if True, extra columns are ignored. PASS if ALL files contain all expected columns with matching types and no extra columns (when allowed_extra=False). FAIL if ANY file is missing a column, has a mismatched type, or has extra columns when allowed_extra=False. | `landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'`<br>`regex = '^dq_src_input2.csv.$'`<br>`report = (`<br>`    ValidationOrchestrator(spark)`<br>`    .for_files(landing_path,regex)`<br>`    .check_data_dtype({'Id':'Integer','Name':'string','Marks':'integer','Date_of_Birth':'date'}, True)`<br>`    .build_report()`<br>`)` |
# MAGIC | SCHEMA | COLUMN_ORDER_CHECK | `check_column_order(expected_order: list)` | Validates that DataFrame/files columns appear in the exact expected order. The comparison is case-insensitive, but the relative ordering of columns must match the expected_order exactly. PASS if ALL files contain the same columns as expected_order and the column sequence matches exactly (case-insensitive). FAIL if ANY file has columns in a different order, is missing expected columns, or contains extra columns not listed in expected_order. | `landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'`<br>`regex = '^dq_src_input2.csv.$'`<br>`report = (`<br>`    ValidationOrchestrator(spark)`<br>`    .for_files(landing_path,regex)`<br>`    .check_column_order(['Id','NaMe','Marks','Date_of_Birth','Pass'])`<br>`    .build_report()`<br>`)` |
# MAGIC | SCHEMA | COLUMN_COUNT_CHECK | `check_column_count(min_count: int = None, max_count: int = None)` | Validates that the number of columns in each DataFrame falls within the specified minimum and maximum bounds. The check enforces column count limits only and does not consider column names or order. PASS if ALL files have a column count >= min_count and <= max_count. FAIL if ANY file has fewer columns than min_count or more columns than max_count. | `landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'`<br>`regex = '^dq_src_input2.csv.$'`<br>`report = (`<br>`    ValidationOrchestrator(spark)`<br>`    .for_files(landing_path,regex)`<br>`    .check_column_count(1, 10)`<br>`    .build_report()`<br>`)` |
# MAGIC | SCHEMA | NULLABLE_CONSTRAINT_CHECK | `check_null(non_null_cols: list)` | Validates that specified columns do not contain NULL values. The check is case-insensitive and ensures each column listed in non_null_cols exists in the DataFrame and has zero NULL values. PASS if ALL files contain all listed columns and none of those columns contain NULL values. FAIL if ANY file is missing a listed column or contains one or more NULL values in those columns. | `landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'`<br>`regex = '^dq_src_input2.csv.$'`<br>`report = (`<br>`    ValidationOrchestrator(spark)`<br>`    .for_files(landing_path,regex)`<br>`    .check_null(['id'])`<br>`    .build_report()`<br>`)` |
# MAGIC | SCHEMA | COLUMN_SENSITIVITY | `check_column_sensitivity(cols: list)` | Validates the presence of specified columns with exact case sensitivity. This check enforces strict, case-sensitive matching of column names against the provided list. Unlike other column checks, no case normalization is applied. PASS if ALL files contain every column listed in cols with the exact same casing. FAIL if ANY file is missing one or more columns due to case mismatch or does not contain a column with the exact expected name. | `landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'`<br>`regex = '^dq_src_input2.csv.$'`<br>`report = (`<br>`    ValidationOrchestrator(spark)`<br>`    .for_files(landing_path,regex)`<br>`    .check_column_sensitivity(['project'])`<br>`    .build_report()`<br>`)` |
# MAGIC | SCHEMA | FILE_HEADER_PRESENCE_CHECK *(NEW)* | `check_file_header_presence()` | Validates that input files contain a proper header row. This check ensures that each DataFrame/File has at least one column and does not rely on Spark auto-generated column names (e.g. _c0, _c1, ...), which indicate a missing header. PASS if ALL files contain one or more columns and do not have only auto-generated Spark column names. FAIL if ANY file has no columns (empty or malformed) or has a missing header row detected via auto-generated columns. | `landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'`<br>`regex = '^dq_src_input2.csv.$'`<br>`report = (`<br>`    ValidationOrchestrator(spark)`<br>`    .for_files(landing_path,regex)`<br>`    .check_file_header_presence()`<br>`    .build_report()`<br>`)` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## DATA Checks
# MAGIC
# MAGIC | Category | Requirement Name | Function | Explanation | Code Example |
# MAGIC |----------|-----------------|----------|-------------|--------------|
# MAGIC | DATA | CHECK EXPECTED VALUES | `check_expected_values(column: str, expected_values: list, case_insensitive: bool = True)` | Validates that a specified column in multiple DataFrames contains only values from a given list of expected values. Iterates over each file/DataFrame pair and verifies: the target column exists; all values in the column belong to the allowed expected values list; comparison can be case-insensitive (optional). PASS if all files contain only expected values. FAIL if validation fails due to invalid input types, missing columns, unexpected values, or runtime exceptions. | `SOURCE_FILES_PATH = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'`<br>`regex = '^dq_src_input.*$'`<br>`report = (`<br>`    ValidationOrchestrator(spark)`<br>`    .for_files(SOURCE_FILES_PATH, regex)`<br>`    .check_expected_values('Pass', ['true','false'])`<br>`    .build_report()`<br>`)` |
# MAGIC | DATA | CHECK INVALID VALUES | `check_invalid_values(column: str, invalid_values: list, case_insensitive: bool = True)` | Validates that a column does not contain any disallowed (invalid) values. The check verifies values in the specified column against a provided list of invalid values. Comparison can be case-insensitive or case-sensitive. PASS if ALL files contain the specified column and do not contain any values listed in invalid_values. FAIL if ANY file is missing the specified column or contains one or more invalid values in that column. | `SOURCE_FILES_PATH = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'`<br>`regex = '^dq_src_input.*$'`<br>`report = (`<br>`    ValidationOrchestrator(spark)`<br>`    .for_files(SOURCE_FILES_PATH, regex)`<br>`    .check_invalid_values('name', ['Amit','Anita','Karan','Neha'], True)`<br>`    .build_report()`<br>`)` |
# MAGIC | DATA | NO_NEGATIVE_VALUES_CHECK *(NEW)* | `check_no_negative_values(column: str)` | Validates that a specified column in multiple DataFrames contains only non-negative numeric values. Verifies: the target column exists (case-insensitive match); all non-null values in the column are numeric; no negative values are present. PASS if all files contain only non-negative numeric values. FAIL if validation fails due to invalid column argument, missing columns, non-numeric values, negative values, or runtime exceptions. | `SOURCE_FILES_PATH = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'`<br>`regex = '^dq_src_input.*$'`<br>`report = (`<br>`    ValidationOrchestrator(spark)`<br>`    .for_files(SOURCE_FILES_PATH, regex)`<br>`    .check_no_negative_values('marks')`<br>`    .build_report()`<br>`)` |
# MAGIC | DATA | STRING_LENGTH_CHECK *(NEW)* | `check_string_length(column: str, min_len: int, max_len: int)` | Validates that a specified string column in multiple DataFrames has values whose trimmed length falls within configured bounds. Verifies: the target column exists (case-insensitive match); the column data type is a string; all non-null values after trimming whitespace have lengths within the inclusive range [min_len, max_len]. PASS if all files satisfy the string length constraints. FAIL if validation fails due to invalid arguments, missing columns, non-string column types, string length violations, or runtime exceptions. | `SOURCE_FILES_PATH = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'`<br>`regex = '^dq_src_input.*$'`<br>`report = (`<br>`    ValidationOrchestrator(spark)`<br>`    .for_files(SOURCE_FILES_PATH, regex)`<br>`    .check_string_length('Date_of_Birth', 10, 10)`<br>`    .build_report()`<br>`)` |
# MAGIC | DATA | NO_WHITESPACE_CHECK *(NEW)* | `check_no_whitespace(column: str)` | Validates that a specified string column in multiple DataFrames does not contain any whitespace characters. Verifies: the target column exists (case-insensitive match); the column data type is a string; no non-null values in the column contain whitespace characters (spaces, tabs, newlines, etc.). PASS if all files have no whitespace in the specified column. FAIL if validation fails due to invalid column argument, missing columns, non-string column types, presence of whitespace, or runtime exceptions. | `SOURCE_FILES_PATH = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'`<br>`regex = '^dq_src_input.*$'`<br>`report = (`<br>`    ValidationOrchestrator(spark)`<br>`    .for_files(SOURCE_FILES_PATH, regex)`<br>`    .check_no_whitespace('name')`<br>`    .build_report()`<br>`)` |
# MAGIC | DATA | CHECK DATE_FORMAT | `check_date_format(column: str, date_format: str)` | Validates that a specified column in multiple DataFrames conforms to a given date format. Verifies: the target column exists (case-insensitive match); invalid or unparsable date values are detected. PASS if all files contain valid dates in the expected format. FAIL if validation fails due to invalid input arguments, missing columns, date parsing failures, or runtime exceptions. | `SOURCE_FILES_PATH = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'`<br>`regex = '^dq_src_input.*$'`<br>`report = (`<br>`    ValidationOrchestrator(spark)`<br>`    .for_files(SOURCE_FILES_PATH, regex)`<br>`    .check_date_format('date_of_Birth', 'dd/mm/yyyy')`<br>`    .build_report()`<br>`)` |
# MAGIC | DATA | ROW_COUNT_CHECK | `check_row_count(min_count: int = None, max_count: int = None)` | Validates that each DataFrame contains a number of rows within specified minimum and/or maximum bounds. Verifies: row count is >= min_count (if provided); row count is <= max_count (if provided); bounds are logically valid. PASS if all files have row counts within the expected limits. FAIL if validation fails due to invalid input arguments, row count below minimum or above maximum, or runtime exceptions. | `SOURCE_FILES_PATH = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'`<br>`regex = '^dq_src_input.*$'`<br>`report = (`<br>`    ValidationOrchestrator(spark)`<br>`    .for_files(SOURCE_FILES_PATH, regex)`<br>`    .check_row_count(7, 7)`<br>`    .build_report()`<br>`)` |
# MAGIC | DATA | COMPLETENESS_RATIO_CHECK *(NEW)* | `check_completeness_ratio(threshold_pct: int/float)` | Validates that the overall data completeness of each DataFrame meets or exceeds a specified percentage threshold. Completeness = (Number of non-null cells / Total number of cells) * 100. Verifies: the threshold is a valid percentage (0–100); the DataFrame is non-empty; the calculated completeness ratio meets or exceeds the threshold. PASS if all files meet or exceed the completeness threshold. FAIL if validation fails due to invalid threshold value, empty datasets, completeness below threshold, or runtime exceptions. | `SOURCE_FILES_PATH = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'`<br>`regex = '^dq_src_input.*$'`<br>`report = (`<br>`    ValidationOrchestrator(spark)`<br>`    .for_files(SOURCE_FILES_PATH, regex)`<br>`    .check_completeness_ratio(80)`<br>`    .build_report()`<br>`)` |
# MAGIC | DATA | UNIQUE_CHECK | `check_unique(key_columns: list)` | Validates that the specified key columns uniquely identify each row in multiple DataFrames. Verifies: the list of key columns is valid and non-empty; all key columns exist in the DataFrame (case-insensitive match); the combination of key column values is unique for every row. PASS if all files contain unique key combinations. FAIL if validation fails due to invalid key column list, missing key columns, duplicate key combinations, or runtime exceptions. | `SOURCE_FILES_PATH = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'`<br>`regex = '^dq_src_input.*$'`<br>`report = (`<br>`    ValidationOrchestrator(spark)`<br>`    .for_files(SOURCE_FILES_PATH, regex)`<br>`    .check_unique(['Marks','Name'])`<br>`    .build_report()`<br>`)` |
# MAGIC | DATA | KPI_AGGREGATE_CHECK | `check_kpi_aggregate(expr: str, min_val, max_val)` | Validates that the result of a KPI aggregate expression falls within a specified numeric range for multiple DataFrames. Verifies: the aggregate expression is a valid, non-empty Spark SQL expression; the expression can be successfully evaluated on the DataFrame; the resulting aggregate value is non-null and lies within the inclusive range [min_val, max_val]. PASS if all files produce an aggregate value within the expected range. FAIL if validation fails due to invalid expression or parameters, expression evaluation errors, NULL aggregate results, aggregate value outside the allowed range, or runtime exceptions. | `SOURCE_FILES_PATH = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'`<br>`regex = '^dq_src_input.*$'`<br>`report = (`<br>`    ValidationOrchestrator(spark)`<br>`    .for_files(SOURCE_FILES_PATH, regex)`<br>`    .check_kpi_aggregate("sum(markS)", 7, 7)`<br>`    .build_report()`<br>`)` |
# MAGIC | DATA | VALUE_RANGE_CHECK | `check_value_range(column_name: str, min_val, max_val)` | Validates that all values in a specified column fall within a defined numeric range across multiple DataFrames. Verifies: the target column exists (case-insensitive match); all non-null values in the column are numeric; all numeric values fall within the inclusive range [min_val, max_val]. PASS if all files contain only numeric values within the range. FAIL if validation fails due to invalid input parameters, missing columns, presence of non-numeric values, values outside the allowed range, or runtime exceptions. | `SOURCE_FILES_PATH = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'`<br>`regex = '^dq_src_input.*$'`<br>`report = (`<br>`    ValidationOrchestrator(spark)`<br>`    .for_files(SOURCE_FILES_PATH, regex)`<br>`    .check_value_range('marks', 0, 66)`<br>`    .build_report()`<br>`)` |
# MAGIC | DATA | REGEX_FORMAT_CHECK | `check_regex_format(column_name: str, pattern: str)` | Validates that values in a specified column conform to a given regular expression pattern across multiple DataFrames. Verifies: the target column exists (case-insensitive match); the provided regex pattern is valid and non-empty; all relevant non-null, non-literal-'null' values in the column match the supplied regular expression after trimming whitespace. PASS if all files contain values matching the regex pattern. FAIL if validation fails due to invalid input parameters, missing columns, values that do not match the regex pattern, or runtime exceptions. | `SOURCE_FILES_PATH = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'`<br>`regex = '^dq_src_input.*$'`<br>`pattern = r"^(0[1-9]|[12][0-9]|3[01])/(0[1-9]|1[0-2])/\d{4}$"`<br>`report = (`<br>`    ValidationOrchestrator(spark)`<br>`    .for_files(SOURCE_FILES_PATH, regex)`<br>`    .check_regex_format('Date_of_BirtH', pattern)`<br>`    .build_report()`<br>`)` |
# MAGIC | DATA | NO_FUTURE_DATE_CHECK | `check_no_future_date(date_column: str, date_format: str)` | Validates that a specified date column does not contain any future dates across multiple DataFrames. Verifies: the target date column exists (case-insensitive match); date values conform to the specified date format; all valid dates are <= the current system date; invalid or unparsable date values are detected separately. PASS if all files contain no future dates. FAIL if validation fails due to invalid input parameters, missing columns, invalid date formats, presence of future dates, or runtime exceptions. | `SOURCE_FILES_PATH = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'`<br>`regex = '^dq_src_input.*$'`<br>`report = (`<br>`    ValidationOrchestrator(spark)`<br>`    .for_files(SOURCE_FILES_PATH, regex)`<br>`    .check_no_future_date('Date_of_BirtH', 'dd/mm/yyyy')`<br>`    .build_report()`<br>`)` |
# MAGIC | DATA | DUPLICATE_ROWS_CHECK | `check_duplicate_rows()` | Validates that each DataFrame does not contain duplicate rows. Verifies: the total number of rows equals the number of distinct rows; any duplicate rows (entire row duplicates across all columns) are detected and reported. No parameters required. PASS if all files contain only unique rows. FAIL if validation fails due to presence of duplicate rows or runtime exceptions. | `SOURCE_FILES_PATH = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'`<br>`regex = '^dq_src_input.*$'`<br>`report = (`<br>`    ValidationOrchestrator(spark)`<br>`    .for_files(SOURCE_FILES_PATH, regex)`<br>`    .check_duplicate_rows()`<br>`    .build_report()`<br>`)` |

# COMMAND ----------

# MAGIC %md
# MAGIC # How to Use the Validation Framework
# MAGIC
# MAGIC ## Setup
# MAGIC
# MAGIC Before anything else, run the orchestrator notebook:
# MAGIC
# MAGIC ```python
# MAGIC %run ./nb_validation_orchestrator
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Two Ways to Use It
# MAGIC
# MAGIC | # | Method | Function | Available Checks | When to Use |
# MAGIC |---|--------|----------|-----------------|-------------|
# MAGIC | 1 | **DataFrame-based** | `for_dataframes(df_list)` | Schema & Data checks only | When you already have DataFrames loaded in memory |
# MAGIC | 2 | **File-based** | `for_files(landing_path, regex)` | FILE + Schema + Data checks (all) | When validating raw files directly from a storage path |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Method 1 — `for_dataframes()`
# MAGIC
# MAGIC Pass a list of DataFrames directly into the orchestrator.
# MAGIC
# MAGIC >  **Important:** Each DataFrame **must** contain a column named `_source_path_`. This column is used to distinguish one DataFrame from another within the list. You may use any meaningful name to keep track of the source.
# MAGIC
# MAGIC ```python
# MAGIC report = (
# MAGIC     ValidationOrchestrator(spark)
# MAGIC     .for_dataframes(df_list)
# MAGIC     .check_required_columns(['name'], True)
# MAGIC     .check_data_dtype({'Name': 'string', 'Id': 'Integer', 'Marks': 'integer', 'Date_of_Birth': 'date'}, True)
# MAGIC     .check_column_order(['Id', 'NaMe', 'Marks', 'Date_of_Birth', 'Pass'])
# MAGIC     .check_null(['id'])
# MAGIC     .build_report()
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Method 2 — `for_files()`
# MAGIC
# MAGIC Pass the landing path and a regex pattern. The orchestrator will validate **every file** at that location whose name matches the regex.
# MAGIC
# MAGIC >  **Tip:** For Schema and Data checks, the orchestrator internally reads files and derives DataFrames automatically. However, you can also supply your own list of DataFrames by passing it as a third argument: `.for_files(landing_path, regex, df_list)`
# MAGIC
# MAGIC ```python
# MAGIC landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
# MAGIC regex        = '^dq_src_input2.csv.$'
# MAGIC # df_list    = [df2, df3]   # optional — pass your own DataFrames if needed
# MAGIC
# MAGIC report = (
# MAGIC     ValidationOrchestrator(spark)
# MAGIC     .for_files(landing_path, regex)
# MAGIC     .check_file_count()
# MAGIC     .check_expected_file_count(expected=1)
# MAGIC     .check_size(min_size=0, max_size=100000)
# MAGIC     .build_report()
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC | Point | Detail |
# MAGIC |-------|--------|
# MAGIC | **Custom DataFrames** | You can optionally pass a pre-built DataFrame list as a third argument instead of relying on the default file reader: `.for_files(landing_path, regex, df_list)` |
# MAGIC | **Regex Precision** | The more specific and robust your regex, the more accurate the file matching. Use uppercase/lowercase patterns in the regex itself to control case sensitivity. |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Raising an Alert on Failure
# MAGIC
# MAGIC To stop pipeline execution when validation fails, use `raise_if_failed()`:
# MAGIC
# MAGIC ```python
# MAGIC try:
# MAGIC     report.raise_if_failed()
# MAGIC     print("Overall Status: Passed")
# MAGIC except Exception as e:
# MAGIC     dbutils.notebook.exit(f"Pipeline Failed: {str(e)}")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Considerations
# MAGIC
# MAGIC | Point | Detail |
# MAGIC |-------|--------|
# MAGIC | **Flexibility** | You can chain any number of check functions based on your validation requirements. |
# MAGIC | **Performance Trade-off** | Each additional check consumes more compute resources. Use only the checks that are necessary for your use case. |
# MAGIC | **Source Tracking** | In `for_dataframes()` mode, ensure the `_source_path_` column is present in every DataFrame so results can be traced back to the correct source. |
# MAGIC