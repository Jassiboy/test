Category	Requirement Name	Function	Explanation	Code_Example
FILE	FILE_NAME_PATTERN_CHECK	check_file_count()	"Counts files in a given path whose filenames match the regex.
FAIL if zero files are found.This will return Number of files present at landing path with matching regex expression ,the more robust regex, the better validation result. If you want uppercase , put it regex, if you want lower case , then also regex will help.THROWS ERROR if NO FILE FOUND"	"landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
regex = '^dq_src_input2.csv.$'
report = (
    ValidationOrchestrator(spark)
    .for_files(landing_path,regex)
    .check_file_count() .build_report()
)"
FILE	FILE_NAMES_EXACT_CHECK	check_file_count()		
FILE	FILE_EXISTENCE_CHECK	check_file_count()		
FILE	FILE_COUNT_CHECK	check_expected_file_count(expected = int)	" Counts files in a given path whose filenames match the regex.
FAIL if expected files count are not found.This  will check whether the number of file matching the regex is present at landing_path is equal to expected count. Throws error if not matched.
"	"landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
regex = '^dq_src_input2.csv.$'
report = (
    ValidationOrchestrator(spark)
    .for_files(landing_path,regex)
    .check_expected_file_count(expected = 1) .build_report()
)"
FILE	FILE_SIZE_CHECK	check_size( min_size: int,     max_size: int   )	"Validates the size (in bytes) of each file in a given path
whose filenames match the regex.

PASS if ALL matched files have size within [min_size, max_size].
FAIL if ANY file is outside the expected size range.Checks size of all the files present at landing path with given regex .Throw ERROR if any of file does not fall within the range."	"landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
regex = '^dq_src_input2.csv.$'
report = (
    ValidationOrchestrator(spark)
    .for_files(landing_path,regex)
    .check_size(min_size = 0 ,max_size = 100000) .build_report()
)"
FILE	FILE_FRESHNESS_CHECK	check_file_freshness(max_age_hours: int )	"Checks the freshness of files in a given path whose filenames
 match the regex.

 file is considered fresh if:
        current_time - file_modification_time <= max_age_hours

PASS if ALL matched files are fresh.
FAIL if ANY file is older than max_age_hours.
"	"landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
regex = '^dq_src_input2.csv.$'
report = (
    ValidationOrchestrator(spark)
    .for_files(landing_path,regex)
    .check_file_freshness(max_age_hours = 424) .build_report()
)"
FILE	FILE_ALREADY_ARCHIVED	check_file_already_archived(archive_path: str)	"Checks whether files matching the given regex already exist
in the archive location.

FAIL if any matching file is found.
PASS if no matching files are present.It basically take list of all the files present at landing path with matching regex, Now it search in the archive location for each file present in the list. Throw ERROR if any landing file present in archive.
"	"landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
regex = '^dq_src_input2.csv.$'
report = (
    ValidationOrchestrator(spark)
    .for_files(landing_path,regex)
    .check_file_already_archived(ARCHIVE_PATH).build_report()
)"
FILE	DUPLICATE_INGESTION_CHECK	check_duplicate_file( )	"Checks for duplicate files in a given path whose filenames
match the regex.

A duplicate is defined as:
        - Same file name (exact match)
        - Same file size (exact match)

PASS if no duplicates are found.
FAIL if any duplicates are detected.
"	"landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
regex = '^dq_src_input2.csv.$'
report = (
    ValidationOrchestrator(spark)
    .for_files(landing_path,regex)
    .check_duplicate_file().build_report()
)"
FILE	FILE_COMPRESSION_CHECK	check_file_compression( allowed_compressions: list )	" Validates that each file matching the regex uses an allowed compression format.

PASS if ALL matched files use allowed compression.
FAIL if ANY file uses a disallowed compression.
"	"landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
regex = '^dq_src_input2.csv.$'
report = (
    ValidationOrchestrator(spark)
    .for_files(landing_path,regex)
    .check_file_compression(['zip']).build_report()
)"
FILE	FILE_EXTENSION_CHECK	check_file_extension( allowed_ext: list ) 	"Validates that each file matching the regex has an allowed extension.
PASS if ALL matched files have extensions in allowed_ext.
 FAIL if ANY file has an invalid extension.
"	"landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
regex = '^dq_src_input2.csv.$'
report = (
    ValidationOrchestrator(spark)
    .for_files(landing_path,regex)
    .check_file_extension(['csv']).build_report()
)"
Category	Requirement Name	Function	Explanation	Code_Example
SCHEMA	REQUIRED_COLUMN_CHECK	check_required_columns(required_cols: list,  allow_extra: bool = False)-----> allow_extra is Optional , By default it is False	"        """"""
        Validates that all input DataFrames/files contain the required set of columns.

        The check is case-insensitive and ensures:
        - All required columns are present in each DataFrame.
        - No extra columns are present, unless allow_extra is set to True.

        PASS if ALL files:
        - Contain every column listed in required_cols
        - And (when allow_extra=False) contain no additional columns

        FAIL if ANY file:
        - Is missing one or more required columns
        - Or contains extra columns when allow_extra=False
        """""""	"landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
regex = '^dq_src_input2.csv.$'
report = (
    ValidationOrchestrator(spark)
    .for_files(landing_path,regex)
    .check_required_columns(['name'],True ).build_report()
)"
SCHEMA	FORBIDDEN_COLUMN_CHECK	check_forbidden_columns( forbidden_cols: list)	"
    """"""
    Validates that none of the forbidden columns are present
    in any of the input DataFrames/FILE.

    The check is case-insensitive and compares DataFrame column
    names against the provided forbidden column list.

    PASS if ALL files:
    - Do not contain any column listed in forbidden_cols

    FAIL if ANY file:
    - Contains one or more forbidden columns
    """""""	"landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
regex = '^dq_src_input2.csv.$'
report = (
    ValidationOrchestrator(spark)
    .for_files(landing_path,regex)
    .check_forbidden_columns(['name'],True) .build_report()
)"
SCHEMA	DATA_DTYPE_CHECK	check_data_dtype( type_map: dict,  allowed_extra: bool = False )----> allow_extra is Optional , By default it is False	"
    """"""
    Validates that DataFrame columns match the expected logical data types
    defined in the type_map.

    Column name comparison is case-insensitive, and Spark-specific data
    types are normalized to logical types before comparison:
    - StringType           -> ""string""
    - Integer/Long/Short   -> ""integer""
    - Float/Double/Decimal -> ""float""
    - BooleanType          -> ""boolean""
    - DateType             -> ""date""
    - TimestampType        -> ""timestamp""

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
    """""""	"landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
regex = '^dq_src_input2.csv.$'
report = (
    ValidationOrchestrator(spark)
    .for_files(landing_path,regex)
    .check_data_dtype({'Id':'Integer','Name':'string','Marks':'integer','Date_of_Birth':'date'},True) .build_report()
)"
SCHEMA	COLUMN_ORDER_CHECK	check_column_order(expected_order: list ) 	"        """"""
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
        """""""	"landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
regex = '^dq_src_input2.csv.$'
report = (
    ValidationOrchestrator(spark)
    .for_files(landing_path,regex)
    .check_column_order(['Id', 'NaMe', 'Marks', 'Date_of_Birth', 'Pass']) .build_report()
)"
SCHEMA	COLUMN_COUNT_CHECK 	check_column_count(dict,  min_count: int = None, max_count: int = None )	"        """"""
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
        """""""	"landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
regex = '^dq_src_input2.csv.$'
report = (
    ValidationOrchestrator(spark)
    .for_files(landing_path,regex)
    .check_column_count(1,10) .build_report()
)"
SCHEMA	NULLABLE_CONSTRAINT_CHECK 	check_null(non_null_cols: list ) 	"        """"""
        Validates that specified columns do not contain NULL values.

        The check is case-insensitive and ensures that each column listed
        in non_null_cols exists in the DataFrame and has zero NULL values.

        PASS if ALL files:
        - Contain all columns listed in non_null_cols
        - And none of those columns contain NULL values

        FAIL if ANY file:
        - Is missing a column listed in non_null_cols
        - Or contains one or more NULL values in those columns
        """""""	"landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
regex = '^dq_src_input2.csv.$'
report = (
    ValidationOrchestrator(spark)
    .for_files(landing_path,regex)
    .check_null(['id']) .build_report()
)"
SCHEMA	COLUMN_SENSITIVITY	check_column_sensitivity( cols: list )	"
    """"""
    Validates the presence of specified columns with exact case sensitivity.

    This check enforces strict, case-sensitive matching of column names
    against the provided list. Unlike other column checks, no case
    normalization is applied.

    PASS if ALL files:
    - Contain every column listed in cols with the exact same casing

    FAIL if ANY file:
    - Is missing one or more columns due to case mismatch
    - Or does not contain a column with the exact expected name
    """""""	"landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
regex = '^dq_src_input2.csv.$'
report = (
    ValidationOrchestrator(spark)
    .for_files(landing_path,regex)
    .check_column_sensitivity(['project']) .build_report()
)"
SCHEMA	FILE_HEADER_PRESENCE_CHECK [NEW]	 check_file_header_presence()	"        """"""
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
        """""""	"landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
regex = '^dq_src_input2.csv.$'
report = (
    ValidationOrchestrator(spark)
    .for_files(landing_path,regex)
    . check_file_header_presence().build_report()
)"
Category	Requirement Name	Function	Explanation	Code_Example
DATA	CHECK EXPECTED VALUES	check_expected_values( column: str, expected_values: list, case_insensitive: bool = True )	"    """"""
    Validates that a specified column in multiple DataFrames contains
    only values from a given list of expected values.

    This check iterates over each file/DataFrame pair and verifies:
    - The target column exists
    - All values in the column belong to the allowed expected values list
    - Comparison can be performed in a case-insensitive manner (optional)

    Parameters:
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
    """""""	"SOURCE_FILES_PATH = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
regex             = '^dq_src_input.*$'

# df_list = [df2,df3]
report = (
    ValidationOrchestrator(spark)
    .for_files(SOURCE_FILES_PATH,regex)
    .check_expected_values('Pass',['true', 'false'])
    .build_report()
)"
DATA	CHECK INVALID VALUES	check_invalid_values( column: str, invalid_values: list, case_insensitive: bool = True)	"            """"""
            Validates that a column does not contain any disallowed (invalid) values.

            The check verifies values in the specified column against a provided
            list of invalid values. Comparison can be performed in a
            case-insensitive or case-sensitive manner.

            PASS if ALL files/DataFrame:
            - Contain the specified column
            - And do not contain any values listed in invalid_values

            FAIL if ANY file:
            - Is missing the specified column
            - Or contains one or more invalid values in that column
            """""""	"SOURCE_FILES_PATH = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
regex             = '^dq_src_input.*$'

# df_list = [df2,df3]
report = (
    ValidationOrchestrator(spark)
    .for_files(SOURCE_FILES_PATH,regex)
    .check_invalid_values('name',['Amit','Anita','Karan','Neha'],True)
    .build_report()
)"
DATA	NO_NEGATIVE_VALUES_CHECK [NEW]	check_no_negative_values( column: str )	"        """"""
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
        """""""	"SOURCE_FILES_PATH = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
regex             = '^dq_src_input.*$'

# df_list = [df2,df3]
report = (
    ValidationOrchestrator(spark)
    .for_files(SOURCE_FILES_PATH,regex)
   .check_no_negative_values('marks')
    .build_report()
)"
DATA	STRING_LENGTH_CHECK [NEW]	 check_string_length( column: str, min_len: int,  max_len: int ) 	"        """"""
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
        """""""	"SOURCE_FILES_PATH = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
regex             = '^dq_src_input.*$'

# df_list = [df2,df3]
report = (
    ValidationOrchestrator(spark)
    .for_files(SOURCE_FILES_PATH,regex)
    .check_string_length('Date_of_Birth',10,10)
    .build_report()
)"
DATA	NO_WHITESPACE_CHECK [NEW]	check_no_whitespace(column: str )	"        """"""
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
        """""""	"SOURCE_FILES_PATH = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
regex             = '^dq_src_input.*$'

# df_list = [df2,df3]
report = (
    ValidationOrchestrator(spark)
    .for_files(SOURCE_FILES_PATH,regex)
   .check_no_whitespace('name')
    .build_report()
)"
DATA	CHECK DATE_FORMAT	check_date_format(column: str, date_format: str)	"        """"""
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
        """""""	"SOURCE_FILES_PATH = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
regex             = '^dq_src_input.*$'

# df_list = [df2,df3]
report = (
    ValidationOrchestrator(spark)
    .for_files(SOURCE_FILES_PATH,regex)
    .check_date_format('date_of_Birth','dd/mm/yyyy')
    .build_report()
)"
DATA	ROW_COUNT_CHECK	check_row_count( min_count: int = None,  max_count: int  = None)	"        """"""
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
        """""""	"SOURCE_FILES_PATH = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
regex             = '^dq_src_input.*$'

# df_list = [df2,df3]
report = (
    ValidationOrchestrator(spark)
    .for_files(SOURCE_FILES_PATH,regex)
    .check_row_count(7,7)
    .build_report()
)"
DATA	COMPLETENESS_RATIO_CHECK [NEW]	check_completeness_ratio( threshold_pct: int/float )	"        """"""
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
        """""""	"SOURCE_FILES_PATH = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
regex             = '^dq_src_input.*$'

# df_list = [df2,df3]
report = (
    ValidationOrchestrator(spark)
    .for_files(SOURCE_FILES_PATH,regex)
    .check_completeness_ratio(80)
    .build_report()
)"
DATA	UNIQUE_CHECK	check_unique(key_columns: list  )	"        """"""
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
        """""""	"SOURCE_FILES_PATH = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
regex             = '^dq_src_input.*$'

# df_list = [df2,df3]
report = (
    ValidationOrchestrator(spark)
    .for_files(SOURCE_FILES_PATH,regex)
    .check_unique(['Marks','Name'])
    .build_report()
)"
DATA	KPI_AGGREGATE_CHECK	check_kpi_aggregate(  expr: str, min_val, max_val )	"        """"""
        Validates that the result of a KPI aggregate expression falls within
        a specified numeric range for multiple DataFrames.

        This check iterates over each file/DataFrame pair and verifies:
        - The aggregate expression is a valid, non-empty Spark SQL expression
        - The expression can be successfully evaluated on the DataFrame
        - The resulting aggregate value is non-null
        - The aggregate value lies within the inclusive range [min_val, max_val]

        Parameters:

            expr (str):
                Spark SQL aggregate expression to evaluate (e.g. ""sum(revenue)"",
                ""avg(score)"", ""count(*)"").
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
        """""""	"SOURCE_FILES_PATH = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
regex             = '^dq_src_input.*$'

# df_list = [df2,df3]
report = (
    ValidationOrchestrator(spark)
    .for_files(SOURCE_FILES_PATH,regex)
    .check_kpi_aggregate(""sum(markS)"",7,7)
    .build_report()
)"
DATA	VALUE_RANGE_CHECK	 check_value_range(column_name: str,  min_val,  max_val )	"        """"""
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
        """""""	"SOURCE_FILES_PATH = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
regex             = '^dq_src_input.*$'

# df_list = [df2,df3]
report = (
    ValidationOrchestrator(spark)
    .for_files(SOURCE_FILES_PATH,regex)
   .check_value_range('marks',00,66)
    .build_report()
)"
DATA	REGEX_FORMAT_CHECK	check_regex_format(column_name: str, pattern: str )	"        """"""
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
        """""""	"SOURCE_FILES_PATH = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
regex             = '^dq_src_input.*$'
pattern = r""^(0[1-9]|[12][0-9]|3[01])/(0[1-9]|1[0-2])/\d{4}$""
# df_list = [df2,df3]
report = (
    ValidationOrchestrator(spark)
    .for_files(SOURCE_FILES_PATH,regex)
   .check_regex_format('Date_of_BirtH',pattern)
    .build_report()
)"
DATA	NO_FUTURE_DATE_CHECK	check_no_future_date( date_column: str,  date_format: str )	"
    """"""
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
    """""""	"SOURCE_FILES_PATH = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
regex             = '^dq_src_input.*$'

# df_list = [df2,df3]
report = (
    ValidationOrchestrator(spark)
    .for_files(SOURCE_FILES_PATH,regex)
    .check_no_future_date('Date_of_BirtH','dd/mm/yyyy')
    .build_report()
)"
DATA	DUPLICATE_ROWS_CHECK	check_duplicate_rows(  ) 	"        """"""
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
        """""""	"SOURCE_FILES_PATH = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
regex             = '^dq_src_input.*$'

# df_list = [df2,df3]
report = (
    ValidationOrchestrator(spark)
    .for_files(SOURCE_FILES_PATH,regex)
    .check_duplicate_rows()
    .build_report()
)"
<img width="481" height="24728" alt="image" src="https://github.com/user-attachments/assets/92d7d1e9-3c91-434e-aa9d-ab2379d8ae23" />
