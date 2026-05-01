# Databricks notebook source
# DBTITLE 1,Run nb_file_checks
# MAGIC %run ./nb_all_checks

# COMMAND ----------

# DBTITLE 1,Run nb_schema_checks
class ValidationError(Exception):
    """Custom exception that displays only the error message (no traceback)."""
    def _render_traceback_(self):
        return [f'\033[0;31mValidationError: {self}\033[0m']

@dataclass
class ValidationReport:

    results: List[ValidationResult] = field(default_factory=list)
    def add(self, result: ValidationResult):
        self.results.append(result)

    def is_ok(self) -> bool:
        return all(r.status == "PASS" for r in self.results)

    # def print_summary(self):
    #     print("=" * 80)
    #     print(f"|| VALIDATION REPORT ||")
    #     print("-" * 80)
    #     for r in self.results:
    #         print(r)
    #     print("-" * 80)
    #     print("\u2705 ALL SELECTED CHECKS PASSED" if self.is_ok() else "\u274c VALIDATION FAILED")
    #     print("=" * 80)
# ---------------------------------------------
    def print_summary(self):
        TABLE_WIDTH = 86
        CHECK_COL = 28
        STATUS_COL = 8
        DETAILS_COL = 50

        def truncate(text, width):
            return text if len(text) <= width else text[: width - 3] + "..."

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
            details = truncate(r.message, DETAILS_COL)
            print(f"| {r.check_name.ljust(CHECK_COL)} | "
                f"{r.status.ljust(STATUS_COL)} | "
                f"{details.ljust(DETAILS_COL)} |")
                # f"{r.message.ljust(DETAILS_COL)} |")

        print()
        print("-" * TABLE_WIDTH)

        if self.is_ok():
            print("OVERALL RESULT : PASS (All checks successful)")
        else:
            print("OVERALL RESULT : FAIL (One or more checks failed)")

        print("=" * TABLE_WIDTH)

    def raise_if_failed(self):
        if not self.is_ok():
            failed = [r for r in self.results if r.status == "FAIL"]
            msg = "; ".join([f"{r.check_name}: {r.message}" for r in failed])
            raise ValidationError(f"Validation Failed: {msg}")

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

# =========================================================
# BUILDER (METHOD CHAINING)
# =========================================================

class ValidationBuilder:
    def __init__(self, spark: SparkSession, file_path: str, regex: str, base_transform: Optional[Callable[[str], DataFrame]] = None):
        self.spark = spark
        self.file_path = file_path
        self.regex = regex
        self.base_transform = base_transform  
        # self.df_list = df_list
        self._df_cache = {}
        # self.delimiter = delimiter

        self.file = FileChecks(spark)
        self.schema = SchemaChecks(spark)
        self.data  = DataChecks(spark)

        self._steps: List[Callable[[ValidationReport], None]] = []
        self._df: Optional[DataFrame] = None

        # ── Plugin slot ──────────────────────────────────────────────────────
        # Holds the project-supplied custom checks instance (or None).
        self._plugin = None

    # =========================================================================
    # ★ NEW — register a custom checks plugin
    # =========================================================================
    def register_checks(self, plugin_instance):
        """
        Attach a project-level checks object to the builder.

        The plugin can expose any methods it likes.  After registration those
        methods become callable on the builder directly via the magic
        __getattr__ below — no boilerplate wiring required.

        The builder injects two context attributes into the plugin so its
        methods can reach Spark and the loaded DataFrames:
            plugin.spark      → active SparkSession
            plugin.file_path  → resolved file path
            plugin.regex           → regex string
            plugin.base_transform  → the active base transform (or None)  [NEW-03]
            plugin._get_file_dfs   → callable returning {path: DataFrame}
        """
        self._plugin = plugin_instance
        # Inject builder context so the plugin can use it
        plugin_instance.spark         = self.spark
        plugin_instance.file_path     = self.file_path
        plugin_instance.regex         = self.regex
        plugin_instance.base_transform = self.base_transform   # [NEW-03]
        plugin_instance._get_file_dfs  = self._get_file_dfs
        return self

    # =========================================================================
    # ★ NEW — magic routing: unknown method calls → plugin
    # =========================================================================
    def __getattr__(self, name: str):
        """
        Called only when normal attribute lookup fails (i.e. the method is not
        defined on ValidationBuilder itself).

        If a plugin is registered and it has the requested method, return a
        wrapper that:
          1. Calls plugin.method_name(*args, **kwargs)  → ValidationResult
          2. Appends it as a step in self._steps
          3. Returns self  (keeps the fluent chain alive)

        This means the builder automatically gains every method defined in the
        plugin — no manual wrapping, no extra classes.
        """
        plugin = self.__dict__.get("_plugin")          # avoid infinite recursion
        if plugin is not None and hasattr(plugin, name):
            def _chainable(*args, **kwargs):
                # Snapshot args now to avoid late-binding closure issues
                _name   = name
                _args   = args
                _kwargs = kwargs
                self._steps.append(
                    lambda report: report.add(
                        getattr(self._plugin, _name)(*_args, **_kwargs)
                    )
                )
                return self                              # keep chain alive
            return _chainable

        raise AttributeError(
            f"'ValidationBuilder' has no check '{name}'. "
            f"Define it in your custom checks class and pass it to with_checks()."
        )

    # ---------------------------- 
    def _read_file(self, path: str, file_ext:str):
        """
        Reads only top N rows from supported file types:
        csv, parquet, json, text, xls, xlsx
        """
        try:
            # display('READING')
            if file_ext == "csv" :
                df = (
                    self.spark.read
                    .option("header", True)
                    .option("inferSchema", True)
                    .option("quote", '"')
                    .option("escape", '"')
                    .option("multiLine", "true")
                    .csv(path)
                )

            elif file_ext == "parquet":
                df = self.spark.read.parquet(path)

            elif file_ext == "json":
                df = (
                    self.spark.read
                    .option("multiLine", "true")
                    .json(path)
                )

            elif file_ext == "text":
                df = self.spark.read.text(path)

            elif file_ext in ["xls", "xlsx"]:
                df = (
                    self.spark.read.format("com.crealytics.spark.excel")
                    .option("header", True)
                    .option("inferSchema", True)
                    .option("dataAddress", "'Sheet1'!")  # default sheet
                    .load(path)
                )

            else:
                raise ValueError(f"Unsupported file format: {file_ext}")

            return df

        except Exception as e:
            raise ValidationError(f"Read_files function has failed: {str(e)}")


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
            files = dbutils.fs.ls(self.file_path)
            matched_files = [f for f in files if pattern.match(f.name)]


            # if self.df_list is not None and len(matched_files) != len(self.df_list):
            #     raise ValueError("matched_files and df_list length mismatch")

            # if self.df_list is not None:
            #     received_files_frm_df = []
            #     available_files = []
                
            #     for df in self.df_list:
            #         if "_source_path_" not in df.columns:
            #             raise ValueError("Please add a new column _source_path_ for better validation")

            #         paths = df.select("_source_path_").distinct().collect()
            #         if len(paths) != 1:
            #             raise ValueError("Each DataFrame must have exactly one source path")

            #         file_path = df.select('_source_path_').first()[0]
            #         received_files_frm_df.append(file_path)
                
            #     for f in matched_files:
            #         available_files.append(f.path)

            #     if sorted(received_files_frm_df) != sorted(available_files):
            #         raise ValueError(
            #             f"File mismatch: received {received_files_frm_df} from DataFrames "
            #             f"but found {available_files} in path"
            #         )
    
            #     for df in self.df_list:
            #         file_path = df.select('_source_path_').first()[0]
            #         df = df.drop(col('_source_path_'))
            #         df = df.cache()
            #         df.count()
            #         self._df_cache[file_path] = df
            # else:
            #     for file_path in matched_files:
            #         if file_path.isDir():
            #             file_ext = 'csv'
            #         else:
            #             _, file_ext = os.path.splitext(file_path.name)
            #             file_ext = file_ext[1:] if file_ext else None
            #         # print(f"path={f.path}, ext={ext}")
            #         df = self._read_file(file_path.path , file_ext)
            #         df = df.cache()
            #         df.count()
            #         self._df_cache[file_path.path] = df

            for f in matched_files:


                # df = self._read_file(f.path, file_ext)

                # [NEW-01] Apply base transform if provided
                if self.base_transform is not None:
                    df = self.base_transform(f.path)
                else:
                    _, ext = os.path.splitext(f.name)
                    file_ext = ext[1:].lower() if ext else "csv"
                    df = self._read_file(f.path, file_ext)

                df = df.cache()
                df.count()                          # materialise the cache
                self._df_cache[f.path] = df

            return self._df_cache
        
        except Exception as e:
            raise ValidationError(f"_get_file_dfs function has failed: {str(e)}")

    # ---------------------------
    # Chainable FILE checks
    # ---------------------------

    
    def check_file_count(self):
        self._steps.append(
            lambda report: report.add(self.file.check_file_count(self.file_path, self.regex))
        )
        return self
    
    def check_expected_file_count(self, expected:int ):
        self._steps.append(
            lambda report: report.add(self.file.check_expected_file_count(self.file_path, self.regex , expected))
        )
        return self
    
    def check_size(self, min_size:int , max_size:int ):
        self._steps.append(
            lambda report: report.add(self.file.check_size(self.file_path, self.regex , min_size , max_size))
        )
        return self
    
    def check_duplicate_file(self):
        self._steps.append(
            lambda report: report.add(self.file.check_duplicate_file(self.file_path, self.regex))
        )
        return self
    
    def check_file_already_archived(self,archive_path:str):
        self._steps.append(
            lambda report: report.add(self.file.check_file_already_archived(self.file_path, self.regex , archive_path))
        )
        return self

    def check_file_freshness(self, max_age_hours:int ):
        self._steps.append(
            lambda report: report.add(self.file.check_file_freshness(self.file_path, self.regex , max_age_hours))
        )
        return self
    
    def check_file_extension(self, allowed_ext:list ):
        self._steps.append(
            lambda report: report.add(self.file.check_file_extension(self.file_path, self.regex , allowed_ext))
        )
        return self
    
    def check_file_compression(self, allowed_compressions:list ):
        self._steps.append(
            lambda report: report.add(self.file.check_file_compression(self.file_path, self.regex , allowed_compressions))
        )
        return self
    
    # ---------------------------
    # Chainable SCHEMA checks
    # ---------------------------

    def check_required_columns(self, required_cols: List[str], allow_extra: bool = False):
        self._steps.append(
            lambda report: report.add(self.schema.check_required_columns(self._get_file_dfs(), required_cols, allow_extra))
        )
        return self
    
    def check_forbidden_columns(self, forbidden_cols: List[str]):
        self._steps.append(
            lambda report: report.add(self.schema.check_forbidden_columns(self._get_file_dfs(), forbidden_cols))
        )
        return self
    
    def check_data_dtype(self, type_map: dict,  allow_extra: bool = False):
        self._steps.append(
            lambda report: report.add(self.schema.check_data_dtype(self._get_file_dfs(), type_map, allow_extra))
        )
        return self
    
    def check_column_order(self, expected_order: List[str]):
        self._steps.append(
            lambda report: report.add(self.schema.check_column_order(self._get_file_dfs(), expected_order))
        )
        return self
    
    def check_column_count(self, min_count: int = None, max_count: int = None ):
        self._steps.append(
            lambda report: report.add(self.schema.check_column_count(self._get_file_dfs(), min_count , max_count))
        )
        return self
    
    def check_column_sensitivity(self, col: List[str]):
        self._steps.append(
            lambda report: report.add(self.schema.check_column_sensitivity(self._get_file_dfs(), col))
        )
        return self
    

    def check_null(self, col: List[str]):
        self._steps.append(
            lambda report: report.add(self.schema.check_null(self._get_file_dfs(), col))
        )
        return self

    def check_file_header_presence(self ):
        self._steps.append(
            lambda report: report.add(self.schema.check_file_header_presence(self._get_file_dfs()))
        )
        return self
    
    # ---------------------------
    # Chainable DATA checks
    # ---------------------------

    def check_invalid_values(self, column: str, invalid_values: list, case_insensitive: bool = True ):
        self._steps.append(
            lambda report: report.add(self.data.check_invalid_values(self._get_file_dfs(), column , invalid_values,case_insensitive))
        )
        return self
    
    def check_expected_values(self, column: str, expected_values: list, case_insensitive: bool = True ):
        self._steps.append(
            lambda report: report.add(self.data.check_expected_values(self._get_file_dfs(), column , expected_values,case_insensitive))
        )
        return self

    def check_date_format(self, column: str, date_format: str):
        self._steps.append(
            lambda report: report.add(self.data.check_date_format(self._get_file_dfs(), column , date_format))
        )
        return self
    
    def check_no_negative_values(self, column: str):
        self._steps.append(
            lambda report: report.add(self.data.check_no_negative_values(self._get_file_dfs(), column ))
        )
        return self
    
    def check_no_whitespace(self, column: str):
        self._steps.append(
            lambda report: report.add(self.data.check_no_whitespace(self._get_file_dfs(), column ))
        )
        return self
    
    def check_string_length(self, column: str, min_len: int,  max_len: int ):
        self._steps.append(
            lambda report: report.add(self.data.check_string_length(self._get_file_dfs(), column, min_len, max_len ))
        )
        return self
    
    def check_row_count(self, min_count: int = None,  max_count: int  = None):
        self._steps.append(
            lambda report: report.add(self.data.check_row_count(self._get_file_dfs(), min_count, max_count ))
        )
        return self
    
    def check_completeness_ratio(self, threshold_pct: float ):
        self._steps.append(
            lambda report: report.add(self.data.check_completeness_ratio(self._get_file_dfs(), threshold_pct ))
        )
        return self
    
    def check_unique(self, key_columns: list ):
        self._steps.append(
            lambda report: report.add(self.data.check_unique(self._get_file_dfs(), key_columns ))
        )
        return self
    
    def check_kpi_aggregate(self,   expr: str,  min_val,  max_val ):
        self._steps.append(
            lambda report: report.add(self.data.check_kpi_aggregate(self._get_file_dfs(), expr , min_val, max_val ))
        )
        return self
    
    def check_value_range(self,   column_name: str,  min_val,  max_val ):
        self._steps.append(
            lambda report: report.add(self.data.check_value_range(self._get_file_dfs(), column_name , min_val, max_val ))
        )
        return self
    
    def check_regex_format(self,   column_name: str, pattern: str ):
        self._steps.append(
            lambda report: report.add(self.data.check_regex_format(self._get_file_dfs(), column_name , pattern ))
        )
        return self
    
    def check_no_future_date(self, date_column: str,  date_format: str ):
        self._steps.append(
            lambda report: report.add(self.data.check_no_future_date(self._get_file_dfs(), date_column , date_format ))
        )
        return self
    
    def check_duplicate_rows(self ):
        self._steps.append(
            lambda report: report.add(self.data.check_duplicate_rows(self._get_file_dfs() ))
        )
        return self

    def add_custom_check(self, fn:Callable[[ValidationReport], None]):
        self._steps.append(fn)
        return self
# =================================================================

    # ---------------------------
    # Final execution
    # ---------------------------

    def build_report(self) -> ValidationReport:
        """
        Execute all queued steps and return the completed ValidationReport.
        The DataFrame cache is always unpersisted in the finally block — even
        if a check raises mid-run — to prevent Spark memory leaks.  [FIX-04]
        """
        try:
            report = ValidationReport()
            for step in self._steps:
                step(report)

            report.print_summary()
            return report
            # if self._df_cache:
            #     for df in self._df_cache.values():
            #         df.unpersist()
            #         # display('Unpersits')
            # return report
        except Exception as e:
            raise ValidationError(f"build_report function has failed: {str(e)}")
        finally:                                   # [FIX-04] always unpersist
            for df in self._df_cache.values():
                df.unpersist()
            self._df_cache.clear()

#========================================
#========================================

class ValidationBuilderChild(ValidationBuilder):
    def __init__(self, spark: SparkSession, df_list:list):
        self.spark = spark
        self.df_list = df_list
        self._df_cache = {}

        self.schema = SchemaChecks(spark)
        self.data  = DataChecks(spark)

        self._steps: List[Callable[[ValidationReport], None]] = []
        self._df: Optional[DataFrame] = None
        self._plugin    = None

    def _get_file_dfs(self) -> Dict[str, DataFrame]:

        try:
            if self.df_list is None:
                raise ValueError("There is no dataFrames available.")
        
            if self._df_cache:
                return self._df_cache


            if self.df_list is not None:
                received_files_frm_df = []
                
                for df in self.df_list:
                    if "_source_path_" not in df.columns:
                        raise ValueError("Please add a new column _source_path_ for better validation")

                    paths = df.select("_source_path_").distinct().collect()
                    if len(paths) != 1:
                        raise ValueError("Each DataFrame must have exactly one source path")

                    file_path = df.select('_source_path_').first()[0]
                    received_files_frm_df.append(file_path)
                    
                for df in self.df_list:
                    file_path = df.select('_source_path_').first()[0]
                    df = df.drop(col('_source_path_'))
                    df = df.cache()
                    # print('cached')
                    df.count()
                    self._df_cache[file_path] = df

            return self._df_cache
        
        except Exception as e:
            raise ValidationError(f"_get_file_dfs function from child class has failed: {str(e)}")



# COMMAND ----------

# =========================================================
# MAIN ORCHESTRATOR
# =========================================================

class ValidationOrchestrator:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._plugin = None          # ← holds project checks instance

    # =========================================================================
    # attch a custom checks plugin before calling for_files
    # =========================================================================
    def with_checks(self, plugin_instance):
        """
        Register a project-level custom checks object.

        Call this ONCE on the orchestrator; the plugin is automatically
        forwarded to every builder returned by for_files() / for_dataframes().

        Example
        -------
            report = (
                ValidationOrchestrator(spark)
                .with_checks(SdlChecks())          # ← register once
                .for_files(path, regex)
                .check_file_count()                # base check
                .find_start_cell(keywords=[...])   # custom check — just works
                .build_report()
            )
        """
        self._plugin = plugin_instance
        return self

    @staticmethod
    def _validate_regex(regex: str) -> tuple:
        if not regex or not regex.strip():
            return False, "Regex cannot be empty"

        if regex.strip() in {"*", "+", "?", "{", "}"}:
            return False, f"Invalid standalone regex operator: '{regex}'"

        if regex.strip() in {".*", "^.*$", ".+", "^.+$"}:
            return False, "Overly broad regex that matches everything is not allowed"

        # Require at least one literal character
        if not re.search(r"[A-Za-z0-9]", regex):
            return False, "Regex must contain at least one literal character"

        try:
            re.compile(regex)
        except re.error as e:
            return False, f"Invalid regex syntax: {str(e)}"

        return True, ""
    
    @staticmethod
    def _validate_required_params(file_path: str, regex: str) -> None:
        missing = [name for name, value in {"file_path": file_path, "regex": regex}.items() if value is None]
        if missing:
            raise ValidationError( f"FAIL: Required parameter(s) missing: {', '.join(missing)}")
        
        empty = [
            name
            for name, value in {"file_path": file_path, "regex": regex}.items()
            if not str(value).strip()
        ]
        if empty:
            raise ValidationError( f"FAIL: Parameter(s) cannot be empty: {', '.join(empty)}" )

    @staticmethod
    def _validate_file_path(file_path: str, regex: str) -> None:
        try:
            files = dbutils.fs.ls(file_path)
            pattern = re.compile(regex)
            matched_files = [f for f in files  if pattern.search(f.name)  ]
            
        except Exception as e:
            raise ValidationError( f"FAIL: Unable to access location '{file_path}': {e}" )
        if not files:
            raise ValidationError(f"FAIL: Mentioned location '{file_path}' is empty." )
        if not matched_files:
            raise ValidationError(f"FAIL: No matching file available for regex '{regex}' at path '{file_path}'." )

    @staticmethod
    def _validate_df_list( df_list: List[DataFrame]) -> Tuple[bool, str]:

        try:
            # ---------- LIST LEVEL VALIDATION ----------
            if df_list is None:
                return False, "df_list is None"

            if not isinstance(df_list, list):
                return False, f"Expected df_list to be list, got {type(df_list)}"

            if len(df_list) == 0:
                return False, "df_list is empty"


            # ---------- DATAFRAME LEVEL VALIDATION ----------
            for idx, df in enumerate(df_list):

                if df is None:
                    return False, f"DataFrame at index {idx} is None"

                if not isinstance(df, DataFrame):
                    return False, f"Item at index {idx} is not a PySpark DataFrame"
                
                if df.limit(1).count() == 0:          # [NEW-05] avoids full RDD materialisation
                    return False, f"DataFrame at index {idx} has zero rows"

                # # Zero rows check (safe + efficient)
                # if df.rdd.isEmpty():
                #     return False, f"DataFrame at index {idx} has zero rows"

                # Zero columns check
                if not df.columns:
                    return False, f"DataFrame at index {idx} has zero columns"

                # Duplicate column names
                if len(df.columns) != len(set(df.columns)):
                    return False, f"Duplicate column names in DataFrame at index {idx}"

            return True, "All PySpark DataFrames are valid"
        
        except Exception as e:
            return False, f"DataFrame Validation has failed"

# ── Entry points ──────────────────────────────────────────────────────────

    def for_files(
        self,
        file_path: str,
        regex: str,
        base_transform: Optional[Callable[[str], DataFrame]] = None, 
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
            is_valid, message = self._validate_regex(regex)
            if not is_valid:
                raise ValidationError(f"FAIL: {message}")
            # if df_list is not None:
            #     is_valid_df, msg_df = self._validate_df_list(df_list)
            #     if not is_valid_df:
            #         raise ValidationError(f"FAIL: {msg_df}")
            self._validate_file_path(file_path, regex)

            builder = ValidationBuilder(self.spark, file_path, regex, base_transform=base_transform)

            # ★ Auto-attach plugin if one was registered via with_checks()
            if self._plugin is not None:
                builder.register_checks(self._plugin)

            return builder
        except ValidationError:
            raise
        except Exception as e:
            raise ValidationError(f"for_files failed: {str(e)}")

    def for_dataframes(self, df_list) -> ValidationBuilderChild:
        try:
            is_valid, message = self._validate_df_list(df_list)
            if not is_valid:
                raise ValidationError(f"FAIL: {message}")

            builder = ValidationBuilderChild(self.spark, df_list=df_list)

            # ★ Auto-attach plugin if one was registered via with_checks()
            if self._plugin is not None:
                builder.register_checks(self._plugin)

            return builder
        except Exception as e:
            raise ValidationError(f"for_dataframes failed: {str(e)}")
    
    # def for_files(self, file_path: str, regex: str, df_list: Optional[List[DataFrame]] = None) -> ValidationBuilder:
    #     try:          
    #         self._validate_required_params(file_path,regex)
    #         is_valid, message = self._validate_regex(regex)
    #         is_valid_df_list = True
    #         if df_list is not None:
    #             is_valid_df_list, message = self._validate_df_list(df_list)

    #         if not is_valid or not is_valid_df_list :
    #             raise ValidationError(f"FAIL: {message}")

    #         self._validate_file_path(file_path,regex)

    #         return ValidationBuilder(self.spark, file_path, regex, df_list = df_list)
    #     except ValidationError:
    #         raise
    #     except Exception as e:
    #         raise ValidationError(f"For_files function has failed: {str(e)}")


    # def for_dataframes(self, df_list) -> ValidationBuilder:
        
    #     try:

    #         is_valid_df_list,message = self._validate_df_list(df_list)

    #         if not is_valid_df_list :
    #             raise ValidationError(f"FAIL: {message}")          
            
    #         builder =  ValidationBuilderChild(self.spark, df_list = df_list)

    #     except Exception as e:
    #         raise ValidationError(f"for_dataframes function has failed: {str(e)}")