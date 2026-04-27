class SdlValidationOrchestrator(ValidationOrchestrator,ValidationBuilder):
    
    # def __init__(self, spark: SparkSession, file_path: str, regex: str, df_list:list):
    #     super().__init__(spark, file_path, regex, df_list)
        # self.keywords = keywords
    def __init__(self, spark: SparkSession):
        self.spark = spark

    """
    Read the csv file and store in the dataframe
    """
    def _read_csv(self, start_cell):
        try:
            return (
                self.spark.read
                .option("header", "False")
                .option("inferSchema", "False")
                .csv(self.file_path)
                .withColumn("_row_id", monotonically_increasing_id())
                .filter(col("_row_id") >= start_cell["row_idx"])
            )
        except Exception as e:
            dbutils.notebook.exit(f"ERROR in _read_csv : {str(e)}")   

    """
    Read the exact cell and return the value
    """
    def read_cell(self, col_idx, row_idx):

        try:
            start_cell = {"row_idx": 0, "col_idx": col_idx}
            df = self._read_csv(start_cell)
            if df is None:
                return None
            
            cols = df.columns
            if col_idx >= len(cols):
                return None

            row = (
                df.filter(col("_row_id") == row_idx - 1)
                .select(cols[col_idx])
                .limit(1)
                .collect()
            )
            return row[0][0] if row else None
        
        except Exception as e:
            raise Exception(f'Failed at read_cell :- {str(e)}') 

    """
    Find the cell based on the given keywords
    """
    def find_start_cell(self, keywords, max_rows=10, max_cols=4):
        try:
            for col_idx in range(max_cols):
                for row_idx in range(1, max_rows + 1):

                    curr = self.read_cell(col_idx, row_idx)
                    if curr is None:
                        continue

                    if not any(
                        str(curr).strip().lower().startswith(p)
                        for p in keywords
                    ):
                        continue

                    below = self.read_cell(col_idx, row_idx + 1)
                    if below not in (None, "", "null"):
                        continue

                    right = self.read_cell(col_idx + 1, row_idx)
                    if right is None:
                        continue

                    right_string = str(right).strip().lower()
                    if not right_string.startswith(("rev", "adv", "wpp")):
                        continue

                    return {
                        "row_idx": row_idx - 1,  # 0-based
                        "col_idx": col_idx
                    }

            return ValidationResult(
                "FIND_START_CELL",
                "FAIL",
                f"Start of the cell could not be find on given keywords: {str(e)}"
            )

        except ValueError as e:
            # self.write_log("ERROR", f"Start_Cell failure: {str(e)}")
            return ValidationResult(
                "FIND_START_CELL",
                "FAIL",
                f"Unable to validate invalid values: {str(e)}"
            )

SOURCE_FILES_PATH = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_COE/Private/Inbox/archive/SDL/A4T/ingest_dt=2026-04-19/'

regex = '^ec_model_new\.csv$'

report = (
    SdlValidationOrchestrator(spark)
    .for_files(SOURCE_FILES_PATH,regex)
    .find_start_cell()
    .build_report()
)
