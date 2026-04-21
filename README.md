# How to Use the Validation Orchestrator

## Setup

Before anything else, run the orchestrator notebook:

```python
%run ./nb_validation_orchestrator
```

---

## Two Ways to Use It

| # | Method | Function | Available Checks | When to Use |
|---|--------|----------|-----------------|-------------|
| 1 | **DataFrame-based** | `for_dataframes(df_list)` | Schema & Data checks only | When you already have DataFrames loaded in memory |
| 2 | **File-based** | `for_files(landing_path, regex)` | FILE + Schema + Data checks (all) | When validating raw files directly from a storage path |

---

## Method 1 — `for_dataframes()`

Pass a list of DataFrames directly into the orchestrator.

> ⚠️ **Important:** Each DataFrame **must** contain a column named `_source_path_`. This column is used to distinguish one DataFrame from another within the list. You may use any meaningful name to keep track of the source.

```python
report = (
    ValidationOrchestrator(spark)
    .for_dataframes(df_list)
    .check_required_columns(['name'], True)
    .check_data_dtype({'Name': 'string', 'Id': 'Integer', 'Marks': 'integer', 'Date_of_Birth': 'date'}, True)
    .check_column_order(['Id', 'NaMe', 'Marks', 'Date_of_Birth', 'Pass'])
    .check_null(['id'])
    .build_report()
)
```

---

## Method 2 — `for_files()`

Pass the landing path and a regex pattern. The orchestrator will validate **every file** at that location whose name matches the regex.

> 💡 **Tip:** For Schema and Data checks, the orchestrator internally reads files and derives DataFrames automatically. However, you can also supply your own list of DataFrames by passing it as a third argument: `.for_files(landing_path, regex, df_list)`

```python
landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
regex        = '^dq_src_input2.csv.$'
# df_list    = [df2, df3]   # optional — pass your own DataFrames if needed

report = (
    ValidationOrchestrator(spark)
    .for_files(landing_path, regex)
    .check_file_count()
    .check_expected_file_count(expected=1)
    .check_size(min_size=0, max_size=100000)
    .build_report()
)
```

| Point | Detail |
|-------|--------|
| **Custom DataFrames** | You can optionally pass a pre-built DataFrame list as a third argument instead of relying on the default file reader: `.for_files(landing_path, regex, df_list)` |
| **Regex Precision** | The more specific and robust your regex, the more accurate the file matching. Use uppercase/lowercase patterns in the regex itself to control case sensitivity. |

---

## Raising an Alert on Failure

To stop pipeline execution when validation fails, use `raise_if_failed()`:

```python
try:
    report.raise_if_failed()
    print("Overall Status: Passed")
except Exception as e:
    dbutils.notebook.exit(f"Pipeline Failed: {str(e)}")
```

---

## Key Considerations

| Point | Detail |
|-------|--------|
| **Flexibility** | You can chain any number of check functions based on your validation requirements. |
| **Performance Trade-off** | Each additional check consumes more compute resources. Use only the checks that are necessary for your use case. |
| **Source Tracking** | In `for_dataframes()` mode, ensure the `_source_path_` column is present in every DataFrame so results can be traced back to the correct source. |
