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
