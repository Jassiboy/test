%md
How to Use it?
First you need to run following
%run ./nb_validation_orchestrator.

After that create the following object based on your need.
There are 2 ways to do it.

1.Using for_dataFrame ,here you only get schema checks and File checks only.You need pass the list of dataFrames into this funtcion. NOte:- Make sure your all dataframe contain a column name _source_path_, this columnn will segregrated the one df from other.You can pass any name also to keep the track.Refer the example below.
report = (
    ValidationOrchestrator(spark)
    .for_dataframes(df_list)
    .check_required_columns(['name'],True)
    .check_data_dtype({'Name':'string','Id':'Integer','Marks':'integer','Date_of_Birth':'date'},True)
    .check_column_order(['Id','NaMe', 'Marks', 'Date_of_Birth','Pass' ])
    .check_null(['id'])
    .build_report()
)

2. using for_datafiles, here you will get all the checks (FILE, SCHEMA,DATA). But you need to pass landing_path and regex of that file.Now it will valiadte every file present at that loaction matching the regex.
FOR SCHEMA and DATA checks ,though the code haveused basic read function and derive the dataframe based on that , but you can also make your own list of dataframes and pass the list to the function .for_files(Landing_path,regex,df_list).

Landing_path = 'abfss://datalake@stadslabsdl001.dfs.core.windows.net/COMN_ADS/Private/f6180854_test_dq/'
regex = '^dq_src_input2.csv.$'
# df_list = [2,3]
report = (
    ValidationOrchestrator(spark)
    .for_files(Landing_path,regex)
    .check_file_count()
    .check_expected_file_count(expected = 1)
    .check_size(min_size = 0 ,max_size = 100000)
    .build_report()
)

---> You can aslo raise the report on Failure using following
try:
  report.raise_if_failed()
     print("Overall Status : passed")
except Exception as e:
     dbutils.notebook.exit(f"Pipeline Failed: {str(e)}")


Note:- YOu can use any number of funtions based on your need, but it comes with some trade-offs, the more function you use, the more compute will be utilised.
