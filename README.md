target_dataset_layer		source_table_name		target_table_name			opco		ingestion_mode:string		load_frequency:string
											
acct		Base_table_1, data_mart_table2		fxg_acct_bill_dly_hist			GROUND		Incremental		daily
		Base_table_1, data_mart_table3		fxg_acct_bill_mth_hist			GROUND		Incremental		daily
											
		ds_fcst_de_schema.CUST_SE_SEG_MAP,ds_fcst_de_schema.CUST_CE_SEG_MAP		ds_fcst_de_schema.fxg_fac_enti_bill_dly_hist			GROUND		Incremental		daily
facility		ds_fcst_de_schema.CUST_SE_SEG_MAP,ds_fcst_de_schema.CUST_CE_SEG_MAP,ds_fcst_de_schema.CDL_MTR_GROUP_DEFINITION		ds_fcst_de_schema.fxg_fac_enti_bill_mth_hist			GROUND		Incremental		daily
											
		ds_fcst_de_schema.fxg_fac_enti_bill_dly_hist		ds_fcst_de_schema.fxg_ctry_enti_bill_mth_hist			GROUND		Incremental		daily
country		ds_fcst_de_schema.fxg_fac_enti_bill_mth_hist		ds_fcst_de_schema.fxg_ctry_enti_bill_dly_hist			GROUND		Incremental		daily
											
											
global		ds_fcst_de_schema.fxg_ctry_enti_bill_mth_hist		ds_fcst_de_schema.fxg_glbl_enti_bill_dly_hist			GROUND		Incremental		daily
		ds_fcst_de_schema.fxg_ctry_enti_bill_dly_hist		ds_fcst_de_schema.fxg_glbl_enti_bill_mth_hist			GROUND		Incremental		daily
											
<img width="1214" height="301" alt="image" src="https://github.com/user-attachments/assets/cbef492d-4f0d-4885-9a4b-62c77c53e6f4" />

table_id:string
source_system:string
source_table_name:string
source_filter:string
target_system:string
target_table_name:string
target_dataset_layer:string
opco:string
ingestion_mode:string
load_frequency:string
watermark_column:string
last_watermark_value:timestamp
last_successful_run:date
partition_col:string
data_quality_rules:string
dq_threshold_pct:double
retry_max_attempts:integer
retry_initial_wait_sec:integer
retry_backoff_multiplier:double
retry_max_wait_sec:integer
alert_config:string
is_active:boolean
created_at:timestamp
<img width="65" height="461" alt="image" src="https://github.com/user-attachments/assets/184e4472-b1df-46b9-86c2-6c80c0591a0e" />

