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


====================

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, TimestampType, DateType
)
from pyspark.sql import functions as F
import json
import re

spark = SparkSession.builder.appName("cdl_table_config_loader").getOrCreate()

# ---------------------------------------------------------------------
# 1. Target schema — exactly matches cdl_table_config
# ---------------------------------------------------------------------
schema = StructType([
    StructField("table_id",               StringType(),    False),
    StructField("source_system",          StringType(),    True),
    StructField("source_table_name",      StringType(),    True),   # comma-separated for multi-source
    StructField("source_filter",          StringType(),    True),
    StructField("target_system",          StringType(),    True),
    StructField("target_table_name",      StringType(),    True),
    StructField("target_dataset_layer",   StringType(),    True),
    StructField("opco",                   StringType(),    True),
    StructField("ingestion_mode",         StringType(),    True),
    StructField("load_frequency",         StringType(),    True),
    StructField("watermark_column",       StringType(),    True),
    StructField("last_watermark_value",   TimestampType(), True),
    StructField("last_successful_run",    DateType(),      True),
    StructField("partition_col",          StringType(),    True),
    StructField("data_quality_rules",     StringType(),    True),   # JSON string
    StructField("dq_threshold_pct",       DoubleType(),    True),
    StructField("retry_max_attempts",     IntegerType(),   True),
    StructField("retry_initial_wait_sec", IntegerType(),   True),
    StructField("retry_backoff_multiplier", DoubleType(),  True),
    StructField("retry_max_wait_sec",     IntegerType(),   True),
    StructField("alert_config",           StringType(),    True),   # JSON string
    StructField("is_active",              BooleanType(),   True),
    StructField("created_at",             TimestampType(), True),
])

# ---------------------------------------------------------------------
# 2. Reusable defaults (DQ rules + alerting) — parameterized by target col
# ---------------------------------------------------------------------
def default_dq_rules(pk_column: str = "account_id"):
    return json.dumps([
        {"rule_id": f"not_null_{pk_column}", "rule_type": "NOT_NULL",
         "target_column": pk_column, "rule_expression": f"{pk_column} IS NOT NULL",
         "severity": "CRITICAL", "error_threshold_pct": 0.0},
        {"rule_id": "row_count_min", "rule_type": "ROW_COUNT",
         "target_column": None, "rule_expression": json.dumps({"min_rows": 1}),
         "severity": "CRITICAL", "error_threshold_pct": 0.0},
    ])

def default_alert_config():
    return json.dumps([
        {"channel": "EMAIL",
         "recipients": "cdl-alerts@company.com,data-eng-oncall@company.com",
         "on_status": ["FAILED", "HALTED"]},
        {"channel": "TEAMS",
         "recipients": "https://company.webhook.office.com/webhookb2/cdl-alerts",
         "on_status": ["FAILED"]},
    ])

def make_table_id(opco: str, layer: str, target_table_name: str) -> str:
    """Deterministic, idempotent ID — safe to re-run the loader."""
    short_name = target_table_name.split(".")[-1]          # strip schema prefix
    raw = f"{opco}_{layer}_{short_name}".lower()
    return re.sub(r"[^a-z0-9_]", "_", raw)

# ---------------------------------------------------------------------
# 3. Row definitions — matches your sample sheet
#    (source_table_name kept comma-separated exactly as provided)
# ---------------------------------------------------------------------
raw_rows = [
    dict(layer="acct", sources="Base_table_1,data_mart_table2",
         target="fxg_acct_bill_dly_hist", freq="daily", pk="account_id"),
    dict(layer="acct", sources="Base_table_1,data_mart_table3",
         target="fxg_acct_bill_mth_hist", freq="daily", pk="account_id"),

    dict(layer="facility",
         sources="ds_fcst_de_schema.CUST_SE_SEG_MAP,ds_fcst_de_schema.CUST_CE_SEG_MAP",
         target="ds_fcst_de_schema.fxg_fac_enti_bill_dly_hist", freq="daily", pk="facility_id"),
    dict(layer="facility",
         sources="ds_fcst_de_schema.CUST_SE_SEG_MAP,ds_fcst_de_schema.CUST_CE_SEG_MAP,ds_fcst_de_schema.CDL_MTR_GROUP_DEFINITION",
         target="ds_fcst_de_schema.fxg_fac_enti_bill_mth_hist", freq="daily", pk="facility_id"),

    dict(layer="country",
         sources="ds_fcst_de_schema.fxg_fac_enti_bill_dly_hist",
         target="ds_fcst_de_schema.fxg_ctry_enti_bill_mth_hist", freq="daily", pk="country_code"),
    dict(layer="country",
         sources="ds_fcst_de_schema.fxg_fac_enti_bill_mth_hist",
         target="ds_fcst_de_schema.fxg_ctry_enti_bill_dly_hist", freq="daily", pk="country_code"),

    dict(layer="global",
         sources="ds_fcst_de_schema.fxg_ctry_enti_bill_mth_hist",
         target="ds_fcst_de_schema.fxg_glbl_enti_bill_dly_hist", freq="daily", pk="global_entity_id"),
    dict(layer="global",
         sources="ds_fcst_de_schema.fxg_ctry_enti_bill_dly_hist",
         target="ds_fcst_de_schema.fxg_glbl_enti_bill_mth_hist", freq="daily", pk="global_entity_id"),
]

# ---------------------------------------------------------------------
# 4. Build full rows with defaults filled in
# ---------------------------------------------------------------------
now_ts = spark.sql("SELECT current_timestamp() AS ts").collect()[0]["ts"]

built_rows = []
for r in raw_rows:
    built_rows.append((
        make_table_id("GROUND", r["layer"], r["target"]),   # table_id
        None,                                                # source_system (internal chain -> null; set explicit system if raw external source)
        r["sources"],                                        # source_table_name (comma-separated)
        None,                                                 # source_filter
        "DELTA_LAKE",                                          # target_system
        r["target"],                                            # target_table_name
        r["layer"],                                              # target_dataset_layer
        "GROUND",                                                 # opco
        "Incremental",                                             # ingestion_mode
        r["freq"],                                                  # load_frequency
        "last_modified_ts",                                          # watermark_column
        None,                                                         # last_watermark_value
        None,                                                          # last_successful_run
        "load_date",                                                    # partition_col
        default_dq_rules(r["pk"]),                                       # data_quality_rules
        95.0,                                                             # dq_threshold_pct
        3, 30, 2.0, 300,                                                   # retry_*
        default_alert_config(),                                             # alert_config
        True,                                                                 # is_active
        now_ts,                                                                # created_at
    ))

df = spark.createDataFrame(built_rows, schema=schema)

# ---------------------------------------------------------------------
# 5. Upsert (idempotent) into cdl_table_config using Delta MERGE
# ---------------------------------------------------------------------
from delta.tables import DeltaTable

target_path = "cdl_db.cdl_table_config"   # or a path if not using Hive metastore

if DeltaTable.isDeltaTable(spark, target_path):
    dt = DeltaTable.forName(spark, target_path)
    (dt.alias("t")
       .merge(df.alias("s"), "t.table_id = s.table_id")
       .whenMatchedUpdateAll()
       .whenNotMatchedInsertAll()
       .execute())
else:
    df.write.format("delta").mode("overwrite").saveAsTable(target_path)

print(f"Loaded {df.count()} rows into {target_path}")
df.show(truncate=False)

