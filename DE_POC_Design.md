# Enterprise-Grade Batch Data Platform — POC Design Document

## 1. The Business Problem

**"Global Retail Order & Customer 360 Analytics Platform"**

A mid-size e-commerce company needs a unified analytics platform that combines:
- Live transactional order data from its OLTP database
- Legacy ERP/warehouse inventory feeds (an "on-prem" system that only exports files)
- External marketing/finance data delivered as flat files

This scenario is deliberately chosen because it **forces** every concept on your list to appear naturally:
- Orders need **CDC / incremental load** (high volume, frequently updated — order status changes).
- Customers and products need **SCD Type 1/2** (customers change address/tier; products change price/category).
- Warehouse/ERP files arrive **irregularly and late** → **file arrival detection, late-arriving data, watermarking**.
- Historical sales need **backfill and reprocessing** when new business logic is introduced (e.g., new tax rules).
- Multiple source systems with different schemas → **schema evolution, metadata-driven pipelines**.
- Order-status corrections and re-sent files → **deduplication, idempotency**.
- High file counts from daily exports → **small file handling, partitioning, compaction**.

This is a much better fit than a "sales dashboard" toy project because it has **entity relationships, state changes over time, and multi-source heterogeneity** — the three things that make batch pipelines hard in real life.

---

## 2. Data Sources (and why these are the right ones)

Your instinct (RDBMS + GCS files + on-prem) is correct. Concretely:

| # | Source | Real Dataset / How to Simulate | Load Pattern |
|---|--------|--------------------------------|--------------|
| 1 | **Cloud SQL (MySQL/Postgres)** — Orders, Order Items, Customers, Payments | Load the **Olist Brazilian E-commerce dataset** (Kaggle, real-world, has orders/customers/products/payments/reviews with real messiness) into Cloud SQL. Replay it as a stream of inserts/updates using a Python "producer" script to simulate a live OLTP system | Full load (day 0) + CDC/incremental thereafter |
| 2 | **GCS flat files** — Product catalog (JSON), currency/FX rates (CSV), marketing spend (CSV), returns (CSV) | Generate synthetically with `Faker`, or use Olist's product/category files. Drop files on a schedule to simulate daily feeds | Full + incremental file drops |
| 3 | **"On-prem" ERP/Warehouse system** | Simulate with a small **Cloud Function / VM / cron script** that periodically writes CSV/fixed-width files to an "SFTP-like" GCS landing bucket (`landing/onprem_erp/`), with realistic issues: duplicate re-sends, late files, occasional schema drift, malformed rows | Batch file drop, irregular arrival |

**Optional 4th source (adds real CDC credibility):** Enable **Datastream** (GCP-native CDC) on Cloud SQL to stream binlog changes directly to GCS as Avro/JSON. This lets you say in an interview: *"I implemented actual log-based CDC using Datastream, not just polling with a watermark,"* which is a strong differentiator. Keep the polling/watermark approach as a fallback path (for the ERP source, which has no CDC capability — this is realistic, since legacy systems rarely support CDC).

---

## 3. End-to-End Architecture

```
                         ┌─────────────────────────────────────────────┐
                         │              ORCHESTRATION LAYER              │
                         │        Cloud Composer (Airflow 2.x)          │
                         │  - Metadata-driven DAG generation             │
                         │  - Sensors, retries, alerting, SLAs           │
                         └───────────────┬───────────────────────────────┘
                                         │ triggers
      ┌────────────────────┬────────────┼────────────────┬───────────────────┐
      ▼                    ▼            ▼                ▼                   ▼
 Cloud SQL           Datastream     GCS Landing      GCS Landing         Control Plane
 (OLTP: orders,      (CDC binlog    (flat files:      (simulated          Cloud SQL/Firestore
  customers,          → GCS Avro)   catalog, FX,      on-prem ERP        (pipeline_config,
  products)                          marketing)        drops)             audit_log, watermark)
      │                    │              │                │                    │
      └────────────────────┴──────────────┴────────────────┘
                                  │
                                  ▼
                     ┌─────────────────────────┐
                     │   BRONZE (Raw Zone)      │  GCS: gs://.../bronze/
                     │  - As-is, immutable      │  Iceberg/Delta tables
                     │  - Partitioned by         │  Schema = source schema + metadata cols
                     │    ingestion_date         │  (ingest_ts, source_file, batch_id)
                     └────────────┬─────────────┘
                                  │ Dataproc PySpark (cleanse, validate, dedupe, conform)
                                  ▼
                     ┌─────────────────────────┐
                     │   SILVER (Cleansed)      │  GCS: gs://.../silver/
                     │  - Deduplicated            │  Iceberg/Delta MERGE for CDC + SCD2
                     │  - Standardized types/nulls│ Partitioned by business date
                     │  - SCD1 (products) /       │
                     │    SCD2 (customers) applied│
                     │  - DQ checks enforced       │
                     └────────────┬─────────────┘
                                  │ Dataproc PySpark (aggregate, join, business rules)
                                  ▼
                     ┌─────────────────────────┐
                     │   GOLD (Curated)          │  GCS (Iceberg/Delta) + BigQuery
                     │  - Star schema             │  (fact_orders, dim_customer,
                     │  - Pre-aggregated marts     │   dim_product, agg_daily_sales)
                     │  - BI/analytics-ready       │  Partition + cluster for BigQuery
                     └────────────┬─────────────┘
                                  ▼
                     BigQuery (serving) → Looker Studio / BI tool
```

**Cross-cutting layers (touch every stage):**
- **Metadata/control plane** (Cloud SQL table or Firestore): drives what runs, how, and tracks watermarks.
- **Audit & lineage layer**: every job writes to `audit_log`; OpenLineage emits lineage events to Marquez/Data Catalog.
- **Monitoring & alerting**: Cloud Monitoring dashboards + Airflow callbacks → Slack/Email/PagerDuty.

---

## 4. Mapping Every Concept to the Architecture

| Concept | Where It Lives | How It's Demonstrated |
|---|---|---|
| **Full Load** | Day-0 ingestion | Initial DAG run with `load_type=full` pulls entire Cloud SQL tables and all historical files |
| **Incremental Load** | Bronze ingestion | Config-driven `incremental_column` (e.g., `updated_at`) read from `control.watermark_table`; only rows > last watermark pulled |
| **CDC** | Datastream → Bronze, or Spark JDBC diff for ERP | Datastream captures binlog inserts/updates/deletes; Silver layer applies them via Iceberg/Delta `MERGE INTO` |
| **Backfill** | Airflow CLI/`dagrun` with date range params | `airflow dags backfill` re-runs historical partitions using the same DAG logic (idempotent by design) |
| **Historical Reprocessing** | Gold layer | A `reprocess_gold` DAG param wipes and rebuilds specific partitions when business logic changes (e.g., new tax calc) |
| **Parameterized DAGs** | All DAGs | DAGs take `{{ dag_run.conf }}` params: `source_name`, `load_type`, `start_date`, `end_date`, `reprocess=True/False` |
| **Retry Mechanisms** | Task-level | `retries=3`, `retry_delay`, `retry_exponential_backoff=True` on all Dataproc/Sensor tasks |
| **Idempotent Design** | Silver/Gold Spark jobs | Use Iceberg/Delta `MERGE` (upsert) instead of `INSERT`; re-running a task never duplicates data |
| **Watermark-based Processing** | Bronze ingestion + control table | `control.source_watermark(source, table, last_value, last_run_ts)` updated only on task success |
| **Partitioning Strategies** | Bronze/Silver/Gold | Bronze: partition by `ingestion_date`. Silver: partition by `order_date`. Gold: partition by `order_date`, cluster by `region`/`customer_id` |
| **Schema Evolution** | Silver ingestion + Iceberg/Delta | Iceberg/Delta natively supports adding columns; Spark job reads `control.schema_registry` to detect and log new/dropped columns |
| **Data Validation/Quality** | Bronze→Silver transition | Great Expectations or custom PySpark DQ framework: null checks, referential integrity, range checks, row-count reconciliation; failures routed to a quarantine zone |
| **Metadata-driven Pipeline** | Control plane | One generic DAG + `pipeline_config` table describes source, target, load type, schedule, columns — DAGs generated dynamically at parse time |
| **Dynamic Configuration** | Airflow Variables/Connections + config table | Cluster size, Spark configs, file paths are all parameterized, not hardcoded |
| **Error Handling/Logging** | All PySpark jobs | Structured logging (`logging` module → Cloud Logging), try/except with custom exceptions, failed records written to `error/` zone with reason codes |
| **Audit Tables** | Control plane | `audit_log(run_id, dag_id, task_id, source, rows_read, rows_written, rows_rejected, start_ts, end_ts, status)` |
| **Checkpointing** | Bronze ingestion for large JDBC pulls | Spark job writes progress checkpoints (last successfully processed chunk/offset) so a failed job resumes, not restarts |
| **Late-Arriving Data** | Silver merge logic | Silver job re-opens partitions within a configurable "grace window" (e.g., 3 days) and merges late records instead of dropping them |
| **File Arrival Detection** | Airflow Sensors | `GCSObjectExistenceSensor` / `GCSObjectsWithPrefixExistenceSensor`, or event-driven via **Cloud Storage → Pub/Sub → Airflow Dataset trigger** |
| **Deduplication** | Bronze→Silver | Window function (`ROW_NUMBER() OVER (PARTITION BY pk ORDER BY updated_at DESC)`) keeps latest record per key |
| **SCD Type 1** | Product dimension | Overwrite attributes (e.g., price correction) — no history kept |
| **SCD Type 2** | Customer dimension | `effective_start_date`, `effective_end_date`, `is_current` flag; new row inserted on change, old row closed |
| **Performance Optimization** | Dataproc job design | Broadcast joins for small dims, `repartition`/`coalesce` tuning, caching reused DataFrames, Iceberg/Delta file compaction |
| **Small File Handling** | Bronze ingestion of ERP files | Scheduled `OPTIMIZE`/compaction job merges small files into target file size (~128–256MB) |
| **Partition Pruning** | Gold/BigQuery queries | Partitioned + clustered tables; queries filter on partition column, verified via `EXPLAIN`/query plan |
| **Job Monitoring** | Cloud Monitoring + Airflow UI | Custom metrics (rows processed, job duration) pushed to Cloud Monitoring; Airflow Gantt/Tree view for task health |
| **Alerting** | Airflow callbacks | `on_failure_callback`, `sla_miss_callback` → Slack webhook / email; Cloud Monitoring alert policies on Dataproc job failures |
| **Failure Recovery** | Idempotent + checkpointed design | Failed DAG can be cleared and re-run safely; no manual cleanup needed |
| **Cost Optimization** | Dataproc cluster policy | Ephemeral clusters (create → run → tear down), preemptible/Spot worker nodes, autoscaling policies, right-sized machine types |
| **Data Lineage** | OpenLineage + Data Catalog/Marquez | Every Spark job emits lineage events showing source→target dependencies, visualized in Marquez UI |
| **Orchestration Best Practices** | Airflow design | TaskGroups for modularity, Dynamic Task Mapping for per-source parallelism, Airflow Datasets for data-aware scheduling, no top-level code side effects |

---

## 5. Airflow Features Worth Demonstrating

- **Metadata-driven dynamic DAG generation** (loop over `pipeline_config` rows to generate DAGs/tasks at parse time)
- **TaskGroups** to organize Bronze/Silver/Gold stages within one DAG
- **Dynamic Task Mapping** (`.expand()`) to fan out per-source or per-partition processing
- **Sensors**: `GCSObjectExistenceSensor`, `SqlSensor` (wait for upstream table freshness), deferrable sensors for cost efficiency
- **Airflow Datasets** (data-aware scheduling): Gold DAG triggers automatically when Silver dataset is updated, instead of pure time-based scheduling
- **BranchPythonOperator**: choose full-load vs incremental-load path based on config
- **DataprocCreateClusterOperator / DataprocSubmitJobOperator / DataprocDeleteClusterOperator**, or better, **DataprocWorkflowTemplateInstantiateOperator** for atomic create-run-teardown
- **Retries with exponential backoff**, custom `on_retry_callback`
- **SLAs** and `sla_miss_callback`
- **XComs** to pass row counts / watermark values between tasks for audit logging
- **Params** (Airflow 2.x typed DAG params) for `airflow dags trigger` with runtime config (backfill ranges, reprocess flags)
- **Trigger rules** (`all_done`, `one_failed`) to always run audit/logging tasks regardless of upstream success/failure

---

## 6. Metadata Tables & Configuration-Driven Design

Store these in Cloud SQL (a lightweight "control plane" DB — realistic and queryable):

```sql
-- Defines every source/table this platform manages
control.pipeline_config (
  pipeline_id, source_system, source_table, target_layer,
  load_type ENUM('full','incremental','cdc'),
  incremental_column, primary_keys, partition_column,
  schedule_cron, is_active, dq_rule_set_id
)

-- Tracks incremental progress per source
control.watermark_table (
  source_system, source_table, last_value, last_loaded_ts, updated_by_run_id
)

-- Every job run — this is your audit trail
control.audit_log (
  run_id, dag_id, task_id, pipeline_id,
  rows_read, rows_written, rows_rejected,
  start_ts, end_ts, status, error_message
)

-- Column-level schema tracking, powers schema evolution detection
control.schema_registry (
  source_table, column_name, data_type, is_active,
  first_seen_ts, last_seen_ts
)

-- Configurable DQ rules per table (great for a "rules engine" talking point)
control.data_quality_rules (
  rule_id, source_table, column_name, rule_type
    ENUM('not_null','unique','range','regex','referential_integrity'),
  rule_expression, severity ENUM('warn','fail')
)

-- Tracks file arrivals for the "on-prem" simulated feed
control.file_tracker (
  file_name, source_system, checksum, arrival_ts,
  processed_ts, status, is_duplicate
)
```

Your Airflow DAGs and Spark jobs **read these tables at runtime** — nothing about source/target/columns is hardcoded. This "metadata-driven" framing is exactly what interviewers want to hear vs. a project with N hardcoded DAGs.

---

## 7. GCS Folder Structure

```
gs://de-poc-platform/
├── landing/                        # raw source drops (mimics SFTP/on-prem)
│   ├── onprem_erp/{yyyy}/{mm}/{dd}/
│   ├── marketing_feed/{yyyy}/{mm}/{dd}/
│   └── cdc_raw/{table}/{yyyy}/{mm}/{dd}/       # Datastream output
│
├── bronze/
│   └── {source_system}/{table_name}/ingestion_date={yyyy-mm-dd}/   # Iceberg/Delta table
│
├── silver/
│   └── {domain}/{table_name}/                  # partitioned by business date inside table
│
├── gold/
│   └── {domain}/{mart_name}/                   # e.g. sales/fact_orders, sales/agg_daily_sales
│
├── quarantine/                      # rows failing DQ checks, with reason codes
│   └── {table_name}/dt={yyyy-mm-dd}/
│
├── checkpoints/                     # Spark job checkpoints for resumability
│   └── {job_name}/
│
├── configs/                         # optional: DAG configs as JSON/YAML if not fully DB-driven
│   └── pipeline_config.yaml
│
├── code/
│   ├── dataproc_jobs/{bronze,silver,gold}/*.py
│   └── dags/*.py
│
└── logs/                            # supplementary job logs (in addition to Cloud Logging)
```

**Design notes:**
- `landing/` is intentionally separate from `bronze/` — landing is untouched vendor format (CSV/fixed-width); bronze is the same data in Iceberg/Delta with ingestion metadata columns added. This separation is itself an interview talking point (immutability + replayability of the true "raw" copy).
- Partition columns are chosen for **query pattern**, not just ingestion convenience — Gold partitions by `order_date` because that's how BI tools will filter.

---

## 8. Dataproc Job Structure

Recommended repo layout for the Spark codebase:

```
dataproc_jobs/
├── common/
│   ├── spark_session.py         # builds SparkSession w/ Iceberg/Delta configs
│   ├── config_loader.py         # reads control.pipeline_config, control.data_quality_rules
│   ├── dq_framework.py          # generic rule engine: not_null, unique, range, referential
│   ├── audit_writer.py          # writes to control.audit_log at job start/end
│   ├── merge_utils.py           # generic SCD1/SCD2/CDC merge functions
│   └── logger.py                # structured logging to Cloud Logging
│
├── bronze/
│   ├── ingest_jdbc_full.py      # full load from Cloud SQL
│   ├── ingest_jdbc_incremental.py
│   ├── ingest_cdc_datastream.py
│   └── ingest_files.py          # ingest CSV/JSON landing files → bronze
│
├── silver/
│   ├── cleanse_orders.py
│   ├── scd2_customers.py
│   ├── scd1_products.py
│   └── dedupe_and_conform.py
│
├── gold/
│   ├── build_fact_orders.py
│   └── build_daily_sales_agg.py
│
└── tests/
    ├── test_dq_framework.py
    └── test_merge_utils.py      # pytest + chispa/spark-testing-base
```

**Execution pattern (interview-worthy):**
- Use **Dataproc Workflow Templates** (not manually creating/deleting clusters in the DAG) — atomically create an ephemeral cluster, run a DAG-of-Spark-jobs, tear down cluster, all as one Airflow operator call (`DataprocInstantiateWorkflowTemplateOperator`). This is the pattern real companies use for cost control.
- **Cluster config**: autoscaling policy, 2 preemptible/Spot secondary workers + 2 standard primary workers, initialization action to install Iceberg/Delta jars.
- Every job accepts `--pipeline_id` and looks up its own config — one generic `bronze_ingest.py` handles all sources, not one script per table.

---

## 9. Logging, Monitoring & Audit Strategy

| Layer | Tool | What It Captures |
|---|---|---|
| **Job-level logs** | Python `logging` → stdout → Cloud Logging (auto-captured from Dataproc) | Structured JSON logs: pipeline_id, run_id, rows processed, warnings |
| **Audit trail** | `control.audit_log` table | Row counts (read/written/rejected), duration, status — queryable for reconciliation |
| **Data quality metrics** | `control.dq_results` table + optional Great Expectations data docs | Per-rule pass/fail counts, trend over time |
| **Orchestration monitoring** | Airflow UI (Composer) + Cloud Monitoring | DAG/task success rate, duration trends, SLA misses |
| **Infra monitoring** | Cloud Monitoring dashboards | Dataproc cluster CPU/memory/YARN utilization, autoscaling events |
| **Alerting** | Airflow `on_failure_callback` → Slack/Email; Cloud Monitoring alert policies | Real-time notification on failure, SLA breach, or DQ rule failures above threshold |
| **Lineage** | OpenLineage → Marquez (or Dataplex/Data Catalog if you want pure-GCP) | Visual source→bronze→silver→gold dependency graph, column-level lineage |

A good demo artifact: a small **Looker Studio / Streamlit dashboard** reading from `audit_log` and `dq_results` showing pipeline health — this alone is a strong portfolio visual.

---

## 10. Failure Scenarios to Simulate

Build these in deliberately — they're what separates a POC from a toy demo:

1. **Source DB unavailable** during scheduled extraction → task retries with backoff, eventually alerts.
2. **Malformed/corrupt file** in landing zone (bad delimiter, broken JSON) → quarantined with reason code, pipeline continues for other files.
3. **Schema drift**: ERP feed suddenly adds a new column or renames one → schema_registry detects it, job logs a warning and either auto-adds the column (Iceberg/Delta evolution) or fails gracefully depending on config.
4. **Duplicate file re-delivery** (same file, same name, re-uploaded) → `file_tracker` checksum comparison prevents double-processing.
5. **Late-arriving data**: an order confirmed 2 days ago gets a correction file today → Silver merge re-opens that partition within grace window.
6. **Dataproc cluster preemption** (using Spot/preemptible workers) → Spark job checkpoint allows resumption without full restart.
7. **Partial write failure** (job dies mid-write) → because writes are idempotent (Iceberg/Delta MERGE, staged writes), a re-run produces correct results, no manual cleanup.
8. **Downstream BigQuery load failure** (quota, schema mismatch) → task fails independently without corrupting the Gold Iceberg/Delta table; alert fires; can be retried standalone.
9. **DQ threshold breach** (e.g., >5% null customer_ids in a batch) → pipeline halts before promoting Bronze→Silver, alert raised, bad batch quarantined for manual review.
10. **Backfill collision**: someone triggers a backfill for a date range that overlaps with a currently-running incremental load → demonstrate via Airflow's `max_active_runs` / pool-based concurrency control to prevent conflicting writes.

---

## 11. Interview-Worthy Differentiators (Beyond Standard ETL)

These are the things that make reviewers sit up:

- **Metadata-driven, config-generated DAGs** instead of hand-written per-source DAGs (shows platform-thinking, not script-thinking).
- **Open table format (Iceberg or Delta Lake) with time travel** — demo a query against an old snapshot to "prove" you can recover from bad data or audit historical state.
- **Data lineage visualization** (OpenLineage/Marquez) — very few portfolio projects have this.
- **CI/CD**: Cloud Build pipeline that lints/tests PySpark code (pytest + `chispa`) and validates DAGs (`airflow dags list-import-errors`) before deployment — shows software engineering maturity, not just data plumbing.
- **Infrastructure as Code**: Terraform for GCS buckets, Cloud SQL, Composer environment, Dataproc autoscaling policies, IAM roles — this alone signals "production mindset."
- **Data contracts / schema registry** enforced before promotion between layers.
- **Cost-conscious engineering**: ephemeral clusters, Spot VMs, autoscaling, partition pruning shown with query cost comparison (bytes scanned before/after partitioning).
- **A working "Customer 360" or reconciliation dashboard** consuming Gold — ties the pipeline to a visible business outcome, not just plumbing.
- **A DR/runbook doc**: "if X fails, here's the recovery procedure" — very few candidates think this far, and it's a strong interview talking point.

---

## 12. Additional Production-Grade Concepts to Include

Experienced data engineers will look for these even if not explicitly on your list:

- **Infrastructure as Code (Terraform)** for all GCP resources — reproducible environments.
- **Environment promotion**: dev/staging/prod separation (different GCS buckets, Composer environments or namespaces, or at minimum config-driven environment suffixes).
- **Secrets management**: Cloud SQL credentials, Slack webhook tokens via Secret Manager, not Airflow Variables in plaintext.
- **IAM least privilege**: dedicated service accounts per component (Composer, Dataproc, Cloud SQL access) rather than one broad service account.
- **Data cataloging**: register Bronze/Silver/Gold tables in **Dataplex/Data Catalog** for discoverability.
- **PII handling**: column-level masking or tokenization for customer PII in Silver/Gold (e.g., hashed email), demonstrating compliance-awareness.
- **Testing strategy**: unit tests for PySpark transformation logic, DAG integrity tests, data contract tests — not just "it ran once and worked."
- **Capacity/SLA documentation**: expected data volumes, expected job duration, and what "on time" means for this pipeline.
- **Disaster recovery**: backup/retention policy for Bronze layer (source of truth for reprocessing), point-in-time recovery approach.
- **Version control & code review discipline**: PR-based workflow even solo, tagged releases for the Spark job codebase.

---

## Suggested Build Order (for your own sanity)

1. Terraform: buckets, Cloud SQL, Composer, service accounts
2. Load Olist dataset into Cloud SQL; build the ERP file simulator
3. Control-plane tables (config, watermark, audit, DQ rules, file tracker)
4. Bronze ingestion jobs (full + incremental + file-based) + generic DAG
5. Silver: dedup, SCD1/SCD2, DQ framework, late-arrival handling
6. Gold: star schema build + BigQuery publish
7. Failure-scenario testing (inject each of the 10 above, verify recovery)
8. Monitoring/alerting + lineage + a small health dashboard
9. CI/CD wiring + README with architecture diagram and "if X fails, do Y" runbook

This build order lets you demo a working (if partial) pipeline at every stage — useful if you want to talk about "what I built and why" incrementally in interviews rather than only showing a finished black box.
