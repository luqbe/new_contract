# Framework_Library
# Framework_Library
import os
import sys
import re
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, current_timestamp
from soda.scan import Scan


# --- Validation Helper ---

def validate_data_contract(spark, df, dataset_name, contract_path):
    """
    Uses Soda Scan API to validate data against YAML contract.
    Creates temporary Delta table in default schema for reliable Soda access.
    """
    # Create temp table in default catalog
    temp_table = f"temp_{dataset_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

    try:
        # Write DataFrame as temporary Delta table
        df.write.format("delta").mode("overwrite").saveAsTable(f"default.{temp_table}")

        scan = Scan()
        scan.set_scan_definition_name(f"{dataset_name}_validation")
        scan.set_data_source_name("spark")
        scan.add_spark_session(spark, data_source_name="spark")

        # Load contract YAML
        with open(contract_path, 'r') as f:
            yaml_content = f.read()

        # Use regex to replace "checks for ANY_TABLE_gate:" with temp table
        # This handles customers_gate, customers_1_gate, sales_gate, etc.
        modified_yaml = re.sub(
            r'checks for (\w+_gate):',
            f'checks for default.{temp_table}:',
            yaml_content
        )

        print(f"DEBUG: Using temp table: default.{temp_table}")
        print(f"DEBUG: Modified YAML:\n{modified_yaml[:500]}")

        scan.add_sodacl_yaml_str(modified_yaml)

        # Execute scan
        scan.execute()

        print(scan.get_logs_text())

        return scan

    finally:
        # Clean up temporary table
        try:
            spark.sql(f"DROP TABLE IF EXISTS default.{temp_table}")
        except:
            pass


# --- Silver Upsert Logic ---
def upsert_to_silver(spark, df, target_table, merge_key):
    """
    Performs a Delta Merge (Upsert) into the Silver table.
    """
    from delta.tables import DeltaTable

    # Standard cleaning: Trim strings and add timestamp
    str_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() == "string"]
    for c in str_cols:
        df = df.withColumn(c, trim(col(c)))

    df = df.withColumn("_last_updated_at", current_timestamp())

    # Check if table exists to perform Merge, otherwise Create
    if spark.catalog.tableExists(target_table):
        print(f"Merging updates into {target_table} on {merge_key}...")
        silver_table = DeltaTable.forName(spark, target_table)

        silver_table.alias("target").merge(
            df.alias("updates"),
            f"target.{merge_key} = updates.{merge_key}"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        print(f"Creating new Silver table: {target_table}")
        df.write.format("delta").mode("overwrite").saveAsTable(target_table)


# --- Metadata Logger ---

def log_metadata(spark, dataset, status, row_count, target, error=None):
    from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

    spark.sql("CREATE SCHEMA IF NOT EXISTS contract.raw_data")

    error_msg = error if error is not None else ""

    log_entry = [(datetime.now(), dataset, status, int(row_count), target, error_msg)]

    schema = StructType([
        StructField("run_time", TimestampType(), False),
        StructField("dataset", StringType(), False),
        StructField("status", StringType(), False),
        StructField("row_count", LongType(), False),
        StructField("target_table", StringType(), False),
        StructField("error_message", StringType(), True)
    ])

    spark.createDataFrame(log_entry, schema).write.mode("append").format("delta").saveAsTable(
        "contract.raw_data.ingestion_stats")


# --- Main Medallion Orchestrator ---
def run_medallion_ingestion():
    spark = SparkSession.builder.getOrCreate()

    source = dbutils.widgets.get("source_path").strip()
    contract = dbutils.widgets.get("contract_path").strip()
    bronze_t = dbutils.widgets.get("bronze_table").strip()
    silver_t = dbutils.widgets.get("silver_table").strip()
    merge_id = dbutils.widgets.get("merge_key").strip()

    # Regex cleaning for dataset/file name
    file_name = os.path.basename(source).split('.')[0]
    file_name = re.sub(r'[^a-zA-Z0-9_]', '_', file_name)
    file_name = re.sub(r'_+', '_', file_name).strip('_')

    dataset_gate = f"{file_name}_gate"

    try:
        df_raw = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(source)

        # 1. Validate
        scan_results = validate_data_contract(spark, df_raw, dataset_gate, contract)

        # 2. Check for Failures
        if not scan_results.has_check_fails():
            # 2a. Bronze (Append Raw)
            df_raw.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable(bronze_t)

            # 2b. Silver (Upsert Clean)
            upsert_to_silver(spark, df_raw, silver_t, merge_id)

            log_metadata(spark, file_name, "SUCCESS", df_raw.count(), silver_t)
            print(f"Successfully processed {file_name}")
        else:
            err = f"Soda scan failed - check logs"
            log_metadata(spark, file_name, "FAILED", 0, silver_t, err)
            print(f"✗ Validation failed:\n{scan_results.get_logs_text()}")
            sys.exit(1)

    except Exception as e:
        log_metadata(spark, file_name, "ERROR", 0, silver_t, str(e))
        raise e