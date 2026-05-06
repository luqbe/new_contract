# Framework_Library
import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, current_timestamp
from soda.contracts.contract_verification import ContractVerification


# --- Validation Helper ---
def validate_data_contract(spark, df, dataset_name, contract_path):
    df.createOrReplaceTempView(dataset_name)
    verification = (
        ContractVerification.builder()
        .with_contract_yaml_file(contract_path)
        .with_dataset_name(dataset_name)
        .with_spark_session(spark)
        .execute()
    )
    return verification


# --- Silver Upsert Logic ---
def upsert_to_silver(df, target_table, merge_key):
    """
    Performs a Delta Merge (Upsert) into the Silver table.
    """
    from delta.tables import DeltaTable

    # Standard cleaning
    str_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() == "string"]
    for c in str_cols:
        df = df.withColumn(c, trim(col(c)))
    df = df.withColumn("_last_updated_at", current_timestamp())

    # Check if table exists to perform Merge, otherwise Create
    if spark.catalog.tableExists(target_table):
        print(f"Merging data into {target_table} on {merge_key}...")
        silver_table = DeltaTable.forName(spark, target_table)

        silver_table.alias("target").merge(
            df.alias("updates"),
            f"target.{merge_key} = updates.{merge_key}"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        print(f"Creating new Silver table: {target_table}")
        df.write.format("delta").saveAsTable(target_table)


# --- Metadata Logger ---
def log_metadata(dataset, status, row_count, target, error=None):
    spark.sql("CREATE SCHEMA IF NOT EXISTS main.metadata")
    log_entry = [(datetime.now(), dataset, status, row_count, target, error)]
    columns = ["run_time", "dataset", "status", "row_count", "target_table", "error_message"]

    spark.createDataFrame(log_entry, columns).write.mode("append").saveAsTable("contract.raw_data.metadata_ingestion_stats")


# --- Main Medallion Orchestrator ---
def run_medallion_ingestion():
    # Pulling from Task Parameters
    source = dbutils.widgets.getArgument("source_path")
    contract = dbutils.widgets.getArgument("contract_path")
    bronze_t = dbutils.widgets.getArgument("bronze_table")
    silver_t = dbutils.widgets.getArgument("silver_table")
    merge_id = dbutils.widgets.getArgument("merge_key")  # e.g., 'order_id'

    file_name = os.path.basename(source).split('.')[0]

    try:
        df_raw = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(source)

        # 1. Validate
        result = validate_data_contract(spark, df_raw, f"{file_name}_gate", contract)

        if result.is_ok():
            # 2. Bronze (Append Raw)
            df_raw.write.mode("append").option("mergeSchema", "true").saveAsTable(bronze_t)

            # 3. Silver (Upsert Clean)
            upsert_to_silver(df_raw, silver_t, merge_id)

            log_metadata(file_name, "SUCCESS", df_raw.count(), silver_t)
        else:
            err = ", ".join([c.name for c in result.get_checks() if c.outcome == "fail"])
            log_metadata(file_name, "FAILED", 0, silver_t, err)
            sys.exit(1)

    except Exception as e:
        log_metadata(file_name, "ERROR", 0, silver_t, str(e))
        raise e