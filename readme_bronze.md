# BRONZE LAYER - ENTERPRISE INCREMENTAL (AUTO LOADER)

import logging
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------------------
# PHASE 1: CONFIGURATION
# --------------------------------------------------------------------------------------
bronze_config = {
    "table_name" : "workspace.default.bronze_transactions",
    "source_path" : "/Volumes/workspace/default/raw_volume/transactions_batch_1/",
    "checkpoint_path" : "/Volumes/workspace/default/checkpoints/bronze_transactions/",
    "batch_id" : f"BRONZE_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
    "schema" : "txn_id STRING, cust_id LONG, amount DOUBLE, is_member BOOLEAN, points INT, status STRING, txn_date STRING",
    "repartition_num" : 5,
    "log_table" : {
        "batch" : "workspace.default.batch_control",
        "sla" : "workspace.default.sla_metrics"
    }
}

# --------------------------------------------------------------------------------------
# PHASE 2: UNIVERSAL HELPERS & AUDIT LOGIC
# --------------------------------------------------------------------------------------
def upsert_log_table(spark, table_name, schema, data, merge_keys):
    df = spark.createDataFrame(data, schema)
    if not spark.catalog.tableExists(table_name):
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    else:
        dt = DeltaTable.forName(spark, table_name)
        condition = " AND ".join([f"t.{k} = s.{k}" for k in merge_keys])
        (dt.alias("t").merge(df.alias("s"), condition)
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
        
def add_audit_col(df, batch_id):
    if "_corrupt_record" not in df.columns:
        df = df.withColumn("_corrupt_record", F.lit(None).cast("string"))

    src_col = F.col("_metadata.file_path") if "_metadata" in df.columns else F.lit("UNKNOWN")
    corrupt_logic = F.when(F.col("_corrupt_record").isNotNull(), F.lit("CORRUPT")).otherwise(F.lit("CLEAN"))

    return (df.withColumn("_source_path", src_col)
              .withColumn("_ingest_at", F.current_timestamp())
              .withColumn("_batch_id_bronze", F.lit(batch_id))
              .withColumn("_ingest_date", F.current_date())
              .withColumn("_record_status", corrupt_logic)
              .drop("_metadata"))

# --------------------------------------------------------------------------------------
# PHASE 3: CORE BRONZE ENGINE (THE GATEKEEPER PATTERN)
# --------------------------------------------------------------------------------------
def bronze_ingestion(spark, config):
    table_name = config['table_name']
    source_path = config['source_path']
    checkpoint_path = config['checkpoint_path']
    run_batch_id = config['batch_id']
    full_schema = f"{config['schema']}, _corrupt_record string"
    
    batch_table = config['log_table']['batch']
    sla_table = config['log_table']['sla']
    
    batch_schema = "batch_id STRING, layer STRING, status STRING, processed_at TIMESTAMP"
    sla_schema = "batch_id STRING, layer STRING, total_rows LONG, clean_rows LONG, quarantine_rows LONG, status STRING, processed_at TIMESTAMP"

    spark.sql(f"CREATE TABLE IF NOT EXISTS {batch_table} (batch_id STRING, layer STRING, status STRING, processed_at TIMESTAMP) USING DELTA")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {sla_table} (batch_id STRING, layer STRING, total_rows LONG, clean_rows LONG, quarantine_rows LONG, status STRING, processed_at TIMESTAMP) USING DELTA")

    logger.info(f"[START] Starting Auto Loader Batch: {run_batch_id}")
    
    upsert_log_table(spark, batch_table, batch_schema, 
                     [(run_batch_id, "BRONZE", "STARTED", datetime.now())], ["batch_id", "layer"])
    
    try:
        # ======================================================================
        # LANGKAH 1: FOREACHBATCH (WRITE DATA & UPDATE SLA SERENTAK)
        # ======================================================================
        def merge_to_delta(micro_batch_df, stream_batch_id):
            df_final = add_audit_col(micro_batch_df, run_batch_id)

            # A. TULIS KE BRONZE TABLE
            if not spark.catalog.tableExists(table_name):
                (df_final.repartition(config.get("repartition_num", 5))
                         .write.format("delta")
                         .partitionBy("_ingest_date")
                         .saveAsTable(table_name))
            else:
                condition = "t.txn_id = s.txn_id AND t._ingest_date = s._ingest_date"
                (DeltaTable.forName(spark, table_name).alias("t")
                           .merge(df_final.alias("s"), condition)
                           .whenMatchedUpdateAll()
                           .whenNotMatchedInsertAll()
                           .execute())

            # B. KIRA SLA & LOG TERUS DI DALAM SINI
            df_res = (spark.table(table_name)
                           .filter(F.col("_ingest_date") == F.current_date()) 
                           .filter(F.col("_batch_id_bronze") == run_batch_id)
                           .groupBy("_record_status")
                           .count()
                           .collect())
            
            stats = {r["_record_status"]: r["count"] for r in df_res}
            total_rows = sum(stats.values())

            if total_rows > 0:
                clean_rows = stats.get("CLEAN", 0)
                quarantine_rows = stats.get("CORRUPT", 0)

            # Update jadual dengan status SUCCESS
                upsert_log_table(spark, batch_table, batch_schema,
                                [(run_batch_id, "BRONZE", "SUCCESS", datetime.now())], ["batch_id", "layer"])
                upsert_log_table(spark, sla_table, sla_schema,
                                [(run_batch_id, "BRONZE", total_rows, clean_rows, quarantine_rows, "SUCCESS", datetime.now())], ["batch_id", "layer"])

        # ======================================================================
        # LANGKAH 2: TRIGGER AUTO LOADER
        # ======================================================================
        stream_df = (spark.readStream.format("cloudFiles")
                          .option("cloudFiles.format", "json")
                          .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
                          .option("cloudFiles.useIncrementalListing", "true")
                          .schema(full_schema)
                          .load(source_path)
                          .select("*", "_metadata"))

        query = (stream_df.writeStream
                          .foreachBatch(merge_to_delta)
                          .option("checkpointLocation", f"{checkpoint_path}/data")
                          .trigger(availableNow=True)
                          .start())
        
        query.awaitTermination()

        # ======================================================================
        # LANGKAH 3: FINAL SUMMARY & SKIPPED LOGIC
        # ======================================================================
        # Semak status terkini selepas stream habis
        current_status_df = spark.table(batch_table).filter(F.col("batch_id") == run_batch_id).select("status").collect()
        current_status = current_status_df[0]["status"] if current_status_df else "UNKNOWN"
        
        if current_status == "STARTED":
            # Jika masih "STARTED", bermaksud foreachBatch tak trigger langsung (Tiada fail baru)
            logger.info("[SKIP] No new data detected. Early exit triggered. Logging as SKIPPED.")
            upsert_log_table(spark, batch_table, batch_schema,
                             [(run_batch_id, "BRONZE", "SKIPPED", datetime.now())], ["batch_id", "layer"])
            
        elif current_status == "SUCCESS":
            # Jika berjaya, tarik final data dari jadual SLA untuk paparan terminal
            sla_df = spark.table(sla_table).filter(F.col("batch_id") == run_batch_id).collect()
            if sla_df:
                t_rows = sla_df[0]["total_rows"]
                c_rows = sla_df[0]["clean_rows"]
                q_rows = sla_df[0]["quarantine_rows"]
                logger.info(f"[SUCCESS] Bronze completed successfully. Total: {t_rows}, Clean: {c_rows}, Corrupt: {q_rows}")

    except Exception as e:
        upsert_log_table(spark, batch_table, batch_schema,
                         [(run_batch_id, "BRONZE", "FAILED", datetime.now())], ["batch_id", "layer"])
        upsert_log_table(spark, sla_table, sla_schema,
                         [(run_batch_id, "BRONZE", 0, 0, 0, "FAILED", datetime.now())], ["batch_id", "layer"])
        
        logger.error(f"Bronze layer encountered an error: {e}")
        raise e

# --------------------------------------------------------------------------------------
# PHASE 4: EXECUTION
# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    bronze_ingestion(spark, bronze_config)