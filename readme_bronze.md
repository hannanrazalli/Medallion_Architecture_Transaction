# BRONZE LAYER - ENTERPRISE INCREMENTAL (AUTO LOADER)

import logging
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------------------
# PHASE 1: CONFIGURATION
# --------------------------------------------------------------------------------------
bronze_config = {
    "table_name" : "workspace.default.bronze_transactions",
    "source_path" : "/Volumes/workspace/default/raw_volume/transactions_batch_1/",
    "checkpoint_path" : "/Volumes/workspace/default/checkpoints/bronze_transactions/",
    "batch_id" : f"BATCH_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
    "schema" : "amount double, cust_id long, is_member boolean, points int, status string, txn_date string, txn_id string",
    "repartition_num" : 5,
    "log_table" : {
        "batch" : "workspace.default.batch_control",
        "sla" : "workspace.default.sla_metrics"
    }}

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
    # Metadata file path disediakan secara automatik oleh Auto Loader
    src_col = F.col("_metadata.file_path") if "_metadata" in df.columns else F.lit("UNKNOWN")
    
    corrupt_logic = F.when(F.col("_corrupt_record").isNotNull(), F.lit("CORRUPT")).otherwise(F.lit("CLEAN"))

    return (df.withColumn("_source_path", src_col)
              .withColumn("_ingest_at", F.current_timestamp())
              .withColumn("_batch_id", F.lit(batch_id))
              .withColumn("_ingest_date", F.current_date()) 
              .withColumn("_record_status", corrupt_logic))

# --------------------------------------------------------------------------------------
# PHASE 3: CORE BRONZE ENGINE (AUTO LOADER - CLEAN ENTERPRISE VERSION)
# --------------------------------------------------------------------------------------
def bronze_ingestion(spark, config):
    table_name = config['table_name']
    source_path = config['source_path']
    checkpoint_path = config['checkpoint_path']
    run_batch_id = config['batch_id']
    full_schema = f"{config['schema']}, _corrupt_record string"
    
    batch_table, sla_table = config['log_table']['batch'], config['log_table']['sla']
    batch_schema = "batch_id string, layer string, status string, processed_at timestamp"
    sla_schema = "batch_id string, layer string, total_rows long, clean_rows long, quarantine_rows long, status string, processed_at timestamp"

    logging.info(f"Starting Auto Loader Bronze: {table_name}")
    upsert_log_table(spark, batch_table, batch_schema, [(run_batch_id, "BRONZE", "STARTED", datetime.now())], ["batch_id", "layer"])
    
    try:
        # ======================================================================
        # LANGKAH 1: DEFINISIKAN LOGIK PROSES (Micro-Batch Action)
        # ======================================================================
        def merge_to_delta(micro_batch_df, stream_batch_id):
            if micro_batch_df.isEmpty(): 
                return
            
            logging.info(f"Processing new files for stream batch: {stream_batch_id}")
            df_final = add_audit_col(micro_batch_df, run_batch_id)

            if not spark.catalog.tableExists(table_name):
                (df_final.repartition(config.get("repartition_num", 5))
                         .write.format("delta")
                         .partitionBy("_ingest_date")
                         .saveAsTable(table_name))
            else:
                condition = "t.txn_id = s.txn_id AND t._batch_id = s._batch_id AND t._ingest_date = s._ingest_date"
                (DeltaTable.forName(spark, table_name).alias("t")
                           .merge(df_final.alias("s"), condition)
                           .whenNotMatchedInsertAll()
                           .execute())

        # ======================================================================
        # LANGKAH 2: BACA FAIL BARU (Auto Loader cloudFiles)
        # ======================================================================
        logging.info(f"Scanning for new files in: {source_path}")
        stream_df = (spark.readStream.format("cloudFiles")
                          .option("cloudFiles.format", "json")
                          .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
                          .option("cloudFiles.useIncrementalListing", "true")
                          .schema(full_schema)
                          .load(source_path)
                          .select("*", "_metadata"))

        # ======================================================================
        # LANGKAH 3: JALANKAN PIPELINE (Trigger AvailableNow)
        # ======================================================================
        logging.info("Executing stream in Batch Mode...")
        query = (stream_df.writeStream
                          .foreachBatch(merge_to_delta)
                          .option("checkpointLocation", f"{checkpoint_path}/data")
                          .trigger(availableNow=True)
                          .start())
        
        query.awaitTermination() # Tahan code di sini sampai stream siap proses semua fail

        # ======================================================================
        # LANGKAH 4: KIRA SLA & LOG SUCCESS
        # ======================================================================
        logging.info("Calculating SLA metrics...")
        
        if spark.catalog.tableExists(table_name):
            df_res = (spark.table(table_name)
                           .filter(F.col("_batch_id") == run_batch_id)
                           .groupBy("_record_status")
                           .count()
                           .collect())
            stats = {r["_record_status"]: r["count"] for r in df_res}
            total_rows, clean_rows, quarantine_rows = sum(stats.values()), stats.get("CLEAN", 0), stats.get("CORRUPT", 0)
        else:
            total_rows, clean_rows, quarantine_rows = 0, 0, 0
            logging.info("Table does not exist yet (No files were processed).")

        upsert_log_table(spark, batch_table, batch_schema,
                         [(run_batch_id, "BRONZE", "SUCCESS", datetime.now())], ["batch_id", "layer"])
        upsert_log_table(spark, sla_table, sla_schema,
                         [(run_batch_id, "BRONZE", total_rows, clean_rows, quarantine_rows, "SUCCESS", datetime.now())], ["batch_id", "layer"])
        logging.info(f"Bronze completed. Total: {total_rows}, Clean: {clean_rows}, Corrupt: {quarantine_rows}")

    except Exception as e:
        upsert_log_table(spark, batch_table, batch_schema,
                         [(run_batch_id, "BRONZE", "FAILED", datetime.now())], ["batch_id", "layer"])
        upsert_log_table(spark, sla_table, sla_schema,
                         [(run_batch_id, "BRONZE", 0, 0, 0, "FAILED", datetime.now())], ["batch_id", "layer"])
        logging.error(f"Bronze failed: {e}")
        raise e

# --------------------------------------------------------------------------------------
# PHASE 4: EXECUTION
# --------------------------------------------------------------------------------------
bronze_ingestion(spark, bronze_config)