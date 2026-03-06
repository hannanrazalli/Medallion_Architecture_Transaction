# SILVER LAYER - ENTERPRISE INCREMENTAL

import logging
from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime

# SETUP LOGGING
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# PHASE 1: CONFIG (Symmetry dengan Bronze)
silver_config = {
    "table_name": "workspace.default.silver_transactions",
    "source_path": "workspace.default.bronze_transactions",
    "quarantine_table": "workspace.default.silver_transactions_quarantine",
    "batch_id": f"SILVER_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
    "schema": """txn_id STRING, cust_id LONG, amount DOUBLE, points INT, is_member BOOLEAN, 
                 status STRING, txn_date STRING, is_deleted BOOLEAN, _ingest_at TIMESTAMP, 
                 _batch_id_bronze STRING, _ingest_date DATE, _source_file STRING, 
                 _processed_at TIMESTAMP, _batch_id_silver STRING""",
    "repartition_num": 5,
    "log_table": {
        "batch": "workspace.default.batch_control",
        "sla": "workspace.default.sla_metrics"
    }
}

# PHASE 2: HELPER UTILS
def upsert_log(spark, table, schema, data, keys):
    df = spark.createDataFrame(data, schema)
    if not spark.catalog.tableExists(table):
        df.write.format("delta").mode("overwrite").saveAsTable(table)
    else:
        dt = DeltaTable.forName(spark, table)
        condition = " AND ".join([f"t.{k} = s.{k}" for k in keys])
        (dt.alias("t").merge(df.alias("s"), condition)
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
        
def checK_schema(spark, table, new_col, col_type="BOOLEAN"):
    if spark.catalog.tableExists(table):
        cols = [c.lower() for c in spark.table(table).columns]
        if new_col.lower() not in cols:
            spark.sql(f"ALTER TABLE {table} ADD COLUMNS {new_col} {col_type}")

# PHASE 3: BRONZE TRANSFORM
def silver_transform(df, batch_id, target_schema):
    # STEP 1: DEDUP
    window_spec = Window.partitionBy("txn_id").orderBy(F.col("_ingest_at").desc())
    df = df.withColumn("rn", F.row_number().over(window_spec)).filter(F.col("rn") == 1).drop("rn")

    # STEP 2: NORMALIZE & SOFT DELETE
    valid_status = ["COMPLETED", "PENDING", "CANCELLED"]
    df = (df.withColumn("status", F.when(F.upper(F.trim(F.col("status"))).isin(valid_status),
                                         F.upper(F.trim(F.col("status")))).otherwise("UNKNOWN"))
            .withColumn("is_deleted", F.col("status") == "CANCELLED"))

    # STEP 3: AUDIT COL
    df = (df.withColumn("_source_file", F.coalesce(F.col("_source_file"), F.lit("UNKNOWN")))
            .withColumn("_processed_at", F.current_timestamp())
            .withColumn("_batch_id_silver", F.lit(batch_id)))

    # STEP 4: ENFORCE SCHEMA
    full_schema = []

    for f in target_schema.fields:
        if f.name in df.columns:
            expr = F.col(f.name).cast(f.dataType)
        else:
            expr = F.lit(None).cast(f.dataType).alias(f.name)
        
        full_schema.append(expr)
    
    return df.select(*full_schema)

# PHASE 4: SILVER ENGINE
def silver_engine(spark, config):
    table_name = config['table_name']
    source_file = config['source_path']
    quarantine_table = config['quarantine_table']
    run_batch_id = config['batch_id']

    batch_table = config['log_table']['batch']
    sla_table = config['log_table']['sla']

    batch_schema = "batch_id STRING, layer STRING, status STRING, processed_at TIMESTAMP"
    sla_schema = "batch_id STRING, layer STRING, total_rows LONG, clean_rows LONG, quarantine_rows LONG, status STRING, processed_at TIMESTAMP"

    logger.info(f"[START] Starting Silver ingestion Batch: {run_batch_id}")

    upsert_log(spark, batch_table, batch_schema,
               [(run_batch_id, "SILVER", "STARTED", datetime.now())], ["batch_id", "layer"])
    
    try:
        # STEP 1: WATERMARK
        if spark.catalog.tableExists(table_name):
            last_wm = spark.table(table_name).agg(F.max("_ingest_at")).first()[0]
        else:
            last_wm = None

        # STEP 2: LOAD
        if last_wm:
            df_raw = spark.table(source_file).filter(F.col("_ingest_at") > last_wm)
        else:
            df_raw = spark.table(source_file)

        # STEP 3: EARLY EXIT
        if not df_raw.limit(1).take(1):
            logger.info(f"[SKIP] No new data found. Logging as SKIPPED")
            upsert_log(spark, batch_table, batch_schema,
                       [(run_batch_id, "SILVER", "SKIPPED", datetime.now())], ["batch_id", "layer"])
            return

        # STEP 4: SPLIT > TRANSFORM > SPLIT
        target_schema = T.StructType.fromDDL(config['schema'])
        bronze_clean = df_raw.filter(F.col("_record_status") == "CLEAN")
        bronze_corrupt = df_raw.filter(F.col("_record_status") == "CORRUPT")

        df_processed = silver_transform(bronze_clean, run_batch_id, target_schema)

        df_clean = df_processed.filter("amount IS NOT NULL AND points IS NOT NULL")
        df_empty = df_processed.filter("amount IS NULL OR points IS NULL")
        df_quarantine = bronze_corrupt.unionByName(df_empty, allowMissingColumns=True)

        # STEP 5: WRITE CLEAN
        if not spark.catalog.tableExists(table_name):
            (df_clean.repartition(config.get("repartition_num", 5))
                .write.format("delta")
                .partitionBy("_ingest_date")
                .saveAsTable(table_name))
        else:
            dt = DeltaTable.forName(spark, table_name)
            condition = "t.txn_id = s.txn_id"
            (dt.alias("t").merge(df_clean.alias("s"), condition)
             .whenMatchedUpdateAll()
             .whenNotMatchedInsertAll()
             .execute())

        # STEP 6: WRITE QUARANTINE
        if not spark.catalog.tableExists(quarantine_table):
            (df_quarantine.write.format("delta")
                .mode("overwrite")
                .saveAsTable(quarantine_table))
        else:
            dt = DeltaTable.forName(spark, table_name)
            condition = "t.txn_id = s.txn_id"
            (dt.alias("t").merge(df_quarantine.alias("s"), condition)
             .whenNotMatchedInsertAll()
             .execute())

        # STEP 7: OPTIMIZE
        spark.sql(f"OPTIMIZE {table_name} ZORDER BY (txn_id)")

        # STEP 8: SLA CALCULATION
        clean_tagged = df_clean.withColumn("is_clean", F.lit(1))
        quarantine_tagged = df_quarantine.withColumn("is_clean", F.lit(0))
        df_combined = clean_tagged.unionByName(quarantine_tagged, allowMissingColumns=True)

        df_summary = df_combined.agg(
            F.count("*").alias("total"),
            F.sum(F.col("is_clean")).alias("clean"))
        
        stats = df_summary.collect()[0]
        t_rows = stats['total'] if stats['total'] else 0
        c_rows = stats['clean'] if stats['clean'] else 0
        q_rows = t_rows -c_rows

        # STEP 9: LOG SUCCESS
        upsert_log(spark, batch_table, batch_schema,
               [(run_batch_id, "SILVER", "SUCCESS", datetime.now())], ["batch_id", "layer"])
        upsert_log(spark, sla_table, sla_schema,
               [(run_batch_id, "SILVER", t_rows, c_rows, q_rows, "SUCCESS", datetime.now())], ["batch_id", "layer"])
        logger.info(f"[SUCCESS] Silver completed. Total: {t_rows}, Clean: {c_rows}, Quarantine: {q_rows}")

    except Exception as e:
        upsert_log(spark, batch_table, batch_schema,
               [(run_batch_id, "SILVER", "FAILED", datetime.now())], ["batch_id", "layer"])
        upsert_log(spark, sla_table, sla_schema,
               [(run_batch_id, "SILVER", 0, 0, 0, "FAILED", datetime.now())], ["batch_id", "layer"])
        logger.error(f"[ERROR] Silver failed: {e}")
        raise e

# PHASE 5: RUN ENGINE
if __name__ == "__main__":
    silver_engine(spark, silver_config)