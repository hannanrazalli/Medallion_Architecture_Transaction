# GOLD LAYER - ENTERPRISE LEVEL (FULLY AUTOMATIC)

import logging
from pyspark.sql import SparkSession, functions as F, types as T
from delta.tables import DeltaTable
from datetime import datetime

# -------------------------
# 1. LOGGING
# -------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# -------------------------
# 2. GOLD TABLE SCHEMA
# -------------------------
gold_schema = T.StructType([
    T.StructField("txn_id", T.StringType(), True),
    T.StructField("cust_id", T.LongType(), True),
    T.StructField("amount", T.DoubleType(), True),
    T.StructField("points", T.IntegerType(), True),
    T.StructField("is_member", T.BooleanType(), True),
    T.StructField("status", T.StringType(), True),
    T.StructField("txn_date", T.StringType(), True),
    T.StructField("_ingested_at", T.TimestampType(), True),
    T.StructField("_batch_id", T.StringType(), True),
    T.StructField("_ingest_date", T.DateType(), True),
    T.StructField("_source_file", T.StringType(), True),
    T.StructField("valid_from", T.TimestampType(), True),
    T.StructField("valid_to", T.TimestampType(), True),
    T.StructField("is_current", T.BooleanType(), True),
    T.StructField("_gold_batch_id", T.StringType(), True)
])

# -------------------------
# 3. UTILITIES
# -------------------------
def add_gold_audit(df, gold_batch_id):
    return df.withColumn("_gold_batch_id", F.lit(gold_batch_id)) \
             .withColumn("valid_from", F.current_timestamp()) \
             .withColumn("valid_to", F.lit(None).cast(T.TimestampType())) \
             .withColumn("is_current", F.lit(True))

def run_gold_ingestion(spark, config):
    table_name = config["table_name"]
    source_table = config["source_table"]
    gold_batch_id = config["gold_batch_id"]
    watermark_col = config.get("watermark_col", "_ingest_date")
    repartition_num = config.get("repartition_num", 5)
    sla_table = config["sla_table"]

    # --- LOGIK AUTOMATIK (Sebab tu nampak pendek sikit tapi power) ---
    # Default ke 1900-01-01 kalau table tak wujud
    current_watermark = "1900-01-01"
    
    if spark.catalog.tableExists(table_name):
        max_dt = spark.sql(f"SELECT max({watermark_col}) FROM {table_name}").collect()[0][0]
        if max_dt:
            current_watermark = str(max_dt)
            logger.info(f"Incremental Load: Processing data after {current_watermark}")
    else:
        logger.info(f"Initial Load: Processing all data starting from {current_watermark}")

    # 1. Read & Filter
    df = spark.table(source_table).filter(F.col(watermark_col) > F.lit(current_watermark))
    
    if df.limit(1).count() == 0:
        logger.info("No new rows found. Exiting process.")
        return

    # 2. Add Audit & Deduplicate source
    df_gold = add_gold_audit(df, gold_batch_id).dropDuplicates(["txn_id"])

    # 3. Schema Alignment
    for field in gold_schema.fields:
        if field.name not in df_gold.columns:
            df_gold = df_gold.withColumn(field.name, F.lit(None).cast(field.dataType))
        else:
            df_gold = df_gold.withColumn(field.name, F.col(field.name).cast(field.dataType))

    # 4. Save/Merge
    if not spark.catalog.tableExists(table_name):
        (df_gold.repartition(repartition_num).write.format("delta").mode("overwrite")
                .partitionBy("_ingest_date").saveAsTable(table_name))
    else:
        delta_obj = DeltaTable.forName(spark, table_name)
        
        # 4a. Expire old records (SCD2 logic)
        staged_updates = df_gold.alias("s").join(
            spark.table(table_name).alias("t"),
            (F.col("s.txn_id") == F.col("t.txn_id")) & (F.col("t.is_current") == True)
        ).filter("t.amount <> s.amount OR t.points <> s.points OR t.status <> s.status") \
         .select("t.txn_id").distinct()

        if staged_updates.limit(1).count() > 0:
            update_ids = [r.txn_id for r in staged_updates.collect()]
            delta_obj.update(
                condition = F.col("txn_id").isin(update_ids) & (F.col("is_current") == True),
                set = {"is_current": F.lit(False), "valid_to": F.current_timestamp()}
            )

        # 4b. Insert new records
        (delta_obj.alias("t").merge(df_gold.alias("s"), "t.txn_id = s.txn_id AND t.valid_from = s.valid_from")
                  .whenNotMatchedInsertAll().execute())

    # 5. Optimization
    spark.sql(f"OPTIMIZE {table_name} ZORDER BY (txn_id, cust_id)")

    # 6. SLA Update (Fixed Full Schema)
    total_rows = df_gold.count()
    sla_schema = "batch_id string, layer string, total_rows long, clean_rows long, quarantine_rows long, status string, processed_at timestamp"
    sla_data = [(gold_batch_id, "GOLD", total_rows, total_rows, 0, "SUCCESS", datetime.now())]
    sla_df = spark.createDataFrame(sla_data, schema=sla_schema)

    if not spark.catalog.tableExists(sla_table):
        sla_df.write.format("delta").mode("overwrite").saveAsTable(sla_table)
    else:
        (DeltaTable.forName(spark, sla_table).alias("t")
                   .merge(sla_df.alias("s"), "t.batch_id = s.batch_id AND t.layer = s.layer")
                   .whenNotMatchedInsertAll().execute())

    logger.info("Gold Ingestion Complete.")

# -------------------------
# 5. CONFIG & EXECUTE
# -------------------------
gold_config = {
    "table_name": "workspace.default.gold_transactions",
    "source_table": "workspace.default.silver_transactions",
    "gold_batch_id": f"GOLD_{datetime.now().strftime('%Y%m%d_%H%M')}",
    "watermark_col": "_ingest_date",
    "repartition_num": 5,
    "sla_table": "workspace.default.sla_metrics"
}

run_gold_ingestion(spark, gold_config)