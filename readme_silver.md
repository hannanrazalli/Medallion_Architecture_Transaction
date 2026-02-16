# SILVER LAYER - ENTERPRISE LEVEL

import logging
from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime

# -------------------------
# 1. LOGGING
# -------------------------
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# -------------------------
# 2. SILVER TARGET SCHEMA
# -------------------------
silver_schema = T.StructType([
    T.StructField("txn_id", T.StringType()),
    T.StructField("cust_id", T.LongType()),
    T.StructField("amount", T.DoubleType()),
    T.StructField("points", T.IntegerType()),
    T.StructField("is_member", T.BooleanType()),
    T.StructField("status", T.StringType()),
    T.StructField("txn_date", T.StringType()),
    T.StructField("_ingested_at", T.TimestampType()),
    T.StructField("_batch_id", T.StringType()),
    T.StructField("_ingest_date", T.DateType()),
    T.StructField("_source_file", T.StringType()),
    T.StructField("_processed_at", T.TimestampType()),
    T.StructField("_silver_batch_id", T.StringType())
])

# -------------------------
# 3. UTILITIES
# -------------------------
def fix_source_file_column(df):
    if "_source_file" in df.columns:
        dtype = dict(df.dtypes)["_source_file"]
        if dtype == "void":
            df = df.drop("_source_file")
    if "_source_file" not in df.columns:
        df = df.withColumn("_source_file", F.lit(None).cast("string"))
    return df

def ensure_columns(df, schema):
    for field in schema.fields:
        if field.name not in df.columns:
            df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
        else:
            if dict(df.dtypes)[field.name] != field.dataType.simpleString():
                df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))
    return df

def deduplicate_logic(df):
    window_spec = Window.partitionBy("txn_id").orderBy(F.col("_ingested_at").desc())
    return df.withColumn("row_num", F.row_number().over(window_spec)) \
             .filter("row_num = 1").drop("row_num")

def normalize_status(df):
    valid_status = ["COMPLETED", "PENDING", "CANCELLED"]
    return df.withColumn("status", F.upper(F.trim(F.col("status")))) \
             .withColumn("status", F.when(F.col("status").isin(valid_status),
                                          F.col("status")).otherwise(F.lit("UNKNOWN")))

def add_silver_audit(df, silver_batch_id):
    return df.withColumn("_processed_at", F.current_timestamp()) \
             .withColumn("_silver_batch_id", F.lit(silver_batch_id))

def apply_data_quality(df):
    clean_df = df.filter("amount IS NOT NULL AND points IS NOT NULL")
    quarantine_df = df.filter("amount IS NULL OR points IS NULL")
    return clean_df, quarantine_df

def update_sla_metrics(batch_id, layer, total_rows, clean_rows, quarantine_rows, status):
    table_name = "workspace.default.sla_metrics"
    now = datetime.now()
    data = [(batch_id, layer, total_rows, clean_rows, quarantine_rows, status, now)]
    schema = T.StructType([
        T.StructField("batch_id", T.StringType()),
        T.StructField("layer", T.StringType()),
        T.StructField("total_rows", T.LongType()),
        T.StructField("clean_rows", T.LongType()),
        T.StructField("quarantine_rows", T.LongType()),
        T.StructField("status", T.StringType()),
        T.StructField("processed_at", T.TimestampType())
    ])
    df = spark.createDataFrame(data, schema)
    if not spark.catalog.tableExists(table_name):
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    else:
        delta_obj = DeltaTable.forName(spark, table_name)
        (delta_obj.alias("t")
         .merge(df.alias("s"), "t.batch_id = s.batch_id AND t.layer = s.layer")
         .whenNotMatchedInsertAll()
         .execute())

def send_alert(batch_id, layer, message):
    # Template alert hook; boleh sambung ke Slack/Email
    logger.error(f"ALERT: Batch {batch_id} Layer {layer}: {message}")

# -------------------------
# 4. CORE ENGINE ENTERPRISE
# -------------------------
def run_silver_ingestion(spark, config):
    table_name = config["table_name"]
    quarantine_table = config["quarantine_table"]
    source_table = config["source_table"]
    silver_batch_id = config["silver_batch_id"]
    repartition_num = config.get("repartition_num", 5)
    watermark_col = "_ingested_at"

    logger.info(f"Starting Silver ingestion for {table_name}")

    # --- Incremental load / watermark ---
    last_watermark = None
    if spark.catalog.tableExists(table_name):
        last_watermark = spark.sql(f"SELECT max({watermark_col}) as max_ts FROM {table_name}").collect()[0]["max_ts"]
    
    df = spark.table(source_table)
    if last_watermark:
        df = df.filter(F.col(watermark_col) > F.lit(last_watermark))

    # --- Transformations ---
    df = deduplicate_logic(df)
    df = normalize_status(df)
    df = fix_source_file_column(df)
    df = add_silver_audit(df, silver_batch_id)
    df = ensure_columns(df, silver_schema)

    # --- Data Quality ---
    clean_df, quarantine_df = apply_data_quality(df)

    # --- Merge clean_df into Silver ---
    try:
        if not spark.catalog.tableExists(table_name):
            (clean_df.repartition(repartition_num)
             .write.format("delta")
             .mode("overwrite")
             .option("mergeSchema", "true")
             .partitionBy("_ingest_date")
             .saveAsTable(table_name))
        else:
            delta_obj = DeltaTable.forName(spark, table_name)
            (delta_obj.alias("t")
             .merge(clean_df.alias("s"), "t.txn_id = s.txn_id")
             .whenMatchedUpdate(
                 condition="t.status <> s.status OR t.amount <> s.amount OR t.points <> s.points",
                 set={
                     "status": "s.status",
                     "amount": "s.amount",
                     "points": "s.points",
                     "_processed_at": "s._processed_at",
                     "_silver_batch_id": "s._silver_batch_id"
                 })
             .whenNotMatchedInsertAll()
             .execute())

        # --- Delta Optimize + ZORDER ---
        spark.sql(f"OPTIMIZE {table_name} ZORDER BY (txn_id, cust_id)")

    except Exception as e:
        send_alert(silver_batch_id, "silver", str(e))
        update_sla_metrics(silver_batch_id, "silver", 0,0,0,"FAIL")
        raise

    # --- Merge quarantine ---
    try:
        if not spark.catalog.tableExists(quarantine_table):
            (quarantine_df.repartition(repartition_num)
             .write.format("delta")
             .mode("overwrite")
             .partitionBy("_ingest_date")
             .saveAsTable(quarantine_table))
        else:
            delta_obj = DeltaTable.forName(spark, quarantine_table)
            (delta_obj.alias("t")
             .merge(quarantine_df.alias("s"),
                    "t.txn_id = s.txn_id AND t._silver_batch_id = s._silver_batch_id")
             .whenNotMatchedInsertAll()
             .execute())

    except Exception as e:
        send_alert(silver_batch_id, "silver_quarantine", str(e))
        raise

    # --- SLA metrics update ---
    total_rows = df.count()
    clean_rows = clean_df.count()
    quarantine_rows = quarantine_df.count()
    update_sla_metrics(silver_batch_id, "silver", total_rows, clean_rows, quarantine_rows, "SUCCESS")

    logger.info("Silver ingestion complete")

# -------------------------
# 5. CONFIG & EXECUTE
# -------------------------
silver_config = {
    "table_name": "workspace.default.silver_transactions",
    "quarantine_table": "workspace.default.silver_transactions_quarantine",
    "source_table": "workspace.default.bronze_transactions",
    "silver_batch_id": "SILVER_2026_02",
    "repartition_num": 5
}

run_silver_ingestion(spark, silver_config)