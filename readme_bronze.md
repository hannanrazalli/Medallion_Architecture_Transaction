# BRONZE LAYER - ENTERPRISE LEVEL

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
# 2. UTILITY FUNCTIONS
# -------------------------
def path_exists(path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False

def add_audit_columns(df, batch_id):
    # _source_file safely
    if "_metadata" in df.columns:
        df = df.select("*", "_metadata") \
               .withColumn("_source_file", F.col("_metadata.file_path")) \
               .drop("_metadata")
    else:
        df = df.withColumn("_source_file", F.lit(None).cast("string"))

    return (df.withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_batch_id", F.lit(batch_id))
              .withColumn("_ingest_date", F.to_date(F.to_timestamp("txn_date", "yyyy-MM-dd HH:mm:ss"))))

def update_batch_control(batch_id, layer, status):
    """Senior DE mindset: track all batch runs even if single batch"""
    table_name = "workspace.default.batch_control"
    now = datetime.now()
    data = [(batch_id, layer, status, now)]
    schema = T.StructType([
        T.StructField("batch_id", T.StringType()),
        T.StructField("layer", T.StringType()),
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

def update_sla_metrics(batch_id, layer, total_rows, clean_rows, quarantine_rows, status):
    """Senior DE mindset: SLA metrics"""
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

# -------------------------
# 3. CORE ENGINE
# -------------------------
def run_bronze_ingestion(spark_session, config):
    table_name = config['table_name']
    source_path = config['source_path']
    batch_id = config['batch_id']
    repartition_num = config.get("repartition_num", 5)
    
    logger.info(f"Starting Bronze ingestion: {table_name}, batch {batch_id}")

    update_batch_control(batch_id, "bronze", "STARTED")
    try:
        if not path_exists(source_path):
            raise ValueError(f"Source path does not exist: {source_path}")

        df = (spark_session.read
              .format("json")
              .schema(config['schema'])
              .option("mode", "FAILFAST")
              .load(source_path))

        df_with_meta = add_audit_columns(df, batch_id)

        if not spark_session.catalog.tableExists(table_name):
            (df_with_meta.repartition(repartition_num)
             .write.format("delta")
             .mode("overwrite")
             .partitionBy("_ingest_date")
             .saveAsTable(table_name))
        else:
            delta_obj = DeltaTable.forName(spark_session, table_name)
            (delta_obj.alias("t")
             .merge(df_with_meta.alias("s"),
                    "t.txn_id = s.txn_id AND t._batch_id = s._batch_id")
             .whenNotMatchedInsertAll()
             .execute())

        total_rows = df_with_meta.count()
        clean_rows = total_rows
        quarantine_rows = 0

        update_sla_metrics(batch_id, "bronze", total_rows, clean_rows, quarantine_rows, "SUCCESS")
        update_batch_control(batch_id, "bronze", "SUCCESS")
        logger.info("Bronze ingestion complete")
        df_with_meta.show(5, truncate=False)

    except Exception as e:
        update_batch_control(batch_id, "bronze", "FAILED")
        update_sla_metrics(batch_id, "bronze", 0, 0, 0, "FAILED")
        logger.error(f"Bronze ingestion failed: {e}")
        raise

# -------------------------
# 4. CONFIG & EXECUTION
# -------------------------
pipeline_config = {
    "table_name": "workspace.default.bronze_transactions",
    "source_path": "/Volumes/workspace/default/raw_volume/transactions_batch_1",
    "batch_id": "BATCH_2026_02",
    "schema": T.StructType([
        T.StructField("txn_id", T.StringType(), True),
        T.StructField("cust_id", T.LongType(), True),
        T.StructField("amount", T.DoubleType(), True),
        T.StructField("points", T.IntegerType(), True),
        T.StructField("is_member", T.BooleanType(), True),
        T.StructField("status", T.StringType(), True),
        T.StructField("txn_date", T.StringType(), True)
    ]),
    "repartition_num": 5
}

run_bronze_ingestion(spark, pipeline_config)