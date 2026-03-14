# GOLD LAYER

import logging
from datetime import datetime, timedelta
from pyspark.sql import functions as F, types as T
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# PHASE 1: GOLD CONFIG
gold_config = {
    "dims" : {
        "dim_customers" : ["cust_id", "is_member"]
    },
    "fact_table" : "workspace.default.fact_transactions",
    "source_file" : "workspace.default.silver_transactions",
    "batch_id_gold" : f"GOLD_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
    "buffer" : 1,
    "log_table" : {
        "batch" : "workspace.default.batch_control",
        "sla" : "workspace.default.sla_metrics"
    }
}

# PHASE 2: HELPER UTILS
def upsert_log(spark, table_name, schema, data, keys):
    df = spark.createDataFrame(data, schema)
    if not spark.catalog.tableExists(table_name):
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)

    else:
        dt = DeltaTable.forName(spark, table_name)
        condition = " AND ".join([f"t.{k} = s.{k}" for k in keys])
        (dt.alias("t").merge(df.alias("s"), condition)
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
        
def sync_schema(spark, table_name, new_cols):
    if not spark.catalog.tableExists(table_name):
        return
    
    current_cols = [c.lower() for c in spark.table(table_name).columns]
    for col_name, col_dtype in new_cols.items():
        if col_name.lower() not in current_cols:
            spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({col_name} {col_dtype})")

# PHASE 3: DIM ENGINE
def dim_engine(spark, df, config):
    dim_count = {}
    req_cols = {
        "is_current" : "BOOLEAN",
        "valid_from" : "TIMESTAMP",
        "valid_to" : "TIMESTAMP"
    }

    for dim_tables, dim_cols in config['dims'].items():
        pk = dim_cols[0]
        df_dim = (df.select(*dim_cols)
                    .dropDuplicates([pk])
                    .withColumn("hash_key", F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in dim_cols]), 256)))
        
        if not spark.catalog.tableExists(dim_tables):
            logger.info(f"[CREATE] Creating new dimension table: {dim_tables}")
            (df_dim.withColumn("is_current", F.lit(True))
                   .withColumn("valid_from", F.current_timestamp())
                   .withColumn("valid_to", F.lit(None).cast("timestamp"))
                   .write.format("delta")
                   .saveAsTable(dim_tables))
        
            new_rows = df_dim.count()

        else:
            logger.info(f"[MERGE] Merging existing dimension table: {dim_tables}")
            dt = DeltaTable.forName(spark, dim_tables)
            sync_schema(spark, dim_tables, req_cols)

            df_target = spark.table(dim_tables).filter(F.col("is_current") == True)
            df_changes = (df_dim.alias("s").join(df_target.alias("t"), pk)
                            .filter("s.hash_key <> t.hash_key")
                            .select("s.*"))
            
            update_parts = df_dim.withColumn("merge_key", F.col(pk))
            insert_parts = df_changes.withColumn("merge_key", F.lit(None).cast("string"))
            df_staged = update_parts.unionByName(insert_parts, allowMissingColumns=True)

            updates = {
                "is_current" : F.lit(False),
                "valid_to" : F.current_timestamp()
            }

            inserts = {
                pk : F.col(f"s.{pk}"),
                "hash_key" : F.col(f"s.hash_key"),
                "is_current" : F.lit(True),
                "valid_from" : F.current_timestamp(),
                "valid_to" : F.lit(None).cast("timestamp")
            }

            for c in dim_cols:
                inserts[c] = F.col(f"s.{c}")

            (dt.alias("t").merge(df_staged.alias("s"), f"t.{pk} = s.merge_key")
             .whenMatchedUpdate(
                 condition = "t.is_current = true AND t.hash_key <> s.hash_key",
                 set = updates
             )
             .whenNotMatchedInsert(values = inserts)
             .execute())
            
            new_rows = df_dim.count()

        dim_count[dim_tables] = new_rows

    return dim_count

# PHASE 4: FACT ENGINE
def fact_engine(spark,df, config):
    fact_table = config['fact_table']

    df_clean = df.filter("amount > 0 AND cust_id IS NOT NULL")
    df_quarantine = df.filter("amount < 0 OR cust_id IS NULL")

    clean_count = df_clean.count()
    quarantine_count = df_quarantine.count()

    if clean_count > 0:
        df_fact = df_clean.select(
            F.col("txn_id").cast("STRING"),
            F.col("cust_id").cast("LONG"),
            F.col("amount").cast("DOUBLE"),
            F.col("points").cast("INT"),
            F.col("status").cast("STRING"),
            F.to_date("txn_date").alias("txn_date_key"))
        
        if not spark.catalog.tableExists(fact_table):
            logger.info(f"[CREATE] Creating new fact table: {fact_table}")
            (df_fact.write.format("delta")
                .partitionBy("txn_date_key")
                .saveAsTable(fact_table))
            
        else:
            logger.info(f"[MERGE] Merging existing fact table: {fact_table}")
            dt = DeltaTable.forName(spark, fact_table)
            condition = "t.txn_id = s.txn_id"
            (dt.alias("t").merge(df_fact.alias("s"), condition)
             .whenMatchedUpdateAll()
             .whenNotMatchedInsertAll()
             .execute())
            
    return clean_count, quarantine_count

# PHASE 5: GOLD ENGINE
def gold_engine(spark, config):
    source_file = config['source_file']
    run_batch_id = config['batch_id_gold']
    buffer = config['buffer']
    
    batch_table = config['log_table']['batch']
    sla_table = config['log_table']['sla']

    batch_schema = "batch_id STRING, layer STRING, status STRING, processed_at TIMESTAMP"
    sla_schema = "batch_id STRING, layer STRING, total_rows LONG, clean_rows LONG, quarantine_rows LONG, status STRING, processed_at TIMESTAMP"

    logger.info(f"[START] Gold pipeline started Batch: {run_batch_id}")

    upsert_log(spark, batch_table, batch_schema,
               [(run_batch_id, "GOLD", "STARTED", datetime.now())], ["batch_id", "layer"])
    
    try:
        # PHASE 1: WATERMARK
        df_batch_table = spark.table(batch_table).filter("layer = 'GOLD' AND status = 'SUCCESS'")
        max_timestamp = df_batch_table.agg(F.max("processed_at")).first()[0] if df_batch_table else None

        # PHASE 2: LOAD
        df = spark.table(source_file).filter(F.col("is_deleted") == False)

        if max_timestamp:
            last_wm = max_timestamp - timedelta(hours = buffer)
            logger.info(f"[CHECK] Watermark found. Processing data after {last_wm}")
            df = df.filter(F.col("_processed_at") > last_wm)

        # PHASE 3: EARLY EXIT
        if df.isEmpty():
            logger.info(f"[SKIP] No new data. Logging as SKIPPED")
            upsert_log(spark, batch_table, batch_schema,
               [(run_batch_id, "GOLD", "SKIPPED", datetime.now())], ["batch_id", "layer"])
            return

        # PHASE 4: TRANSFORM
        logger.info(f"[TRANSFORM] Transforming dimension data...")
        dim_new = dim_engine(spark, df, config)
        logger.info(f"[TRANSFORM] Transforming fact data...")
        fact_new_clean, fact_new_quarantine = fact_engine(spark,df, config)

        # PHASE 5: SLA CALCULATION
        dim_new_rows = sum(dim_new.values())
        t_rows = dim_new_rows + fact_new_clean + fact_new_quarantine
        c_rows = fact_new_clean
        q_rows = fact_new_quarantine

        # PHASE 6: LOG SUCCESS
        upsert_log(spark, batch_table, batch_schema,
               [(run_batch_id, "GOLD", "SUCCESS", datetime.now())], ["batch_id", "layer"])
        upsert_log(spark, sla_table, sla_schema,
               [(run_batch_id, "GOLD", t_rows, c_rows, q_rows, "SUCCESS", datetime.now())], ["batch_id", "layer"])
        logger.info(f"[SUCCESS] Gold completed. Total: {t_rows}, CLean: {c_rows}, Quarantine: {q_rows}")

    except Exception as e:
        upsert_log(spark, batch_table, batch_schema,
               [(run_batch_id, "GOLD", "FAILED", datetime.now())], ["batch_id", "layer"])
        upsert_log(spark, sla_table, sla_schema,
               [(run_batch_id, "GOLD", 0, 0, 0, "FAILED", datetime.now())], ["batch_id", "layer"])
        logger.error(f"[ERROR] Gold failed: {e}")
        raise e

# PHASE 6: RUN GOLD
if __name__ == "__main__":
    gold_engine(spark, gold_config)