from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat, lit, udf, when, coalesce
from pyspark.sql.types import FloatType
import os

SALT = os.environ.get("AGRI_SECRET_SALT", "UADB_AGRI_2025")

spark = (
    SparkSession.builder.appName("AgriInsight_BronzeToSilver")
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse")
    .enableHiveSupport()
    .getOrCreate()
)

# 1. Read from Bronze (External Table)
bronze_df = spark.table("agri_insight.parcelles_bronze")

# 2. Anonymization (Privacy Layer)
silver_df = bronze_df.withColumn(
    "plot_id_secure", sha2(concat(col("parcel_id"), lit(SALT)), 256)
).drop("parcel_id", "producer_name")

# 3. Feature Engineering
BESOIN_PLUVIO = {
    "Mil": 600.0,
    "Riz": 1000.0,
    "Arachide": 700.0,
    "Manioc": 800.0,
    "Niebe": 500.0,
}
bc_besoin = spark.sparkContext.broadcast(BESOIN_PLUVIO)

@udf(returnType=FloatType())
def stress_hydrique(culture, pluvio):
    if pluvio is None:
        return 0.0
    besoin = bc_besoin.value.get(culture, 700.0)
    return float(min(2.0, max(0.0, pluvio / (besoin if besoin > 0 else 700.0))))

gold_prep_df = (
    silver_df
    .withColumn("stress_hydrique", stress_hydrique(col("culture"), col("pluviometrie_annuelle")))
    .withColumn(
        "ph_optimal_score",
        when((col("ph_sol") >= 6.0) & (col("ph_sol") <= 7.5), lit(1.0))
        .when((col("ph_sol") >= 5.5) & (col("ph_sol") <= 8.0), lit(0.7))
        .otherwise(lit(0.3)),
    )
    .withColumn("indice_fertilite", col("teneur_matiere_organique_pct") * col("ph_optimal_score"))
    .withColumn("region_part", col("region")) # For partitioning
)

# 4. Write to Silver Table
spark.sql("DROP TABLE IF EXISTS agri_insight.parcelles_silver")
gold_prep_df.createOrReplaceTempView("temp_silver")
spark.sql("""
    CREATE TABLE agri_insight.parcelles_silver 
    USING parquet 
    PARTITIONED BY (region_part) 
    AS SELECT * FROM temp_silver
""")

print("Tables in agri_insight after creation:", [t.name for t in spark.catalog.listTables("agri_insight")])
print(f"Ingestion Silver terminée : {gold_prep_df.count()} lignes traitées.")
spark.stop()
