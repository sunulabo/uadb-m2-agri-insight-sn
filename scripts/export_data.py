from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .appName("ExportRecommandations") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

try:
    df = spark.sql("SELECT * FROM agri_insight.vue_recommandations")
    # Conversion en Pandas pour un CSV propre (facile à lire par le dashboard)
    pdf = df.toPandas()
    pdf.to_csv("/opt/scripts/recommandations.csv", index=False)
    print("SUCCESS: Données exportées dans /opt/scripts/recommandations.csv")
except Exception as e:
    print(f"ERROR: {e}")
finally:
    spark.stop()
