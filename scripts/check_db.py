from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CheckSchema") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

try:
    print("--- SCHEMA parcelles_silver ---")
    spark.table("agri_insight.parcelles_silver").printSchema()
    
    print("--- SCHEMA predictions_gold ---")
    spark.table("agri_insight.predictions_gold").printSchema()
    
    print("--- TEST VIEW vue_recommandations ---")
    try:
        spark.table("agri_insight.vue_recommandations").show(5)
    except Exception as ve:
        print(f"VIEW ERROR: {ve}")

except Exception as e:
    print(f"GENERAL ERROR: {e}")
finally:
    spark.stop()
