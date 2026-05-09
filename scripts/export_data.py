from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("ExportRecommandationsRobust") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

try:
    print("Lecture des tables...")
    silver = spark.table("agri_insight.parcelles_silver")
    gold = spark.table("agri_insight.predictions_gold")

    # Join and aggregation
    # Note: predictions_gold use "prediction", silver use "rendement_kg_ha"
    df = silver.alias("s").join(
        gold.alias("p"), 
        F.col("s.plot_id_secure") == F.col("p.plot_id_secure"), 
        "left"
    ).select(
        F.col("s.region"),
        F.col("s.type_sol"),
        F.col("s.culture"),
        F.col("s.pluviometrie_annuelle"),
        F.col("s.stress_hydrique"),
        F.col("p.prediction").alias("rendement_predit"),
        F.col("s.rendement_kg_ha")
    )

    # Group by region, culture to get averages
    agg_df = df.groupBy("region", "type_sol", "culture").agg(
        F.avg(F.coalesce(F.col("rendement_predit"), F.col("rendement_kg_ha"))).alias("rendement_moyen_predit"),
        F.avg("pluviometrie_annuelle").alias("pluvio_moyenne"),
        F.avg("stress_hydrique").alias("stress_hydrique_moyen")
    )

    # Calculate recommendation status using Window function
    window_region = Window.partitionBy("region")
    
    final_df = agg_df.withColumn(
        "max_rendement_region", 
        F.max("rendement_moyen_predit").over(window_region)
    ).withColumn(
        "statut_recommandation",
        F.when(F.col("rendement_moyen_predit") >= F.col("max_rendement_region") * 0.9, "CULTURE_OPTIMALE")
         .when(F.col("rendement_moyen_predit") >= F.col("max_rendement_region") * 0.7, "CULTURE_VIABLE")
         .otherwise("CULTURE_DECONSEILLE")
    )

    print("Export vers CSV...")
    pdf = final_df.toPandas()
    pdf.to_csv("/opt/scripts/recommandations.csv", index=False)
    print("SUCCESS: Données exportées dans /opt/scripts/recommandations.csv")

except Exception as e:
    print(f"ERROR: {e}")
finally:
    spark.stop()
