import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline, PipelineModel

# Configuration
SALT = os.environ.get("AGRI_SECRET_SALT", "UADB_AGRI_2025")
MODEL_PATH = "hdfs://namenode:8020/agri_insight/models/yield_rf_latest"

def main():
    spark = (
        SparkSession.builder.appName("AgriInsight_FullPipeline")
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
        .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse")
        .enableHiveSupport()
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")

    # --- PARTIE 1 : INGESTION (BRONZE -> SILVER) ---
    print(">>> Demarrage de l'ingestion Silver...")
    
    # Lecture Bronze
    try:
        df_bronze = spark.table("agri_insight.parcelles_bronze")
        if df_bronze.count() == 0:
            print("!!! Table Bronze vide. Verifiez HDFS.")
            return
    except Exception as e:
        print(f"!!! Erreur lecture Bronze: {e}")
        return
    
    # UDFs pour features
    @F.udf(returnType=StringType())
    def stress_hydrique(pluvio, temp):
        if pluvio is None or temp is None: return "INCONNU"
        return "HAUT" if pluvio < 500 and temp > 35 else "BAS"

    @F.udf(returnType=DoubleType())
    def score_ph(ph):
        if ph is None: return 0.0
        return 1.0 if 6.0 <= ph <= 7.5 else 0.5

    # Transformation Silver
    df_silver = df_bronze.withColumn("stress_hydrique", stress_hydrique("pluviometrie_annuelle", "temperature_moy_celsius")) \
                         .withColumn("ph_optimal_score", score_ph("ph_sol")) \
                         .withColumn("plot_id_secure", F.sha2(F.concat("parcel_id", F.lit(SALT)), 256))

    # Sauvegarde Silver
    spark.sql("DROP TABLE IF EXISTS agri_insight.parcelles_silver")
    df_silver.write.format("parquet").saveAsTable("agri_insight.parcelles_silver")
    print(">>> Table Silver creee avec succes.")

    # --- PARTIE 2 : MACHINE LEARNING (TRAIN) ---
    print(">>> Demarrage de l'entrainement du modele...")
    
    data = spark.table("agri_insight.parcelles_silver")
    
    # Feature Engineering
    indexer = StringIndexer(inputCol="culture", outputCol="culture_idx", handleInvalid="keep")
    assembler = VectorAssembler(
        inputCols=["ph_sol", "pluviometrie_annuelle", "temperature_moy_celsius", "culture_idx", "surface_hectares"],
        outputCol="features",
        handleInvalid="skip"
    )
    
    rf = RandomForestRegressor(featuresCol="features", labelCol="rendement_kg_ha", numTrees=20)
    
    pipeline = Pipeline(stages=[indexer, assembler, rf])
    
    # Train/Test Split
    (train_data, test_data) = data.randomSplit([0.8, 0.2], seed=42)
    
    if train_data.count() == 0:
        print("!!! Pas assez de donnees pour l'entrainement.")
        return
        
    model = pipeline.fit(train_data)
    
    # Evaluation
    predictions = model.transform(test_data)
    if test_data.count() > 0:
        evaluator = RegressionEvaluator(labelCol="rendement_kg_ha", predictionCol="prediction", metricName="rmse")
        rmse = evaluator.evaluate(predictions)
        print(f">>> Modele entraine. RMSE: {rmse:.2f}")
    
    # Sauvegarde du modele
    model.write().overwrite().save(MODEL_PATH)
    print(f">>> Modele sauvegarde sur HDFS : {MODEL_PATH}")

    # --- PARTIE 3 : INFERENCE (SILVER -> GOLD) ---
    print(">>> Generation des predictions Gold...")
    
    loaded_model = PipelineModel.load(MODEL_PATH)
    all_data = spark.table("agri_insight.parcelles_silver")
    
    results = loaded_model.transform(all_data)
    
    gold_df = results.select(
        "plot_id_secure",
        "region",
        "culture",
        F.col("prediction").alias("rendement_predit"),
        F.current_timestamp().alias("prediction_ts")
    )
    
    spark.sql("DROP TABLE IF EXISTS agri_insight.predictions_gold")
    gold_df.write.format("parquet").saveAsTable("agri_insight.predictions_gold")
    print(">>> Table Gold creee avec succes. Pipeline terminee !")

if __name__ == "__main__":
    main()
