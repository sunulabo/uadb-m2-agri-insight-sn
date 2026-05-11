import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat, lit, coalesce, when
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import (
    StringIndexer,
    OneHotEncoder,
    VectorAssembler,
    StandardScaler,
    Imputer,
)
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline, PipelineModel
import argparse

SALT = os.environ.get("AGRI_SECRET_SALT", "default_salt")
MODEL_PATH = "hdfs:///agri_insight/models/yield_rf_latest"

parser = argparse.ArgumentParser()
parser.add_argument("--mode", choices=["train", "infer"], default="train")
parser.add_argument("--input-table", default="agri_insight.parcelles_silver")
args = parser.parse_args()

spark = (
    SparkSession.builder.appName(f"AgriInsight_YieldPrediction_{args.mode}")
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ── Read from Silver ─────────────────────────────────────
print("Tables in agri_insight:", [t.name for t in spark.catalog.listTables("agri_insight")])
gold_df = spark.table(args.input_table)

# Fill nulls for ML
gold_df = gold_df.fillna({
    "pluviometrie_annuelle": 600.0,
    "rendement_kg_ha": 0.0
})

# ── Pipeline MLlib ───────────────────────────────────────────────────────
NUM_COLS = [
    "ph_sol",
    "pluviometrie_annuelle",
    "temperature_moy_celsius",
    "humidite_relative_pct",
    "teneur_matiere_organique_pct",
    "surface_hectares",
    "stress_hydrique",
    "ph_optimal_score",
    "indice_fertilite",
    "annee",
]
CAT_COLS = ["region", "culture", "type_sol"]

for c in NUM_COLS:
    gold_df = gold_df.withColumn(c, col(c).cast(DoubleType()))

gold_df = gold_df.withColumn("label", col("rendement_kg_ha").cast(DoubleType()))

indexers = [
    StringIndexer(inputCol=c, outputCol=c + "_idx", handleInvalid="skip")
    for c in CAT_COLS
]
encoders = [
    OneHotEncoder(
        inputCols=[idx.getOutputCol()],
        outputCols=[idx.getOutputCol().replace("_idx", "_ohe")],
    )
    for idx in indexers
]
ohe_cols = [enc.getOutputCols()[0] for enc in encoders]

assembler = VectorAssembler(
    inputCols=NUM_COLS + ohe_cols, outputCol="unscaled_features", handleInvalid="skip"
)

scaler = StandardScaler(
    inputCol="unscaled_features", outputCol="features", withStd=True, withMean=True
)

rf = RandomForestRegressor(
    featuresCol="features", labelCol="label", numTrees=150, maxDepth=10, seed=42
)

if args.mode == "train":
    ml_pipeline = Pipeline(stages=indexers + encoders + [assembler, scaler, rf])
    train, test = gold_df.randomSplit([0.8, 0.2], seed=42)
    model = ml_pipeline.fit(train)
    preds = model.transform(test)
    ev_rmse = RegressionEvaluator(
        labelCol="label", predictionCol="prediction", metricName="rmse"
    )
    ev_r2 = RegressionEvaluator(
        labelCol="label", predictionCol="prediction", metricName="r2"
    )
    rmse = ev_rmse.evaluate(preds)
    r2 = ev_r2.evaluate(preds)
    print(f"RandomForest | RMSE : {rmse:.2f} kg/ha | R² : {r2:.4f}")

    # Feature importance
    importances = model.stages[-1].featureImportances.toArray()
    names = NUM_COLS + CAT_COLS
    for nm, imp in sorted(
        zip(names[: len(importances)], importances), key=lambda x: -x[1]
    )[:8]:
        print(f" {nm:40s} : {imp:.4f}")

    model.write().overwrite().save(MODEL_PATH)
    print(f"Modèle sauvegardé : {MODEL_PATH}")

else:  # mode infer
    model = PipelineModel.load(MODEL_PATH)
    preds = model.transform(gold_df)
    preds.select("plot_id_secure", "region", "culture", "prediction").write.mode(
        "overwrite"
    ).saveAsTable("agri_insight.predictions_gold")
    print(f"Prédictions écrites : {preds.count()} lignes")

spark.stop()
