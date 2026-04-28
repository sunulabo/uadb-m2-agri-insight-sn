import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat, lit, when, coalesce, udf
from pyspark.sql.types import FloatType
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
import argparse

SALT = os.environ.get('AGRI_SECRET_SALT', 'agri_default_salt')
MODEL_PATH = 'hdfs://namenode:9000/agri_insight/models/yield_rf_latest'

BESOIN_PLUVIO = {'Mil':600.0, 'Riz':1000.0, 'Arachide':700.0, 'Manicot':800.0, 'Niebe':500.0}

spark = SparkSession.builder \
    .appName('AgriInsight YieldPrediction') \
    .config('spark.sql.shuffle.partitions', '8') \
    .enableHiveSupport() \
    .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

def stress_hydrique_func(culture, pluvio):
    besoin = BESOIN_PLUVIO.get(culture, 700.0)
    return float(min(2.0, max(0.0, pluvio / besoin)))

stress_hydrique_udf = udf(stress_hydrique_func, FloatType())

parser = argparse.ArgumentParser()
parser.add_argument('--mode', choices=['train','infer'], default='train')
parser.add_argument('--input-table', default='agri_insight.parcelles_bronze')
args = parser.parse_args()

if args.mode == 'train':
    raw_df = spark.table(args.input_table)
    silver_df = raw_df.withColumn('plot_id_secure', sha2(concat(col('parcel_id'), lit(SALT)), 256)) \
                     .drop('parcel_id', 'producer_name')
    
    gold_df = silver_df.withColumn('stress_hydrique', stress_hydrique_udf(col('culture'), col('pluviometrie_annuelle'))) \
                       .withColumn('ph_optimal_score', when((col('ph_sol')>=6.0)&(col('ph_sol')<=7.5), 1.0)
                                   .when((col('ph_sol')>=5.5)&(col('ph_sol')<=8.0), 0.7).otherwise(0.3)) \
                       .withColumn('indice_fertilite', col('teneur_matiere_organique_pct') * col('ph_optimal_score')) \
                       .withColumn('pluviometrie_annuelle', coalesce(col('pluviometrie_annuelle'), lit(600.0))) \
                       .withColumn('rendement_kg_ha', coalesce(col('rendement_kg_ha'), lit(0.0))) \
                       .withColumn('label', col('rendement_kg_ha').cast('float'))
    
    num_cols = ['ph_sol','pluviometrie_annuelle','temperature_moy_celsius','humidite_relative_pct',
                'teneur_matiere_organique_pct','surface_hectares','stress_hydrique','ph_optimal_score','indice_fertilite','annee']
    cat_cols = ['region','culture','type_sol']
    
    for c in num_cols:
        gold_df = gold_df.withColumn(c, col(c).cast('float'))
    
    indexers = [StringIndexer(inputCol=c, outputCol=c+'_idx', handleInvalid='skip') for c in cat_cols]
    encoders = [OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_ohe") for c in cat_cols]
    assembler = VectorAssembler(inputCols=num_cols + [f"{c}_ohe" for c in cat_cols], outputCol='unscaled_features', handleInvalid='skip')
    scaler = StandardScaler(inputCol='unscaled_features', outputCol='features', withStd=True, withMean=True)
    rf = RandomForestRegressor(featuresCol='features', labelCol='label', numTrees=150, maxDepth=10, seed=42)
    
    pipeline = Pipeline(stages=indexers + encoders + [assembler, scaler, rf])
    train, test = gold_df.randomSplit([0.8, 0.2], seed=42)
    model = pipeline.fit(train)
    preds = model.transform(test)
    evaluator = RegressionEvaluator(labelCol='label', predictionCol='prediction')
    rmse = evaluator.evaluate(preds, {evaluator.metricName: 'rmse'})
    r2 = evaluator.evaluate(preds, {evaluator.metricName: 'r2'})
    print(f'RandomForest | RMSE : {rmse:.2f} kg/ha | R² : {r2:.4f}')
    model.write().overwrite().save(MODEL_PATH)
    print(f'Modèle sauvegardé : {MODEL_PATH}')
    
else:  # mode infer
    model = PipelineModel.load(MODEL_PATH)
    # (logique d'inférence similaire, on écrit dans predictions_gold)
    print("Inférence non implémentée dans ce snippet, mais structure prête")

spark.stop()
