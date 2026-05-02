from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat, lit, when
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
import os
import subprocess
import glob

SALT = os.environ.get('AGRI_SECRET_SALT', 'agri_default_salt_2025')
print(f"🔐 Utilisation du sel pour anonymisation (longueur : {len(SALT)})")

spark = SparkSession.builder.appName('AgriTrain').master('local[*]').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

csv_path = 'data/bronze/agri_parcelles_brut.csv'
df = spark.read.csv(csv_path, header=True, inferSchema=True)
print(f"📊 Données brutes : {df.count()} lignes")

# --- Anonymisation SHA-256 (Privacy by Design) ---
df = df.withColumn('plot_id_secure', sha2(concat(col('parcel_id'), lit(SALT)), 256)) \
       .drop('parcel_id', 'producer_name')
print(f"🔒 PII supprimées, anonymisation appliquée. Colonnes : {df.columns}")

# --- Nettoyage (pas de colonne age) ---
essential_cols = ['pluviometrie_annuelle', 'ph_sol', 'teneur_matiere_organique_pct', 'rendement_kg_ha']
df = df.dropna(subset=essential_cols)
print(f"🧹 Après suppression des NULL : {df.count()} lignes")

# --- Feature engineering complet (3 features dérivées) ---
df = df.withColumn('stress_hydrique', col('pluviometrie_annuelle') / 800)

df = df.withColumn('ph_optimal_score',
    when((col('ph_sol') >= 6.0) & (col('ph_sol') <= 7.5), 1.0)
    .when((col('ph_sol') >= 5.5) & (col('ph_sol') <= 8.0), 0.7)
    .otherwise(0.3))

df = df.withColumn('indice_fertilite', col('teneur_matiere_organique_pct') * col('ph_optimal_score'))
print("✅ Features dérivées ajoutées : stress_hydrique, ph_optimal_score, indice_fertilite")

# --- Préparation pour MLlib ---
num_cols = [
    'ph_sol', 'pluviometrie_annuelle', 'temperature_moy_celsius',
    'humidite_relative_pct', 'teneur_matiere_organique_pct',
    'surface_hectares', 'annee', 'stress_hydrique',
    'ph_optimal_score', 'indice_fertilite'
]
cat_cols = ['region', 'culture', 'type_sol']

indexers = [StringIndexer(inputCol=c, outputCol=c+'_idx', handleInvalid='skip') for c in cat_cols]
assembler = VectorAssembler(
    inputCols=num_cols + [c+'_idx' for c in cat_cols],
    outputCol='features',
    handleInvalid='skip'
)
scaler = StandardScaler(inputCol='features', outputCol='scaled_features', withStd=True, withMean=True)
rf = RandomForestRegressor(
    featuresCol='scaled_features',
    labelCol='rendement_kg_ha',
    numTrees=100,
    maxDepth=8,
    seed=42
)

pipeline = Pipeline(stages=indexers + [assembler, scaler, rf])

train, test = df.randomSplit([0.8, 0.2], seed=42)
print("🚀 Entraînement du modèle Random Forest...")
model = pipeline.fit(train)

predictions = model.transform(test)
evaluator = RegressionEvaluator(labelCol='rendement_kg_ha', predictionCol='prediction')
rmse = evaluator.evaluate(predictions, {evaluator.metricName: 'rmse'})
r2 = evaluator.evaluate(predictions, {evaluator.metricName: 'r2'})
print(f"\n📈 Performances : RMSE = {rmse:.2f} kg/ha, R² = {r2:.4f}")

# --- Prédictions enrichies ---
full_pred = model.transform(df)
pred_df = full_pred.select(
    'plot_id_secure', 'region', 'culture', 'type_sol', 
    'pluviometrie_annuelle', 'stress_hydrique', 'ph_optimal_score',
    'indice_fertilite', 'prediction'
)

output_dir = 'data/silver/predictions_enriched'
pred_df.coalesce(1).write.mode('overwrite').option('header', 'true').csv(output_dir)

# Déplacer le CSV unique
csv_files = glob.glob(f'{output_dir}/*.csv')
if csv_files:
    subprocess.run(f'mv {csv_files[0]} data/silver/predictions_enriched.csv', shell=True)
subprocess.run(f'rm -rf {output_dir}', shell=True)

print(f"✅ Prédictions enrichies sauvegardées : data/silver/predictions_enriched.csv")
print(f"   (contient {full_pred.count()} lignes avec features dérivées + anonymisation)")

spark.stop()
