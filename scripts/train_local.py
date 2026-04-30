from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

spark = SparkSession.builder.appName('AgriTrain').master('local[*]').getOrCreate()

# Chemin du fichier CSV (monté dans le conteneur)
csv_path = '/home/jovyan/work/data/bronze/agri_parcelles_brut.csv'
df = spark.read.csv(csv_path, header=True, inferSchema=True)

print(f"📊 Données chargées : {df.count()} lignes")

# Nettoyage (supprimer les lignes avec valeurs nulles)
df = df.dropna()
print(f"🧹 Après nettoyage : {df.count()} lignes")

# Feature engineering : stress hydrique
df = df.withColumn('stress_hydrique', col('pluviometrie_annuelle') / 800)

# Colonnes numériques et catégorielles
num_cols = [
    'ph_sol', 'pluviometrie_annuelle', 'temperature_moy_celsius',
    'humidite_relative_pct', 'teneur_matiere_organique_pct',
    'surface_hectares', 'annee', 'stress_hydrique'
]
cat_cols = ['region', 'culture', 'type_sol']

# Indexation des variables catégorielles
indexers = [StringIndexer(inputCol=c, outputCol=c+'_idx', handleInvalid='skip') for c in cat_cols]

# Assemblage des features
assembler = VectorAssembler(
    inputCols=num_cols + [c+'_idx' for c in cat_cols],
    outputCol='features'
)

# Normalisation
scaler = StandardScaler(inputCol='features', outputCol='scaled_features', withStd=True, withMean=True)

# Modèle Random Forest
rf = RandomForestRegressor(
    featuresCol='scaled_features',
    labelCol='rendement_kg_ha',
    numTrees=100,
    maxDepth=8,
    seed=42
)

# Pipeline
pipeline = Pipeline(stages=indexers + [assembler, scaler, rf])

# Split train/test
train, test = df.randomSplit([0.8, 0.2], seed=42)

# Entraînement
print("🚀 Entraînement du modèle...")
model = pipeline.fit(train)

# Prédictions sur le test
predictions = model.transform(test)

# Évaluation
evaluator = RegressionEvaluator(labelCol='rendement_kg_ha', predictionCol='prediction')
rmse = evaluator.evaluate(predictions, {evaluator.metricName: 'rmse'})
r2 = evaluator.evaluate(predictions, {evaluator.metricName: 'r2'})

print(f"\n📈 Performances :")
print(f"   ✅ RMSE : {rmse:.2f} kg/ha")
print(f"   ✅ R²   : {r2:.4f}")

# Sauvegarde du modèle
model_path = '/home/jovyan/work/models/agri_model'
model.write().overwrite().save(model_path)
print(f"💾 Modèle sauvegardé : {model_path}")

# Prédictions sur tout le jeu de données (pour les recommandations)
full_pred = model.transform(df)
full_pred.select('region', 'culture', 'type_sol', 'prediction', 'rendement_kg_ha') \
         .show(10, truncate=False)

# Sauvegarde des prédictions en CSV (pour pouvoir les utiliser dans HBase/Hive plus tard)
output_path = '/home/jovyan/work/data/silver/predictions.csv'
full_pred.select('region', 'culture', 'type_sol', 'prediction') \
         .write.mode('overwrite').option('header', 'true').csv(output_path)
print(f"📁 Prédictions sauvegardées dans : {output_path}")

spark.stop()
