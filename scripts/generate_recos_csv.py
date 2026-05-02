from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, row_number, desc
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('Recos').master('local[*]').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

print("📖 Chargement des prédictions enrichies...")
df = spark.read.csv('data/silver/predictions_enriched.csv', header=True, inferSchema=True)
print(f"✅ {df.count()} lignes chargées")

# Agrégation par région, culture, type_sol
agg = df.groupBy('region', 'culture', 'type_sol').agg(avg('prediction').alias('rendement_moyen'))

# Pour chaque région+sol, garder la culture avec le meilleur rendement
window = Window.partitionBy('region', 'type_sol').orderBy(desc('rendement_moyen'))
best = agg.withColumn('rn', row_number().over(window)).filter(col('rn') == 1).drop('rn')

print("\n🌾 **Cultures optimales par région et type de sol** :")
best.show(truncate=False)

# Sauvegarde CSV
best.coalesce(1).write.mode('overwrite').option('header', 'true').csv('data/silver/meilleures_cultures')
import subprocess, glob, os
csv_files = glob.glob('data/silver/meilleures_cultures/*.csv')
if csv_files:
    subprocess.run(f'mv {csv_files[0]} data/silver/recommandations_cultures.csv', shell=True)
    subprocess.run('rm -rf data/silver/meilleures_cultures', shell=True)

print("\n📁 Fichier CSV sauvegardé : data/silver/recommandations_cultures.csv")

# --- Générer les recommandations textuelles ---
print("\n📝 Recommandations agricoles contextualisées :")
best_pd = best.toPandas()
for _, row in best_pd.iterrows():
    msg = f"Dans la région de {row['region']}, sol {row['type_sol']}, la culture optimale est {row['culture']} (rendement prédit {row['rendement_moyen']:.0f} kg/ha)."
    print(f"  → {msg}")

spark.stop()
