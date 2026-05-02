from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, row_number
from pyspark.sql.window import Window
import happybase
import pandas as pd
import os

# --- Charger les prédictions enrichies ---
spark = SparkSession.builder.appName('HBaseRecommendations').master('local[*]').getOrCreate()
df = spark.read.csv('data/silver/predictions_enriched.csv', header=True, inferSchema=True)
print(f"Prédictions chargées : {df.count()} lignes")

# --- Agrégation par région, culture, type_sol ---
agg = df.groupBy('region', 'culture', 'type_sol').agg(avg('prediction').alias('rendement_moyen')).orderBy('region', 'type_sol', col('rendement_moyen').desc())

# --- Pour chaque région+sol, identifier la culture optimale (rendement max) ---
window = Window.partitionBy('region', 'type_sol').orderBy(col('rendement_moyen').desc())
best = agg.withColumn('rank', row_number().over(window)).filter(col('rank') == 1).drop('rank')

print("\n📋 Cultures optimales par région et type de sol :")
best.show(truncate=False)

# --- Connexion HBase ---
conn = happybase.Connection('hbase', port=9090, timeout=10000)
conn.open()
table_name = b'agri:recommendations'
if table_name not in conn.tables():
    conn.create_table(table_name, {'meta': dict(), 'predict': dict(), 'conseil': dict()})
table = conn.table(table_name)

# --- Écriture des recommandations ---
for row in best.collect():
    region = row['region']
    sol = row['type_sol']
    culture = row['culture']
    rendement = round(row['rendement_moyen'], 1)
    key = f"{region}_{sol}".encode()
    # Message textuel contextualisé
    message = f"Dans la zone de {region}, sol {sol}, la culture optimale est {culture} (rendement prédit {rendement} kg/ha)."
    table.put(key, {
        b'meta:region': region.encode(),
        b'meta:type_sol': sol.encode(),
        b'predict:culture': culture.encode(),
        b'predict:rendement': str(rendement).encode(),
        b'conseil:message': message.encode()
    })
    print(f"✅ Recommandation : {region} / {sol} -> {culture} ({rendement} kg/ha)")

conn.close()
print("\n🎯 Toutes les recommandations ont été écrites dans HBase (table agri:recommendations).")

# --- Sauvegarde locale CSV pour le rapport ---
best_pd = best.toPandas()
best_pd.to_csv('data/silver/recommandations.csv', index=False)
print("📁 Sauvegarde CSV : data/silver/recommandations.csv")

spark.stop()
