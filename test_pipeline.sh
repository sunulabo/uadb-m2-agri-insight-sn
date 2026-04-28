#!/bin/bash
# test_pipeline.sh — Guide de test end-to-end Agri-Insight SN

# 1. Démarrage infrastructure
docker compose -f docker/docker-compose.yml up -d zookeeper && sleep 10
docker compose -f docker/docker-compose.yml up -d kafka nifi hbase hive-metastore namenode && sleep 30
docker compose -f docker/docker-compose.yml up -d spark-master spark-worker airflow

# 2. Initialiser HBase
python scripts/hbase_setup.py
# Attendu : Table agri:recommandations créée ✓
# Table agri:predictions créée ✓
# Table agri:drift_monitor créée ✓

# 3. Générer les données
python scripts/generate_agri_data.py
# Attendu : Généré : ~4200 parcelles ✓

# 4. Vérifier les données dans HDFS
docker exec namenode hdfs dfs -ls /agri_insight/bronze/
docker exec namenode hdfs dfs -cat /agri_insight/bronze/agri_parcelles_brut.csv | head -3

# 5. Créer les tables Hive
docker exec hive-metastore hive -f /opt/hive/scripts/hive_setup.sql
# Valider les volumes
docker exec hive-metastore beeline -u jdbc:hive2://localhost:10000 \
 -e 'SELECT * FROM agri_insight.vue_recommandations LIMIT 5;'

# 6. Entraîner le modèle
docker exec spark-master spark-submit \
 --master spark://spark-master:7077 \
 /opt/scripts/yield_prediction.py --mode train
# Attendu : RandomForest | RMSE : xxx kg/ha | R² : 0.xx

# 7. Lancer l'inférence
docker exec spark-master spark-submit \
 --master spark://spark-master:7077 \
 /opt/scripts/yield_prediction.py --mode infer

# 8. Vérifier les recommandations HBase
docker exec hbase hbase shell <<EOF
scan 'agri:recommandations', {LIMIT => 5}
EOF

# 9. Générer le dashboard
python dashboard/dashboard_recommandations.py
# Attendu : dashboard_agri_insight.png sauvegardé ✓
