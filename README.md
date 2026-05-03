# Agri-Insight SN : Optimisation Agricole Intelligente

## Description
Projet Big Data et IA pour la prédiction des rendements agricoles et la génération de recommandations stratégiques au Sénégal.

**Équipe 01 :**
- KANE Abdoul Aziz
- COLY Malick

## Architecture End-to-End
1. **Génération** : Python/Faker (Données de sols et météo).
2. **Validation** : Pandera (Contraintes agronomiques).
3. **Ingestion** : NiFi (Fichier local -> HDFS Bronze).
4. **Transformation** : Spark (Anonymisation SHA-256, Feature Engineering).
5. **Machine Learning** : Spark MLlib (RandomForest Regression).
6. **Stockage** : Hive (Analyses historiques) & HBase (Recommandations temps réel).
7. **Orchestration** : Airflow (MLOps, Drift detection).

## Installation

### Prérequis
- **Docker Desktop** (Windows avec WSL2 recommandé)
- **Python 3.9+**
- **Ressources** : Allouer au moins 12-16 Go de RAM à Docker.

### Démarrage de l'infrastructure
```bash
# Lancement de tous les services (Hadoop, Hive, HBase, Kafka, Spark, NiFi, Airflow)
docker compose up -d
```

### Installation des dépendances Python
```bash
pip install pandas happybase thrift pyhive faker matplotlib seaborn scipy mlflow
```

## Utilisation

### 1. Initialisation (Crucial pour le premier lancement)

#### A. Configurer les permissions HDFS
Hive et NiFi ont besoin de droits d'écriture sur le cluster Hadoop :
```bash
docker exec -it namenode hdfs dfs -mkdir -p /user/hive/warehouse
docker exec -it namenode hdfs dfs -mkdir -p /agri_insight/bronze
docker exec -it namenode hdfs dfs -chmod -R 777 /user
docker exec -it namenode hdfs dfs -chmod -R 777 /agri_insight
```

#### B. Créer le namespace et les tables HBase
```bash
# Création du namespace agri (si non présent)
"create_namespace 'agri'" | docker exec -i hbase hbase shell

# Lancement du script de configuration des tables
python scripts/hbase_setup.py
```

#### C. Créer les schémas Hive
```bash
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 -f /opt/hive/user-scripts/hive_setup.sql
```

### 2. Interfaces de Monitoring
Une fois lancé, accédez aux services via votre navigateur :
- **Hadoop (Filesystem)** : [http://localhost:9870](http://localhost:9870)
- **NiFi (Flux de données)** : [http://localhost:8081/nifi](http://localhost:8081/nifi)
- **Spark (Master)** : [http://localhost:8080](http://localhost:8080)
- **HBase (Master)** : [http://localhost:16010](http://localhost:16010)
- **Airflow (Orchestration)** : [http://localhost:8082](http://localhost:8082)

### 3. Simulation et Pipeline ML
```bash
# Générer des données synthétiques
python scripts/generate_agri_data.py

# Lancer l'entraînement du modèle
spark-submit scripts/yield_prediction.py --mode train
```

### 4. Dashboard de Recommandations
```bash
python dashboard/dashboard_recommandations.py
```

## Structure du Dépôt
- `scripts/` : Scripts de simulation, validation et ML.
- `dags/` : Orchestration MLOps Airflow.
- `docker/` : Infrastructure Docker Compose.
- `dashboard/` : Visualisation et graphiques.
- `data/` : Stockage local temporaire.
- `models/` : Modèles ML sauvegardés.
- `nifi_templates/` : Templates XML pour Apache NiFi.
