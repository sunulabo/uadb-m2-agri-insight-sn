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
- Docker Desktop (Windows)
- Python 3.9+
- 16 Go de RAM minimum

### Démarrage de l'infrastructure
```bash
cd docker
docker compose up -d
```

### Installation des dépendances Python
```bash
pip install -r requirements.txt
```

## Utilisation

### 1. Initialisation
```bash
# Créer les tables HBase
python scripts/hbase_setup.py

# Créer les tables Hive (via le conteneur hive-metastore ou beeline)
docker exec -it hive-metastore hive -f /opt/hive/scripts/hive_setup.sql
```

### 2. Simulation des données
```bash
python scripts/generate_agri_data.py
```
Les données sont générées dans `data/bronze/agri_parcelles_brut.csv`.

### 3. Pipeline ML (Entraînement)
```bash
spark-submit scripts/yield_prediction.py --mode train
```

### 4. Dashboard
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
