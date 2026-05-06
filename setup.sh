#!/bin/bash
# setup.sh — Agri-Insight SN

# Vérification environnement
java -version # Attendu : openjdk 11.x
docker --version # Docker 24.x minimum
free -h # Minimum 14 Go disponibles

# Installation dépendances
python3.9 -m venv venv_agri && source venv_agri/bin/activate
pip install -r requirements.txt

# Démarrage infrastructure
mkdir -p data/bronze data/silver data/gold nifi_templates dags models
docker compose -f docker-compose.yml up -d zookeeper && sleep 10
docker compose -f docker-compose.yml up -d kafka nifi hbase hive-metastore namenode && sleep 30
docker compose -f docker-compose.yml up -d spark-master spark-worker airflow

# Initialiser HBase (une seule fois)
python scripts/hbase_setup.py

# Interfaces web :
# NiFi : http://localhost:8081
# Spark : http://localhost:8080
# HBase : http://localhost:16010
# Airflow : http://localhost:8082
# HDFS : http://localhost:9870
