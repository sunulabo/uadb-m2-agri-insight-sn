SET hive.execution.engine=mr;
CREATE DATABASE IF NOT EXISTS agri_insight 
COMMENT 'Pipeline Agri-Insight SN — UADB 2025-2026'
LOCATION 'hdfs://namenode:8020/user/hive/warehouse/agri_insight.db';

USE agri_insight;

DROP TABLE IF EXISTS predictions_gold;
DROP TABLE IF EXISTS parcelles_silver;
DROP TABLE IF EXISTS parcelles_bronze;
-- ── BRONZE : données brutes ingérées par NiFi ───────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS parcelles_bronze (
    parcel_id STRING,
    producer_name STRING,
    region STRING,
    culture STRING,
    type_sol STRING,
    ph_sol DOUBLE,
    pluviometrie_annuelle DOUBLE,
    temperature_moy_celsius DOUBLE,
    humidite_relative_pct DOUBLE,
    teneur_matiere_organique_pct DOUBLE, 
    surface_hectares DOUBLE,
    annee INT,
    rendement_kg_ha DOUBLE
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION 'hdfs://namenode:8020/agri_insight/bronze/' TBLPROPERTIES ('skip.header.line.count' = '1');
-- ── SILVER : données anonymisées + features dérivées ───────────────────
CREATE TABLE IF NOT EXISTS parcelles_silver (
    plot_id_secure STRING COMMENT 'SHA-256 — jamais parcel_id brut',
    region STRING,
    culture STRING,
    type_sol STRING,
    ph_sol DOUBLE,
    pluviometrie_annuelle DOUBLE,
    temperature_moy_celsius DOUBLE,
    humidite_relative_pct DOUBLE,
    teneur_matiere_organique_pct DOUBLE,
    surface_hectares DOUBLE,
    stress_hydrique DOUBLE,
    ph_optimal_score DOUBLE,
    indice_fertilite DOUBLE,
    annee INT,
    rendement_kg_ha DOUBLE
) PARTITIONED BY (region_part STRING) STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY');
-- ── GOLD : prédictions + recommandations ────────────────────────────────
CREATE TABLE IF NOT EXISTS predictions_gold (
    plot_id_secure STRING,
    region STRING,
    culture STRING,
    rendement_predit DOUBLE,
    prediction_ts TIMESTAMP
) STORED AS ORC;
-- ── Vue recommandations par région et type de sol ───────────────────────
CREATE OR REPLACE VIEW vue_recommandations AS
SELECT s.region,
    s.type_sol,
    s.culture,
    AVG(COALESCE(p.rendement_predit, s.rendement_kg_ha)) AS rendement_moyen_predit,
    COUNT(*) AS nb_parcelles,
    AVG(s.ph_sol) AS ph_moyen,
    AVG(s.pluviometrie_annuelle) AS pluvio_moyenne,
    AVG(s.stress_hydrique) AS stress_hydrique_moyen,
    CASE
        WHEN AVG(p.rendement_predit) >= MAX(AVG(p.rendement_predit)) OVER (PARTITION BY s.region) * 0.9 THEN 'CULTURE_OPTIMALE'
        WHEN AVG(p.rendement_predit) >= MAX(AVG(p.rendement_predit)) OVER (PARTITION BY s.region) * 0.7 THEN 'CULTURE_VIABLE'
        ELSE 'CULTURE_DECONSEILLE'
    END AS statut_recommandation
FROM parcelles_silver s
    LEFT JOIN predictions_gold p ON s.plot_id_secure = p.plot_id_secure
GROUP BY s.region,
    s.type_sol,
    s.culture;