CREATE DATABASE IF NOT EXISTS agri_insight COMMENT 'Pipeline Agri-Insight SN';
USE agri_insight;

-- BRONZE
CREATE EXTERNAL TABLE IF NOT EXISTS parcelles_bronze (
  parcel_id STRING, producer_name STRING, region STRING, culture STRING,
  type_sol STRING, ph_sol DOUBLE, pluviometrie_annuelle DOUBLE,
  temperature_moy_celsius DOUBLE, humidite_relative_pct DOUBLE,
  teneur_matiere_organique_pct DOUBLE, surface_hectares DOUBLE,
  annee INT, rendement_kg_ha DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/agri_insight/bronze/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- SILVER (avec colonnes dérivées – sera rempli par Spark)
CREATE TABLE IF NOT EXISTS parcelles_silver (
  plot_id_secure STRING, region STRING, culture STRING, type_sol STRING,
  ph_sol DOUBLE, pluviometrie_annuelle DOUBLE, temperature_moy_celsius DOUBLE,
  humidite_relative_pct DOUBLE, teneur_matiere_organique_pct DOUBLE,
  surface_hectares DOUBLE, stress_hydrique DOUBLE, ph_optimal_score DOUBLE,
  indice_fertilite DOUBLE, annee INT, rendement_kg_ha DOUBLE
)
PARTITIONED BY (region_part STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- GOLD : prédictions
CREATE TABLE IF NOT EXISTS predictions_gold (
  plot_id_secure STRING, region STRING, culture STRING,
  rendement_predit DOUBLE, prediction_ts TIMESTAMP
)
STORED AS ORC;

-- Vue des recommandations
CREATE OR REPLACE VIEW vue_recommandations AS
SELECT s.region, s.type_sol, s.culture,
       AVG(s.rendement_kg_ha) AS rendement_moyen_predit,
       COUNT(*) AS nb_parcelles,
       AVG(s.ph_sol) AS ph_moyen,
       AVG(s.pluviometrie_annuelle) AS pluvio_moyenne,
       AVG(s.stress_hydrique) AS stress_hydrique_moyen,
       ROW_NUMBER() OVER (PARTITION BY s.region, s.type_sol ORDER BY AVG(s.rendement_kg_ha) DESC) AS rang,
       CASE WHEN ROW_NUMBER() OVER (PARTITION BY s.region, s.type_sol ORDER BY AVG(s.rendement_kg_ha) DESC) = 1
            THEN 'CULTURE_OPTIMALE' ELSE '' END AS statut_recommandation
FROM parcelles_silver s
GROUP BY s.region, s.type_sol, s.culture;
