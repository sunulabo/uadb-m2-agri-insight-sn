import pandera as pa
import pandas as pd, logging

logger = logging.getLogger("AgriSchema")

# Régions couvertes (7 régions agricoles clés du Sénégal)
REGIONS = [
    "Dakar",
    "Thies",
    "Kaolack",
    "Ziguinchor",
    "Saint-Louis",
    "Louga",
    "Tambacounda",
]
CULTURES = ["Mil", "Riz", "Arachide", "Manioc", "Niebe"]
TYPES_SOL = ["Sablonneux", "Argileux", "Limoneux", "Ferralitique", "Dior"]

# Définition du schéma via l'API DataFrameSchema (plus universelle)
AgriSchema = pa.DataFrameSchema({
    "parcel_id": pa.Column(str, unique=True),
    "producer_name": pa.Column(str),
    "region": pa.Column(str, pa.Check.isin(REGIONS)),
    "culture": pa.Column(str, pa.Check.isin(CULTURES)),
    "type_sol": pa.Column(str, pa.Check.isin(TYPES_SOL)),
    "ph_sol": pa.Column(float, pa.Check.in_range(4.5, 8.5)),
    "pluviometrie_annuelle": pa.Column(float, pa.Check.in_range(200, 1500)),
    "temperature_moy_celsius": pa.Column(float, pa.Check.in_range(15.0, 45.0)),
    "humidite_relative_pct": pa.Column(float, pa.Check.in_range(10.0, 100.0)),
    "teneur_matiere_organique_pct": pa.Column(float, pa.Check.in_range(0.1, 5.0)),
    "surface_hectares": pa.Column(float, pa.Check.greater_than(0.0)),
    "annee": pa.Column(int, pa.Check.in_range(2018, 2030)),
    "rendement_kg_ha": pa.Column(float, pa.Check.greater_than_or_equal_to(0.0))
}, 
coerce=True, 
strict=True)

def validate_and_filter(df: pd.DataFrame) -> pd.DataFrame:
    """Valide et filtre les lignes invalides."""
    try:
        return AgriSchema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as exc:
        err = exc.failure_cases
        logger.warning(f"[AgriSchema] {len(err)} erreurs détectées, filtrage en cours...")
        # On ne garde que les lignes qui n'ont pas d'erreurs
        invalid_indices = err["index"].dropna().unique()
        return df.drop(index=invalid_indices, errors='ignore')
