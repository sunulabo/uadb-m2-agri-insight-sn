import pandera as pa
from pandera.typing import Series
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


class AgriDataSchema(pa.SchemaModel):
    """Contrat de données : parcelles agricoles sénégalaises."""

    parcel_id: Series[str] = pa.Field(unique=True)
    producer_name: Series[str] = pa.Field()  # Sera anonymisé
    region: Series[str] = pa.Field(isin=REGIONS)
    culture: Series[str] = pa.Field(isin=CULTURES)
    type_sol: Series[str] = pa.Field(isin=TYPES_SOL)
    ph_sol: Series[float] = pa.Field(ge=4.5, le=8.5)
    pluviometrie_annuelle: Series[float] = pa.Field(ge=200, le=1500)
    temperature_moy_celsius: Series[float] = pa.Field(ge=15.0, le=45.0)
    humidite_relative_pct: Series[float] = pa.Field(ge=10.0, le=100.0)
    teneur_matiere_organique_pct: Series[float] = pa.Field(ge=0.1, le=5.0)
    surface_hectares: Series[float] = pa.Field(gt=0.0, le=5000.0)
    annee: Series[int] = pa.Field(ge=2018, le=2030)
    rendement_kg_ha: Series[float] = pa.Field(ge=0.0)

    # Cohérence agronomique : Riz exige au moins 600mm/an
    @pa.check("pluviometrie_annuelle", name="riz_needs_water")
    def check_riz_pluvio(cls, series, culture):
        riz_mask = culture == "Riz"
        return ~riz_mask | (series >= 600)

    class Config:
        coerce = True
        strict = True


def validate_and_filter(df: pd.DataFrame) -> pd.DataFrame:
    """Valide et filtre les lignes invalides en loguant les erreurs."""
    try:
        return AgriDataSchema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as exc:
        err = exc.failure_cases
        logger.warning(f"[AgriDataSchema] {len(err)} erreur(s) rejetées")
        valid_idx = df.index.difference(err["index"].dropna().astype(int))
        return df.loc[valid_idx]
