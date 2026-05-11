import pandas as pd, numpy as np, random, uuid
from faker import Faker
from schema import validate_and_filter
import os

fake = Faker("fr_FR")
random.seed(42)
np.random.seed(42)

REGIONS_PARAMS = {
    "Louga": {"pluvio": (350, 80), "temp": (28, 2), "cultures": ["Mil", "Arachide"]},
    "Saint-Louis": {"pluvio": (300, 60), "temp": (27, 2), "cultures": ["Riz", "Mil"]},
    "Kaolack": {
        "pluvio": (700, 100),
        "temp": (29, 2),
        "cultures": ["Arachide", "Mil", "Niebe"],
    },
    "Ziguinchor": {
        "pluvio": (1200, 150),
        "temp": (27, 1),
        "cultures": ["Riz", "Manioc", "Arachide"],
    },
    "Thies": {"pluvio": (500, 80), "temp": (26, 2), "cultures": ["Arachide", "Mil"]},
    "Dakar": {"pluvio": (430, 70), "temp": (25, 2), "cultures": ["Arachide", "Mil"]},
    "Tambacounda": {
        "pluvio": (900, 120),
        "temp": (30, 2),
        "cultures": ["Mil", "Manioc", "Niebe"],
    },
}

RENDEMENT_BASE = {
    "Mil": 800,
    "Riz": 2500,
    "Arachide": 1000,
    "Manioc": 8000,
    "Niebe": 700,
}


def compute_rendement(culture, pluvio, temp, ph, mo) -> float:
    """Rendement simulé selon les facteurs agronomiques."""
    base = RENDEMENT_BASE[culture]
    # Sensibilité pluviométrie
    k_pluvio = {"Mil": 0.7, "Riz": 1.0, "Arachide": 0.6, "Manioc": 0.4, "Niebe": 0.5}[
        culture
    ]
    # Sensibilité température (optimum ~27°C)
    k_temp = {
        "Mil": -0.1,
        "Riz": -0.15,
        "Arachide": -0.05,
        "Manioc": -0.05,
        "Niebe": -0.08,
    }[culture]
    r = base
    r *= 1 + k_pluvio * (pluvio - 800) / 800
    r *= 1 + k_temp * (temp - 27) / 5
    r *= 1 + 0.2 * (mo - 1.5) / 1.5
    r *= random.uniform(0.92, 1.08)  # Variabilité naturelle
    return max(50.0, round(r, 1))


def generate_parcelles(n_par_region: int = 100) -> pd.DataFrame:
    """Génère les données de parcelles pour toutes les régions."""
    rows = []
    for region, params in REGIONS_PARAMS.items():
        for year in range(2018, 2024):
            for _ in range(n_par_region):
                culture = random.choice(params["cultures"])
                pluvio = max(200, round(np.random.normal(*params["pluvio"]), 1))
                temp = round(np.random.normal(*params["temp"]), 1)
                ph = round(random.uniform(5.0, 8.0), 1)
                mo = round(random.uniform(0.5, 3.0), 2)
                rows.append(
                    {
                        "parcel_id": str(uuid.uuid4()),
                        "producer_name": fake.name(),
                        "region": region,
                        "culture": culture,
                        "type_sol": random.choice(
                            [
                                "Sablonneux",
                                "Argileux",
                                "Limoneux",
                                "Ferralitique",
                                "Dior",
                            ]
                        ),
                        "ph_sol": ph,
                        "pluviometrie_annuelle": pluvio,
                        "temperature_moy_celsius": temp,
                        "humidite_relative_pct": round(random.uniform(30, 90), 1),
                        "teneur_matiere_organique_pct": mo,
                        "surface_hectares": round(random.uniform(0.5, 50.0), 2),
                        "annee": year,
                        "rendement_kg_ha": compute_rendement(
                            culture, pluvio, temp, ph, mo
                        ),
                    }
                )
    df = pd.DataFrame(rows)
    df = validate_and_filter(df)  # Validation Pandera
    output_dir = "data/bronze"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "agri_parcelles_brut.csv")
    df.to_csv(output_path, index=False)
    print(f"Généré : {len(df)} parcelles OK")
    return df


if __name__ == "__main__":
    generate_parcelles(n_par_region=100)
