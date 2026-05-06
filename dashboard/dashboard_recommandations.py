# dashboard_recommandations.py — Tableau de bord Agri-Insight SN
# Livrable 4 : Dashboard recommandations culture optimale par région
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
from pyhive import hive
import os


def generate_dashboard():
    try:
        conn = hive.Connection(
            host="localhost", port=10000, database="agri_insight"
        )
        # ── 1. Charger les recommandations ───────────────────────────────────────
        df = pd.read_sql(
            """
  SELECT region, type_sol, culture,
  rendement_moyen_predit, nb_parcelles,
  ph_moyen, pluvio_moyenne,
  stress_hydrique_moyen, statut_recommandation
  FROM agri_insight.vue_recommandations
  """,
            conn,
        )

        if df.empty:
            print("Aucune donnée trouvée dans la vue vue_recommandations.")
            return

        # ── 2. Dashboard 4 panneaux ───────────────────────────────────────────────
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle(
            "Agri-Insight SN — Dashboard Recommandations Agricoles",
            fontsize=16,
            fontweight="bold",
            color="#1B4D1E",
        )

        # Panneau 1 : Rendement prédit par région (heatmap culture × région)
        pivot = df.pivot_table(
            values="rendement_moyen_predit",
            index="culture",
            columns="region",
            aggfunc="mean",
        )
        sns.heatmap(
            pivot, annot=True, fmt=".0f", cmap="YlGn", ax=axes[0, 0], linewidths=0.5
        )
        axes[0, 0].set_title("Rendement prédit moyen (kg/ha) par culture et région")
        axes[0, 0].tick_params(axis="x", rotation=30)

        # Panneau 2 : Culture optimale par région (barplot)
        opt = (
            df[df["statut_recommandation"] == "CULTURE_OPTIMALE"]
            .groupby(["region", "culture"])["rendement_moyen_predit"]
            .mean()
            .reset_index()
        )

        if not opt.empty:
            colors = ["#16A34A", "#D97706", "#2563EB", "#9333EA", "#DC2626"]
            for i, (reg, grp) in enumerate(opt.groupby("region")):
                axes[0, 1].barh(
                    reg,
                    grp["rendement_moyen_predit"].max(),
                    color=colors[i % len(colors)],
                    label=grp["culture"].iloc[0],
                )
            axes[0, 1].set_title("Rendement culture optimale par région")
            axes[0, 1].set_xlabel("Rendement prédit (kg/ha)")
            axes[0, 1].legend(title="Culture optimale", fontsize=8)
        else:
            axes[0, 1].text(0.5, 0.5, "Pas de culture optimale identifiée", ha="center")

        # Panneau 3 : Corrélation pluviométrie / rendement par culture
        cultures = df["culture"].unique()
        colors = sns.color_palette("husl", len(cultures))
        for i, culture in enumerate(cultures):
            sub = df[df["culture"] == culture]
            axes[1, 0].scatter(
                sub["pluvio_moyenne"],
                sub["rendement_moyen_predit"],
                label=culture,
                alpha=0.7,
                s=60,
                color=colors[i],
            )
        axes[1, 0].set_xlabel("Pluviométrie moyenne (mm/an)")
        axes[1, 0].set_ylabel("Rendement prédit (kg/ha)")
        axes[1, 0].set_title("Pluviométrie vs Rendement par culture")
        axes[1, 0].legend(fontsize=8)

        # Panneau 4 : Impact du stress hydrique par région
        stress_reg = df.groupby("region")["stress_hydrique_moyen"].mean().sort_values()
        bars = axes[1, 1].barh(
            stress_reg.index,
            stress_reg.values,
            color=["#DC2626" if v < 0.7 else "#16A34A" for v in stress_reg.values],
        )
        axes[1, 1].axvline(
            x=1.0,
            color="black",
            linestyle="--",
            linewidth=1,
            label="Besoin optimal (ratio=1)",
        )
        axes[1, 1].set_title("Indice de stress hydrique moyen par région")
        axes[1, 1].set_xlabel("Ratio pluvio réelle / besoin culture")
        axes[1, 1].legend(fontsize=8)

        plt.tight_layout()
        output_path = "dashboard/dashboard_agri_insight.png"
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Dashboard sauvegardé : {output_path} ✓")

        # ── 3. Recommandations textuelles ────────────────────────────────────────
        print("\n=== RECOMMANDATIONS AGRICOLES ===")
        for _, row in df[df["statut_recommandation"] == "CULTURE_OPTIMALE"].iterrows():
            print(
                f"Dans la région {row['region']} (sol {row['type_sol']}, pH~{row['ph_moyen']:.1f}, {row['pluvio_moyenne']:.0f}mm/an) :"
            )
            print(
                f" → Privilégier le {row['culture']} : rendement prédit {row['rendement_moyen_predit']:.0f} kg/ha"
            )

    except Exception as e:
        print(f"Erreur lors de la génération du dashboard : {e}")


if __name__ == "__main__":
    generate_dashboard()
