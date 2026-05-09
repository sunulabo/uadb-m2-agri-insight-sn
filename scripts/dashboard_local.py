import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

def generate_dashboard_from_csv():
    csv_path = "scripts/recommandations.csv"
    if not os.path.exists(csv_path):
        print(f"Erreur : Le fichier {csv_path} n'existe pas. Lancez d'abord l'export Spark.")
        return

    try:
        df = pd.read_csv(csv_path)
        
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle("Agri-Insight SN — Dashboard Recommandations (Mode CSV)", fontsize=16, fontweight="bold")

        # Heatmap
        pivot = df.pivot_table(values="rendement_moyen_predit", index="culture", columns="region", aggfunc="mean")
        sns.heatmap(pivot, annot=True, fmt=".0f", cmap="YlGn", ax=axes[0, 0])
        axes[0, 0].set_title("Rendement prédit moyen (kg/ha)")

        # Barplot Culture Optimale
        opt = df[df["statut_recommandation"] == "CULTURE_OPTIMALE"]
        sns.barplot(data=opt, x="rendement_moyen_predit", y="region", hue="culture", ax=axes[0, 1])
        axes[0, 1].set_title("Culture optimale par région")

        # Scatter Pluvio
        sns.scatterplot(data=df, x="pluvio_moyenne", y="rendement_moyen_predit", hue="culture", ax=axes[1, 0])
        axes[1, 0].set_title("Pluviométrie vs Rendement")

        # Stress Hydrique
        stress = df.groupby("region")["stress_hydrique_moyen"].mean().sort_values()
        stress.plot(kind="barh", ax=axes[1, 1], color="green")
        axes[1, 1].set_title("Indice de stress hydrique moyen")

        plt.tight_layout()
        plt.savefig("dashboard_agri_insight_final.png")
        print("Dashboard généré avec succès : dashboard_agri_insight_final.png")

    except Exception as e:
        print(f"Erreur : {e}")

if __name__ == "__main__":
    generate_dashboard_from_csv()
