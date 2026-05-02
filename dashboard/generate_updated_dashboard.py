import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Charger les prédictions enrichies (ou le CSV des meilleures cultures)
df_pred = pd.read_csv('data/silver/predictions_enriched.csv')

# 1. Heatmap rendement moyen par culture × région
agg = df_pred.groupby(['region', 'culture'])['prediction'].mean().reset_index()
pivot = agg.pivot(index='culture', columns='region', values='prediction')
plt.figure(figsize=(12,6))
sns.heatmap(pivot, annot=True, fmt='.0f', cmap='YlGn')
plt.title('Rendement prédit moyen (kg/ha) par culture et région')
plt.tight_layout()
plt.savefig('dashboard/heatmap_rendement.png')
print("✅ Heatmap mise à jour : dashboard/heatmap_rendement.png")

# 2. Culture optimale par région (tous sols confondus) : culture avec rendement max
best_global = agg.loc[agg.groupby('region')['prediction'].idxmax()]
plt.figure(figsize=(10,6))
plt.barh(best_global['region'], best_global['prediction'], color='green')
plt.xlabel('Rendement prédit (kg/ha)')
plt.title('Culture optimale par région (meilleur rendement toutes cultures)')
for i, (_, row) in enumerate(best_global.iterrows()):
    plt.text(row['prediction']+10, i, row['culture'], va='center')
plt.tight_layout()
plt.savefig('dashboard/culture_optimale.png')
print("✅ Graphique culture optimale mis à jour : dashboard/culture_optimale.png")

# Bonus : un graphique par type de sol (barres groupées)
plt.figure(figsize=(14,8))
best_by_sol = df_pred.groupby(['region', 'type_sol', 'culture'])['prediction'].mean().reset_index()
best_by_sol = best_by_sol.loc[best_by_sol.groupby(['region','type_sol'])['prediction'].idxmax()]
for sol in best_by_sol['type_sol'].unique():
    subset = best_by_sol[best_by_sol['type_sol'] == sol]
    plt.barh(subset['region'], subset['prediction'], label=sol, alpha=0.7)
plt.xlabel('Rendement prédit (kg/ha)')
plt.title('Culture optimale par région et type de sol')
plt.legend(title='Type de sol')
plt.tight_layout()
plt.savefig('dashboard/culture_optimale_par_sol.png')
print("✅ Graphique détaillé sauvegardé : dashboard/culture_optimale_par_sol.png")
