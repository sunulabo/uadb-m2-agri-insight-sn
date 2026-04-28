import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyhive import hive

conn = hive.Connection(host='hive-metastore', port=10000, database='agri_insight')
df = pd.read_sql("""
    SELECT region, type_sol, culture, rendement_moyen_predit,
           ph_moyen, pluvio_moyenne, stress_hydrique_moyen, statut_recommandation
    FROM vue_recommandations
""", conn)

fig, axes = plt.subplots(2,2, figsize=(16,12))
fig.suptitle('Agri-Insight SN - Dashboard Recommandations', fontsize=16)

# Heatmap rendement par culture/région
pivot = df.pivot_table(values='rendement_moyen_predit', index='culture', columns='region', aggfunc='mean')
sns.heatmap(pivot, annot=True, fmt='.0f', cmap='YlGn', ax=axes[0,0])
axes[0,0].set_title('Rendement prédit moyen (kg/ha)')

# Culture optimale par région (exemple simplifié)
opt = df[df['statut_recommandation']=='CULTURE_OPTIMALE'].groupby('region')['culture'].first().reset_index()
axes[0,1].bar(opt['region'], [1]*len(opt), color='green')
axes[0,1].set_title('Culture optimale par région')
for i, (reg, cult) in enumerate(zip(opt['region'], opt['culture'])):
    axes[0,1].text(i, 0.5, cult, ha='center', va='center')

# Corrélation pluviométrie / rendement
for culture in df['culture'].unique():
    sub = df[df['culture']==culture]
    axes[1,0].scatter(sub['pluvio_moyenne'], sub['rendement_moyen_predit'], label=culture, alpha=0.7)
axes[1,0].set_xlabel('Pluviométrie moyenne (mm/an)')
axes[1,0].set_ylabel('Rendement prédit (kg/ha)')
axes[1,0].set_title('Pluviométrie vs Rendement')
axes[1,0].legend()

# Stress hydrique par région
stress_reg = df.groupby('region')['stress_hydrique_moyen'].mean().sort_values()
bars = axes[1,1].barh(stress_reg.index, stress_reg.values, color=['red' if v<0.7 else 'green' for v in stress_reg.values])
axes[1,1].set_title('Stress hydrique moyen par région')
axes[1,1].set_xlabel('Stress hydrique')

plt.tight_layout()
plt.savefig('../dashboard/dashboard_agri_insight.png', dpi=150)
print("✅ Dashboard sauvegardé : dashboard_agri_insight.png")
