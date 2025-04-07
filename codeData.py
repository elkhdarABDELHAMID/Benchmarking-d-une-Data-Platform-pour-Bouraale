import pandas as pd
import pyarrow.parquet as pq

# Charger les données (exemple)
df = pd.read_csv("data/weather_stations.csv")

# Créer les sous-ensembles
data = {
    "sf10": df.sample(frac=0.1, random_state=42),
    "sf50": df.sample(frac=0.5, random_state=42),
    "sf100": df.copy()
}

# Assurez-vous que le dossier 'data' existe
import os
os.makedirs("data", exist_ok=True)

# Sauvegarder dans le dossier "data/" au format Parquet
for scale, subset in data.items():
    file_path = f"data/data_{scale}.parquet"
    subset.to_parquet(file_path, index=False)
    print(f"✅ Fichier {file_path} sauvegardé")

print("✅ Tous les fichiers Parquet ont été générés")