import time
import os
import polars as pl
import duckdb
import dask.dataframe as dd
from pyspark.sql import SparkSession
from pyspark.sql.functions import max as F_max, min as F_min, avg as F_avg, count as F_count
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# Configuration Spark
spark = SparkSession.builder.appName("Benchmark Analytics").getOrCreate()

# Fonction pour générer des données synthétiques
def generate_synthetic_data(scale_factor, output_dir="data"):
    os.makedirs(output_dir, exist_ok=True)
    num_rows = int(1_000_000 * scale_factor)  # 1M rows pour SF1, ajusté par scale_factor
    stations = [f"Station_{i}" for i in range(100)]
    data = {
        "station": np.random.choice(stations, num_rows),
        "temperature": np.random.uniform(-20, 40, num_rows)
    }
    
    # Créer un DataFrame Polars et sauvegarder en Parquet
    df = pl.DataFrame(data)
    file_path = f"{output_dir}/data_sf{int(scale_factor*100)}.parquet"
    df.write_parquet(file_path)
    print(f"Generated {file_path} with {num_rows} rows")
    return file_path

# Générer les données pour SF10, SF50, SF100
scales = {"sf10": 0.1, "sf50": 0.5, "sf100": 1.0}
for scale_name, scale_factor in scales.items():
    generate_synthetic_data(scale_factor)

# Configurations pour le benchmark
results = {}
tech_groups = {
    "In-Memory & Vectorized": ["polars", "duckdb"],
    "Distributed & Scalable": ["dask", "spark"]
}

# Benchmark
for scale in scales.keys():
    file_path = f"data/data_{scale}.parquet"
    if not os.path.exists(file_path):
        print(f"Erreur : Le fichier {file_path} n'existe pas.")
        continue
    
    station_col = "station"
    temp_col = "temperature"
    
    # Polars (In-Memory & Vectorized)
    df_pl = pl.read_parquet(file_path)
    start = time.perf_counter()
    result_pl = df_pl.group_by(station_col).agg([
        pl.max(temp_col).alias("max_temp"), 
        pl.min(temp_col).alias("min_temp"), 
        pl.mean(temp_col).alias("mean_temp"), 
        pl.len().alias("count")
    ])
    results[f"polars_{scale}"] = time.perf_counter() - start
    print(f"Polars {scale} time: {results[f'polars_{scale}']:.4f} s")

    # DuckDB (In-Memory & Vectorized)
    con = duckdb.connect()
    start = time.perf_counter()
    con.execute(f"SELECT {station_col}, MAX({temp_col}), MIN({temp_col}), AVG({temp_col}), COUNT(*) FROM '{file_path}' GROUP BY {station_col}").fetchall()
    results[f"duckdb_{scale}"] = time.perf_counter() - start
    print(f"DuckDB {scale} time: {results[f'duckdb_{scale}']:.4f} s")
    con.close()

    # Dask (Distributed & Scalable)
    df_dask = dd.read_parquet(file_path)
    start = time.perf_counter()
    df_dask.groupby(station_col).agg({temp_col: ["max", "min", "mean"], station_col: "count"}).compute()
    results[f"dask_{scale}"] = time.perf_counter() - start
    print(f"Dask {scale} time: {results[f'dask_{scale}']:.4f} s")

    # Spark (Distributed & Scalable)
    df_spark = spark.read.parquet(file_path)
    start = time.perf_counter()
    df_spark.groupBy(station_col).agg(
        F_max(temp_col).alias("max_temp"),
        F_min(temp_col).alias("min_temp"),
        F_avg(temp_col).alias("mean_temp"),
        F_count(station_col).alias("count")
    ).collect()
    results[f"spark_{scale}"] = time.perf_counter() - start
    print(f"Spark {scale} time: {results[f'spark_{scale}']:.4f} s")

# Sauvegarde des résultats dans un fichier CSV
if results:
    csv_data = []
    for key, value in results.items():
        tech, scale = key.split("_")
        csv_data.append({"Technology": tech, "Scale": scale, "Time (s)": value})
    
    results_df = pd.DataFrame(csv_data)
    results_df.to_csv("benchmark_results.csv", index=False)
    print("Results saved in benchmark_results.csv")

# Visualisation par groupe de technologies
for group_name, techs in tech_groups.items():
    plt.figure(figsize=(8, 5))
    for scale in scales.keys():
        times = [results.get(f"{tech}_{scale}", 0) for tech in techs]
        plt.plot(techs, times, marker='o', label=scale)
    
    plt.title(f"Query Execution Time - {group_name}")
    plt.xlabel("Technology")
    plt.ylabel("Time (s)")
    plt.legend(title="Scale Factor")
    plt.tight_layout()
    plt.savefig(f"benchmark_{group_name.lower().replace(' ', '_')}.png")
    plt.close()
    print(f"Graph saved as benchmark_{group_name.lower().replace(' ', '_')}.png")

print("Benchmark completed.")