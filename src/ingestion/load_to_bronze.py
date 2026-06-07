"""
ÉTAPE 2 — Chargement (+ amplification) vers la zone BRONZE (S3).

- Lit le CSV brut local (produit par download_data.py).
- L'amplifie pour atteindre une volumétrie Big Data justifiant Spark
  (Telco = 7 043 lignes -> x100 = ~700 k, x1000 = ~7 M).
- Écrit le résultat BRUT dans bronze/ (aucune transformation métier ici).

Bronze = copie fidèle de la source, jamais modifiée par la suite.

Usage :
    python -m src.ingestion.load_to_bronze
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import functions as F  # noqa: E402
from src import config  # noqa: E402
from src.ingestion.download_data import download  # noqa: E402


def amplify(df, factor: int):
    """Réplique le dataset `factor` fois avec un léger bruit numérique et
    des identifiants uniques, pour simuler un volume réaliste."""
    if factor <= 1:
        return df

    replicas = []
    numeric_cols = [c for c, t in df.dtypes if t in ("double", "int", "bigint", "float")]
    for i in range(factor):
        rep = df
        # Jitter +/- 2 % sur les colonnes numériques pour éviter les doublons exacts.
        for c in numeric_cols:
            rep = rep.withColumn(c, F.col(c) * (1 + (F.rand(seed=i) - 0.5) * 0.04))
        # ID unique par réplique si une colonne customerID existe.
        if "customerID" in df.columns:
            rep = rep.withColumn("customerID", F.concat_ws("-", F.col("customerID"), F.lit(f"r{i}")))
        replicas.append(rep)

    out = replicas[0]
    for rep in replicas[1:]:
        out = out.unionByName(rep)
    return out


def main():
    csv_path = download()

    spark = config.build_spark(app_name="churn-ingestion")

    # Le CSV est sur le disque LOCAL du driver. En mode distribué (driver +
    # executors sur des pods séparés), les executors ne voient pas ce fichier,
    # d'où une FileNotFoundException si on fait spark.read.csv("file://...").
    # La source étant petite (~172 Ko), on la lit avec pandas sur le driver
    # puis on la distribue via createDataFrame : c'est l'amplification qui,
    # ensuite, génère le vrai volume côté cluster.
    import pandas as pd
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
    pdf = pd.read_csv(csv_path)
    df = spark.createDataFrame(pdf)
    base_count = df.count()
    print(f"[bronze] Source : {base_count} lignes, {len(df.columns)} colonnes")

    df = amplify(df, config.AMPLIFY_FACTOR)
    print(f"[bronze] Après amplification x{config.AMPLIFY_FACTOR} : {df.count()} lignes")

    out = config.s3_path("bronze")
    df.write.mode("overwrite").option("header", "true").csv(out)
    print(f"[bronze] Écrit -> {out}")
    spark.stop()


if __name__ == "__main__":
    main()
