"""
ETL — BRONZE -> SILVER.

Nettoyage et normalisation. Aucune logique métier/feature ici (-> Gold).
Règles appliquées :
    - standardisation des noms de colonnes (snake_case)
    - correction des types (total_charges textuel/vide -> double)
    - cible churn (Yes/No) -> 0/1
    - imputation des total_charges manquants (tenure * monthly_charges)
    - suppression des doublons et lignes entièrement nulles
Sortie : Parquet (colonnaire, compressé) dans silver/.

OPTIMISATIONS (vs version initiale) :
    - cache() + un seul count() : le DataFrame nettoyé est relu pour
      l'écriture et le décompte ; sans cache, Spark relirait le CSV à chaque
      action.
    - Écriture UNIQUE : table Hive EXTERNE (option "path") pointant sur le
      Parquet S3, au lieu d'un write Parquet PUIS un saveAsTable managé
      (qui stockait la donnée DEUX fois).

Usage :
    python -m src.engineering.bronze_to_silver
"""
import sys
import re
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import DataFrame, functions as F  # noqa: E402
from src import config  # noqa: E402


def to_snake(name: str) -> str:
    """CamelCase / espaces / acronymes -> snake_case.

    >>> to_snake("customerID")
    'customer_id'
    >>> to_snake("MonthlyCharges")
    'monthly_charges'
    """
    s = name.strip().replace(" ", "_")
    s = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", s)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return re.sub(r"_+", "_", s).lower()


def clean(df: DataFrame) -> DataFrame:
    # 1. Standardisation des noms de colonnes.
    for c in df.columns:
        df = df.withColumnRenamed(c, to_snake(c))

    # 2. total_charges : string avec des '' -> double (vide -> NULL).
    if "total_charges" in df.columns:
        df = df.withColumn(
            "total_charges",
            F.when(F.trim(F.col("total_charges").cast("string")) == "", None)
             .otherwise(F.col("total_charges")).cast("double"),
        )

    # 3. monthly_charges / tenure en numérique sûr.
    for c in ("monthly_charges", "tenure"):
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("double"))

    # 4. Cible : Yes/No -> 1/0.
    if "churn" in df.columns:
        df = df.withColumn(
            "churn", F.when(F.lower(F.col("churn").cast("string")) == "yes", 1).otherwise(0)
        )

    # 5. Imputation : total_charges manquant ~ tenure * monthly_charges.
    if {"total_charges", "tenure", "monthly_charges"}.issubset(df.columns):
        df = df.withColumn(
            "total_charges",
            F.when(F.col("total_charges").isNull(),
                   F.col("tenure") * F.col("monthly_charges"))
             .otherwise(F.col("total_charges")),
        )

    # 6. Doublons + lignes entièrement nulles.
    return df.dropDuplicates().na.drop("all")


def main():
    spark = config.build_spark(app_name="bronze-to-silver")

    raw = (
        spark.read
        .option("header", "true").option("inferSchema", "true")
        .csv(config.s3_path("bronze"))
    )

    # cache : df relu pour le count ET l'écriture -> on évite de relire le CSV.
    df = clean(raw).cache()
    n = df.count()  # une seule action matérialise le cache

    out = config.s3_path("silver")
    spark.sql("CREATE DATABASE IF NOT EXISTS churn_db")

    # Écriture UNIQUE : table Hive EXTERNE pointant sur le Parquet S3.
    # option("path", out) -> pas de double stockage (managé + parquet séparé).
    (df.write.mode("overwrite").format("parquet").option("path", out)
       .saveAsTable("churn_db.silver_churn_clean"))

    print(f"[silver] Écrit (Parquet + table Hive externe) -> {out} "
          f"| {n} lignes, {len(df.columns)} colonnes")
    print("[silver] Table Hive : churn_db.silver_churn_clean")

    df.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()