"""
ETL — BRONZE -> SILVER.

Nettoyage et normalisation. Aucune logique métier/feature ici (-> Gold).
Règles appliquées :
    - correction des types (TotalCharges textuel -> double)
    - traitement des valeurs manquantes / vides
    - suppression des doublons
    - standardisation des noms de colonnes (snake_case)
    - cible Churn (Yes/No) -> booléen 0/1
Sortie : Parquet (colonnaire, compressé) dans silver/.

Usage :
    python -m src.engineering.bronze_to_silver
"""
import sys
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
    import re
    s = name.strip().replace(" ", "_")
    s = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", s)   # MonthlyCharges -> Monthly_Charges
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)  # customerID -> customer_ID
    return re.sub(r"_+", "_", s).lower()


def clean(df: DataFrame) -> DataFrame:
    # 1. Standardisation des noms de colonnes
    for c in df.columns:
        df = df.withColumnRenamed(c, to_snake(c))

    # 2. TotalCharges : vient en string avec des espaces vides -> double
    if "total_charges" in df.columns:
        df = df.withColumn(
            "total_charges",
            F.when(F.trim(F.col("total_charges")) == "", None)
             .otherwise(F.col("total_charges")).cast("double"),
        )

    # 3. monthly_charges / tenure en numérique sûr
    for c in ("monthly_charges", "tenure"):
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("double"))

    # 4. Cible : Yes/No -> 1/0
    if "churn" in df.columns:
        df = df.withColumn(
            "churn", F.when(F.lower(F.col("churn")) == "yes", 1).otherwise(0)
        )

    # 5. Valeurs manquantes : on impute total_charges manquant par tenure*monthly
    if {"total_charges", "tenure", "monthly_charges"}.issubset(df.columns):
        df = df.withColumn(
            "total_charges",
            F.when(F.col("total_charges").isNull(),
                   F.col("tenure") * F.col("monthly_charges"))
             .otherwise(F.col("total_charges")),
        )

    # 6. Doublons + lignes entièrement nulles
    df = df.dropDuplicates().na.drop("all")
    return df


def main():
    spark = config.build_spark(app_name="bronze-to-silver")

    src = config.s3_path("bronze")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(src)
    print(f"[silver] Bronze lu : {df.count()} lignes")

    df = clean(df)
    out = config.s3_path("silver")
    df.write.mode("overwrite").parquet(out)
    print(f"[silver] Écrit (Parquet) -> {out} | {df.count()} lignes, {len(df.columns)} colonnes")

    # Enregistrement comme table Hive pour lecture SQL par les Data Scientists.
    spark.sql("CREATE DATABASE IF NOT EXISTS churn_db")
    df.write.mode("overwrite").saveAsTable("churn_db.silver_churn_clean")
    print("[silver] Table Hive : churn_db.silver_churn_clean")
    spark.stop()


if __name__ == "__main__":
    main()
