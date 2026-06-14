"""
ETL — SILVER -> GOLD.

Produit les deux sous-zones métier de Gold :
    - gold/churn_features : variables enrichies prêtes pour le ML
    - gold/churn_kpis     : indicateurs agrégés pour le dashboard

Note de gouvernance (cf. ONBOARDING.md) : en production ce sont les Data
Scientists qui alimentent churn_features et les Data Analysts churn_kpis.
Ce script fournit une base de référence que chaque rôle peut étendre.

OPTIMISATIONS (vs version initiale) :
    - cache() sur les features (relues pour l'écriture ET le calcul des KPIs)
      + un seul count().
    - Écriture UNIQUE par sortie : table Hive EXTERNE (option "path") au lieu
      d'un write Parquet PUIS un saveAsTable managé (double stockage).

Usage :
    python -m src.engineering.silver_to_gold
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import DataFrame, functions as F  # noqa: E402
from src import config  # noqa: E402


def build_features(df: DataFrame) -> DataFrame:
    """Variables dérivées pour le modèle de churn."""
    out = df
    if "tenure" in df.columns:
        out = out.withColumn("tenure_years", F.round(F.col("tenure") / 12, 2))
        out = out.withColumn(
            "tenure_bucket",
            F.when(F.col("tenure") <= 12, "0-1y")
             .when(F.col("tenure") <= 24, "1-2y")
             .when(F.col("tenure") <= 48, "2-4y")
             .otherwise("4y+"),
        )
    if {"total_charges", "tenure"}.issubset(df.columns):
        out = out.withColumn(
            "avg_monthly_spend",
            F.when(F.col("tenure") > 0, F.col("total_charges") / F.col("tenure"))
             .otherwise(F.col("total_charges")),
        )
    service_cols = [c for c in df.columns if c in (
        "online_security", "online_backup", "device_protection",
        "tech_support", "streaming_tv", "streaming_movies", "phone_service",
    )]
    if service_cols:
        expr = sum(F.when(F.lower(F.col(c)) == "yes", 1).otherwise(0) for c in service_cols)
        out = out.withColumn("num_services", expr)
    return out


def build_kpis(df: DataFrame) -> DataFrame:
    """Indicateurs business agrégés pour le dashboard."""
    if "churn" not in df.columns:
        return df.limit(0)
    agg = [F.count("*").alias("nb_clients"),
           F.round(F.avg("churn") * 100, 2).alias("taux_churn_pct")]
    if "monthly_charges" in df.columns:
        agg.append(F.round(F.avg("monthly_charges"), 2).alias("revenu_moyen_mensuel"))
    group = "contract" if "contract" in df.columns else "tenure_bucket"
    return df.groupBy(group).agg(*agg).orderBy(F.desc("taux_churn_pct"))


def main():
    spark = config.build_spark(app_name="silver-to-gold")

    silver = spark.read.parquet(config.s3_path("silver"))

    # cache : features relues pour l'écriture ET pour calculer les KPIs.
    features = build_features(silver).cache()
    n = features.count()  # matérialise le cache une seule fois

    spark.sql("CREATE DATABASE IF NOT EXISTS churn_db")

    # --- Features : écriture unique en table externe ---
    feat_out = config.s3_path("gold_feat")
    (features.write.mode("overwrite").format("parquet").option("path", feat_out)
        .saveAsTable("churn_db.gold_churn_features"))
    print(f"[gold] Features -> {feat_out} | {n} lignes, {len(features.columns)} colonnes")

    # --- KPIs : agrégat (petit) écrit en table externe ---
    kpis = build_kpis(features)
    kpi_out = config.s3_path("gold_kpi")
    (kpis.write.mode("overwrite").format("parquet").option("path", kpi_out)
        .saveAsTable("churn_db.gold_churn_kpis"))
    print(f"[gold] KPIs -> {kpi_out}")
    kpis.show(truncate=False)

    print("[gold] Tables Hive : churn_db.gold_churn_features, churn_db.gold_churn_kpis")
    features.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()