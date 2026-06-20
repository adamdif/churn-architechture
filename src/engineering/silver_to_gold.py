"""
ETL — SILVER -> GOLD (zone churn_features).

Construit la table de features ML pour la prédiction du churn, d'après les
décisions de l'EDA (notebooks/exploration/01_eda_churn.ipynb) :
  - correction : senior_citizen re-binarisé (bruité par l'amplification ×100)
  - identifiant: base_id (= customer_id sans suffixe -rNN) pour le split anti-leakage
  - dérivées   : tenure_bucket, avg_monthly_spend, num_services
  - sélection  : on ne garde que les variables retenues
                 (total_charges, gender, phone_service, multiple_lines,
                  streaming_*, online_backup, device_protection écartées)

Gouvernance (ONBOARDING.md) : churn_features est alimentée par le Data Scientist.
Usage :
    python -m src.engineering.silver_to_gold
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pyspark.sql import DataFrame, functions as F  # noqa: E402
from src import config  # noqa: E402

# Services optionnels résumés par num_services (EDA : redondance V≈0.70–0.77)
SERVICE_COLS = [
    "online_security", "online_backup", "device_protection",
    "tech_support", "streaming_tv", "streaming_movies",
]

# Colonnes finales conservées dans gold/churn_features
FEATURE_COLUMNS = [
    "base_id", "customer_id",                       # identifiants (split + traçabilité)
    "tenure", "monthly_charges",                    # numériques retenues
    "senior_citizen", "partner", "dependents",      # binaires retenues
    "contract", "payment_method", "internet_service",
    "online_security", "tech_support", "paperless_billing",  # catégorielles retenues
    "tenure_bucket", "avg_monthly_spend", "num_services",    # dérivées
    "churn",                                        # cible
]


def build_features(df: DataFrame) -> DataFrame:
    """Construit la table de features curée d'après les décisions d'EDA."""
    out = df

    # 1. Correction : senior_citizen bruité par l'amplification -> re-binarisation
    if "senior_citizen" in out.columns:
        out = out.withColumn("senior_citizen", F.round("senior_citizen").cast("int"))

    # 2. Identifiant client d'origine (split anti-leakage à la modélisation)
    out = out.withColumn("base_id", F.regexp_replace("customer_id", r"-r\d+$", ""))

    # 3. Dérivée : tranche d'ancienneté
    if "tenure" in out.columns:
        out = out.withColumn(
            "tenure_bucket",
            F.when(F.col("tenure") <= 12, "0-1y")
             .when(F.col("tenure") <= 24, "1-2y")
             .when(F.col("tenure") <= 48, "2-4y")
             .otherwise("4y+"),
        )

    # 4. Dérivée : dépense moyenne mensuelle (remplace total_charges, colinéaire 0.83)
    if {"total_charges", "tenure"}.issubset(out.columns):
        out = out.withColumn(
            "avg_monthly_spend",
            F.when(F.col("tenure") > 0, F.col("total_charges") / F.col("tenure"))
             .otherwise(F.col("monthly_charges")),
        )

    # 5. Dérivée : nombre de services optionnels souscrits (résume 6 colonnes redondantes)
    present = [c for c in SERVICE_COLS if c in out.columns]
    if present:
        expr = sum(F.when(F.lower(F.col(c)) == "yes", 1).otherwise(0) for c in present)
        out = out.withColumn("num_services", expr)

    # 6. Sélection finale : seulement les colonnes retenues
    keep = [c for c in FEATURE_COLUMNS if c in out.columns]
    return out.select(*keep)


def build_kpis(df: DataFrame) -> DataFrame:
    """Indicateurs business agrégés pour le dashboard (zone Analyst)."""
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

    df = spark.read.parquet(config.s3_path("silver"))
    print(f"[gold] Silver lu : {df.count()} lignes")

    features = build_features(df)
    feat_out = config.s3_path("gold_feat")
    features.write.mode("overwrite").parquet(feat_out)
    print(f"[gold] Features -> {feat_out} | {len(features.columns)} colonnes")
    features.printSchema()

    kpis = build_kpis(features)
    kpi_out = config.s3_path("gold_kpi")
    kpis.write.mode("overwrite").parquet(kpi_out)
    print(f"[gold] KPIs -> {kpi_out}")
    kpis.show(truncate=False)

    spark.sql("CREATE DATABASE IF NOT EXISTS churn_db")
    features.write.mode("overwrite").saveAsTable("churn_db.gold_churn_features")
    kpis.write.mode("overwrite").saveAsTable("churn_db.gold_churn_kpis")
    print("[gold] Tables Hive : churn_db.gold_churn_features, churn_db.gold_churn_kpis")

    spark.stop()


if __name__ == "__main__":
    main()