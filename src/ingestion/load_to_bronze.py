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

from pyspark.sql import DataFrame, functions as F  # noqa: E402
from src import config  # noqa: E402


# ---------------------------------------------------------------------------
# Colonne(s) sur lesquelles un léger jitter est appliqué.
#
# On ne bruite QUE MonthlyCharges, seule variable réellement CONTINUE (montant).
# Tout le reste est répliqué à l'identique, et ce volontairement :
#   - tenure       : entier borné (0..72 mois). Le bruiter le rendrait
#                    fractionnaire ET dépasserait le max réel (72*1.02=73.4).
#                    -> anomalie détectée par le rapport qualité, donc exclu.
#   - SeniorCitizen: binaire 0/1 -> un jitter en ferait un float aberrant.
#   - TotalCharges : contient des '' dans le Telco brut + on ne nettoie pas
#                    en Bronze (principe Medallion). Cast + impute en Silver.
#   - catégorielles: contract, paymentMethod... -> aucun sens à bruiter.
# L'unicité des lignes est déjà garantie par le suffixe sur customerID.
# ---------------------------------------------------------------------------
CONTINUOUS_COLS = ("MonthlyCharges",)


def amplify(df: DataFrame, factor: int) -> DataFrame:
    """Réplique le dataset `factor` fois, de façon DISTRIBUÉE.

    On duplique chaque ligne en UNE seule transformation Spark via
    array_repeat + posexplode : Spark parallélise, le plan reste plat
    (pas de pile de N unions qui sature le driver à factor élevé).

    - Jitter +/- 2 % UNIQUEMENT sur MonthlyCharges (continu), arrondi à 2
      décimales (précision monétaire).
    - customerID reçoit un suffixe de réplique (rN) -> lignes uniques.
    """
    if factor <= 1:
        return df

    # 1. Duplication distribuée : 1 ligne source -> `factor` lignes.
    out = (
        df
        .withColumn("_rep_arr", F.array_repeat(F.lit(1), factor))
        .select("*", F.posexplode("_rep_arr").alias("_rep", "_one"))
        .drop("_rep_arr", "_one")
    )

    # 2. Jitter +/- 2 % sur les colonnes continues présentes.
    #    try_cast (via expr) -> NULL au lieu de planter si valeur non numérique.
    for c in [c for c in CONTINUOUS_COLS if c in df.columns]:
        safe_num = F.expr(f"try_cast(`{c}` as double)")
        out = out.withColumn(
            c,
            F.round(safe_num * (F.lit(1.0) + (F.rand() - F.lit(0.5)) * F.lit(0.04)), 2),
        )

    # 3. Identifiant unique par réplique.
    if "customerID" in df.columns:
        out = out.withColumn(
            "customerID",
            F.concat_ws("-", F.col("customerID"), F.concat(F.lit("r"), F.col("_rep"))),
        )

    # 4. On retire la colonne technique : Bronze conserve le schéma source.
    return out.drop("_rep")


def main():
    from src.ingestion.download_data import download

    csv_path = download()
    spark = config.build_spark(app_name="churn-ingestion")

    # CSV sur le disque LOCAL du driver -> on le lit via pandas puis on le
    # distribue ; c'est l'amplification qui génère le vrai volume côté cluster.
    import pandas as pd
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
    pdf = pd.read_csv(csv_path)
    df = spark.createDataFrame(pdf)
    print(f"[bronze] Source : {df.count()} lignes, {len(df.columns)} colonnes")

    df = amplify(df, config.AMPLIFY_FACTOR)
    print(f"[bronze] Après amplification x{config.AMPLIFY_FACTOR} : {df.count()} lignes")

    out = config.s3_path("bronze")
    df.write.mode("overwrite").option("header", "true").csv(out)
    print(f"[bronze] Écrit -> {out}")
    spark.stop()


if __name__ == "__main__":
    main()