"""
RAPPORT DE QUALITÉ DES DONNÉES — livrable Data Engineer.

Produit, pour une zone donnée (bronze, silver...), un diagnostic de qualité :
    - nombre de lignes / colonnes
    - type de chaque colonne
    - taux de valeurs manquantes (null OU chaîne vide) par colonne
    - cardinalité (nb de valeurs distinctes, approximatif pour rester scalable)
    - statistiques descriptives des colonnes numériques (min/max/moyenne/écart-type)
    - lignes dupliquées
    - ANOMALIES signalées automatiquement (forte proportion de nulls,
      colonne numérique stockée en texte, colonne constante, etc.)

Le rapport est AFFICHÉ en console ET ÉCRIT sur S3 (zone logs) au format Parquet,
pour la traçabilité et la reproductibilité.

Lancer sur les deux zones (avant/après nettoyage) :
    python -m src.engineering.data_quality
Sur une zone précise :
    python -m src.engineering.data_quality --zone silver
Sans écriture S3 (juste l'affichage) :
    python -m src.engineering.data_quality --zone bronze --no-s3
"""
import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import DataFrame, Row, functions as F  # noqa: E402
from src import config  # noqa: E402


# Seuils de détection d'anomalies (ajustables, et défendables à l'oral).
NULL_RATE_WARN = 0.05      # > 5 % de manquants -> on signale
STRING_NUMERIC_WARN = 0.90  # > 90 % de valeurs parseables en nombre -> "numérique en texte"

_NUMERIC_TYPES = ("int", "bigint", "smallint", "tinyint", "double", "float", "decimal", "long")


def read_zone(spark, zone: str) -> DataFrame:
    """Lit une zone S3 en gérant son format (Bronze = CSV, Silver/Gold = Parquet)."""
    path = config.s3_path(zone)
    if zone == "bronze":
        return spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    return spark.read.parquet(path)


def compute_quality(df: DataFrame):
    """Calcule toutes les métriques de qualité en un minimum de passes Spark.

    Retourne (total_lignes, n_doublons, liste_de_lignes_resume, stats_numeriques).
    """
    # On met en cache : les métriques ci-dessous relisent df plusieurs fois.
    df = df.cache()
    total = df.count()  # déclenche le cache une seule fois
    dtypes = dict(df.dtypes)

    # --- 1. Une seule agrégation pour : manquants + cardinalité par colonne ---
    agg_exprs = []
    for c in df.columns:
        # Manquant = null OU chaîne vide (après trim). On caste en string pour
        # que le trim fonctionne quel que soit le type d'origine.
        is_missing = F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == F.lit(""))
        agg_exprs.append(F.sum(F.when(is_missing, 1).otherwise(0)).alias(f"{c}__miss"))
        # approx_count_distinct : bien moins coûteux qu'un countDistinct exact
        # (pas de shuffle global) -> indispensable à grande échelle.
        agg_exprs.append(F.approx_count_distinct(F.col(c)).alias(f"{c}__dist"))
        # Pour les colonnes texte : proportion de valeurs parseables en nombre
        # (détecte une colonne numérique stockée en texte, ex. TotalCharges).
        if dtypes[c] == "string":
            parseable = F.expr(f"try_cast(`{c}` as double)").isNotNull() & (
                F.trim(F.col(c)) != F.lit("")
            )
            agg_exprs.append(F.avg(F.when(parseable, 1.0).otherwise(0.0)).alias(f"{c}__numlike"))

    row = df.agg(*agg_exprs).collect()[0]

    # --- 2. Stats descriptives des colonnes numériques (une passe) ---
    numeric_cols = [c for c, t in df.dtypes if any(t.startswith(n) for n in _NUMERIC_TYPES)]
    num_stats = {}
    if numeric_cols:
        num_exprs = []
        for c in numeric_cols:
            num_exprs += [
                F.round(F.min(c), 2).alias(f"{c}__min"),
                F.round(F.max(c), 2).alias(f"{c}__max"),
                F.round(F.avg(c), 2).alias(f"{c}__mean"),
                F.round(F.stddev(c), 2).alias(f"{c}__std"),
            ]
        nrow = df.agg(*num_exprs).collect()[0]
        for c in numeric_cols:
            num_stats[c] = {
                "min": nrow[f"{c}__min"], "max": nrow[f"{c}__max"],
                "mean": nrow[f"{c}__mean"], "std": nrow[f"{c}__std"],
            }

    # --- 3. Lignes dupliquées (lignes strictement identiques) ---
    n_distinct_rows = df.dropDuplicates().count()
    n_dup = total - n_distinct_rows

    # --- 4. Assemblage du résumé par colonne + détection d'anomalies ---
    summary = []
    for c in df.columns:
        miss = int(row[f"{c}__miss"])
        dist = int(row[f"{c}__dist"])
        pct_miss = round(100.0 * miss / total, 2) if total else 0.0

        flags = []
        if pct_miss > NULL_RATE_WARN * 100:
            flags.append(f"nulls>{int(NULL_RATE_WARN*100)}%")
        if dtypes[c] == "string":
            numlike = row[f"{c}__numlike"]
            if numlike is not None and numlike >= STRING_NUMERIC_WARN:
                flags.append("numérique stockée en texte")
        if dist <= 1:
            flags.append("colonne constante")
        if dist >= total * 0.95 and total > 0:
            flags.append("quasi-identifiant (haute cardinalité)")

        summary.append(Row(
            colonne=c,
            type=dtypes[c],
            nb_manquants=miss,
            pct_manquants=pct_miss,
            nb_distinct_approx=dist,
            anomalies=", ".join(flags) if flags else "-",
        ))

    return total, n_dup, summary, num_stats


def print_report(zone: str, total: int, n_dup: int, summary, num_stats):
    """Affiche le rapport en console, façon revue technique."""
    print("\n" + "=" * 70)
    print(f"  RAPPORT DE QUALITÉ — zone '{zone}'")
    print("=" * 70)
    print(f"  Lignes        : {total:,}".replace(",", " "))
    print(f"  Colonnes      : {len(summary)}")
    print(f"  Doublons      : {n_dup:,}".replace(",", " "))
    print("-" * 70)
    print(f"  {'colonne':<22}{'type':<10}{'%nuls':>7}{'distinct':>10}  anomalies")
    print("-" * 70)
    for r in summary:
        print(f"  {r['colonne']:<22}{r['type']:<10}{r['pct_manquants']:>7}"
              f"{r['nb_distinct_approx']:>10}  {r['anomalies']}")
    if num_stats:
        print("-" * 70)
        print("  Statistiques descriptives (colonnes numériques) :")
        print(f"  {'colonne':<22}{'min':>12}{'max':>12}{'moyenne':>12}{'écart-type':>12}")
        for c, s in num_stats.items():
            print(f"  {c:<22}{str(s['min']):>12}{str(s['max']):>12}"
                  f"{str(s['mean']):>12}{str(s['std']):>12}")
    print("=" * 70 + "\n")


def run(spark, zone: str, write_s3: bool = True):
    df = read_zone(spark, zone)
    total, n_dup, summary, num_stats = compute_quality(df)
    print_report(zone, total, n_dup, summary, num_stats)

    if write_s3:
        # Écriture du résumé sur S3 (zone logs) au format Parquet, pour garder
        # une trace datée et réutilisable du diagnostic.
        out = config.s3_path("logs", "quality_report", zone)
        spark.createDataFrame(summary).write.mode("overwrite").parquet(out)
        print(f"[quality] Rapport écrit -> {out}\n")


def main():
    parser = argparse.ArgumentParser(description="Rapport de qualité des données")
    parser.add_argument("--zone", default="both",
                        help="bronze | silver | both (défaut: both)")
    parser.add_argument("--no-s3", action="store_true",
                        help="ne pas écrire le rapport sur S3 (affichage seulement)")
    args = parser.parse_args()

    spark = config.build_spark(app_name="data-quality")

    zones = ["bronze", "silver"] if args.zone == "both" else [args.zone]
    for z in zones:
        try:
            run(spark, z, write_s3=not args.no_s3)
        except Exception as e:
            print(f"[quality] Zone '{z}' ignorée ({type(e).__name__}: {e})")

    spark.stop()


if __name__ == "__main__":
    main()