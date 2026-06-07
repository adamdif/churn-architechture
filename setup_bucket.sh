#!/usr/bin/env bash
# =====================================================================
#  setup_bucket.sh — Reconstruit TOUT l'environnement en une commande.
#
#  Onyxia réinitialise le stockage S3 tous les 7 jours : ce script
#  permet à n'importe quel membre de régénérer son dataset de zéro,
#  de manière autonome et reproductible.
#
#  Usage :   bash setup_bucket.sh
#  Pré-requis : service Onyxia lancé avec S3 activé, fichier .env rempli
#               (au moins KAGGLE_USERNAME / KAGGLE_KEY).
# =====================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

echo "==> [1/5] Chargement des variables d'environnement (.env)"
if [ -f .env ]; then
  set -a; source .env; set +a
else
  echo "    .env absent : on suppose que les variables sont déjà exportées (Onyxia)."
fi

echo "==> [2/5] Installation des dépendances Python"
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt

echo "==> [3/5] Vérification de la configuration"
python -m src.config

echo "==> [4/5] Création des zones S3 (idempotent)"
python -m src.ingestion.create_buckets

echo "==> [5/5] Pipeline Medallion : Bronze -> Silver -> Gold"
python -m src.ingestion.load_to_bronze
python -m src.engineering.bronze_to_silver
python -m src.engineering.silver_to_gold

echo ""
echo "==> Terminé. Données régénérées dans les zones bronze/silver/gold."
echo "    Tables Hive disponibles : churn_db.silver_churn_clean,"
echo "    churn_db.gold_churn_features, churn_db.gold_churn_kpis"
