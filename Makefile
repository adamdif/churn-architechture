# =====================================================================
#  Makefile — orchestration du pipeline churn.
#  `make all` = équivalent de setup_bucket.sh, mais étape par étape.
# =====================================================================
.PHONY: help setup buckets ingest silver gold all test clean

help:           ## Affiche cette aide
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

setup:          ## Installe les dépendances Python
	pip install --upgrade pip && pip install -r requirements.txt

buckets:        ## Crée/vérifie les zones S3 (idempotent)
	python -m src.ingestion.create_buckets

ingest:         ## Télécharge + amplifie + charge en Bronze
	python -m src.ingestion.load_to_bronze

silver:         ## ETL Bronze -> Silver (nettoyage)
	python -m src.engineering.bronze_to_silver

gold:           ## ETL Silver -> Gold (features + KPIs)
	python -m src.engineering.silver_to_gold

all: setup buckets ingest silver gold  ## Pipeline complet de A à Z

clean:          ## Supprime les artefacts locaux (pas le S3)
	rm -rf /tmp/churn_data __pycache__ .pytest_cache spark-warehouse metastore_db derby.log
