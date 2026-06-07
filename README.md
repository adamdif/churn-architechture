# Prédiction du Churn — Infrastructure Onyxia

Infrastructure Big Data sur **Onyxia (SSP Cloud)** pour la prédiction du churn
client : stockage S3/MinIO, traitement distribué Spark, architecture Medallion
**Bronze → Silver → Gold**, et pipeline **reproductible** de bout en bout.

> Pour démarrer : lire **[ONBOARDING.md](ONBOARDING.md)** puis lancer `bash setup_bucket.sh`.

## Architecture

```
Kaggle (source)  →  BRONZE (CSV brut)  →  SILVER (Parquet nettoyé)  →  GOLD (features + KPIs)
                         │                       │                          │
                    ingestion .py          engineering .py          Data Science (notebooks)
                                                                            │
                                                              Spark MLlib → predictions → Dashboard
```

- **Stockage** : S3/MinIO (4 zones + models/logs).
- **Traitement** : Spark distribué (driver + workers) sur Kubernetes.
- **Catalogue** : Hive Metastore (base `churn_db`) pour l'accès SQL.
- **Reproductibilité** : `setup_bucket.sh` / `Makefile` régénèrent tout en une commande.

## Rôles & gouvernance

| Rôle           | Lit Bronze | Écrit Silver | Lit/Écrit Gold       | Mission                          |
|----------------|:----------:|:------------:|----------------------|----------------------------------|
| Architecte Data| ✓ (test)   | —            | — (config)           | Setup infra, buckets, README     |
| Data Engineer  | ✓          | ✓            | —                    | Ingestion + ETL (`src/`)         |
| Data Scientist | —          | ✓ (lecture)  | ✓ écrit `features/`  | Feature eng., modèle ML          |
| Data Analyst   | —          | —            | ✓ `kpis/`            | Dashboard, KPIs                  |

## Structure du dépôt

```
churn-project/
├── README.md / ONBOARDING.md      # doc projet + guide d'arrivée
├── bootstrap.sh / Makefile        # reproductibilité en 1 commande
├── requirements.txt / .env.example
├── src/                           # VS Code — code de production
│   ├── config.py
│   ├── ingestion/    (create_buckets, download_data, load_to_bronze)
│   └── engineering/  (bronze_to_silver, silver_to_gold)
├── notebooks/                     # Jupyter — exploration & modeling
│   ├── exploration/
│   └── modeling/
```
