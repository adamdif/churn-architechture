# ONBOARDING — Projet Prédiction du Churn

Guide d'arrivée pour tout membre de l'équipe. En suivant ces étapes, vous
déployez et régénérez l'environnement complet **de zéro, en autonomie**.

> Onyxia réinitialise le stockage S3 **tous les 7 jours**. Les données
> ne survivent donc pas entre les sessions : on ne les versionne pas, on les
> **régénère** avec les scripts de ce dépôt (voir §3).

---

## 1. Lancer son environnement sur Onyxia

1. Aller sur <https://datalab.sspcloud.fr> et se connecter (identifiants institutionnels).
2. Lancer le service **Jupyter Spark (PySpark)** depuis le catalogue, en ajustant en laissant
   la configuration par défaut et en veillant bien à cocher **« Activer la connexion S3 automatique »**
4. Pour l'**ingénierie de données** (scripts ETL), utiliser **VS Code** (service
   « VSCode-python » du catalogue) plutôt que Jupyter - voir §6.
5. Cloner le dépôt puis lancer le script setup_bucket.sh :
   ```bash
   git clone https://github.com/<org>/churn-project.git
   cd churn-project
   cp .env.example .env        # puis renseigner KAGGLE_USERNAME / KAGGLE_KEY
   bash setup_bucket.sh           # recrée S3 + régénère bronze/silver/gold
   ```

---

## 2. Structure des buckets S3 à recréer

Bucket racine : `churn-project` (ou votre nom d'utilisateur SSP Cloud, via `S3_BUCKET`).

```
churn-project/
├── bronze/churn_raw/          # données brutes (CSV), jamais modifiées
├── silver/churn_clean/        # données nettoyées (Parquet)
├── gold/
│   ├── churn_features/        # variables enrichies pour le ML
│   ├── churn_predictions/     # sorties du modèle (proba, score risque)
│   └── churn_kpis/            # indicateurs agrégés pour le dashboard
├── models/                    # modèles ML sérialisés
└── logs/
```

Création automatique et idempotente : `make buckets` (ou inclus dans `setup_bucket.sh`).

---

## 3. Régénérer les données (reproductibilité)

Aucune donnée brute n'est dans Git. La source est re-téléchargée à la demande
via l'**API Kaggle**, puis amplifiée pour atteindre une volumétrie Big Data.

```bash
make ingest    # download Kaggle -> amplification -> Bronze
make silver    # nettoyage -> Silver
make gold      # features + KPIs -> Gold
# ou tout d'un coup :
make all
```

- Source : `blastchar/telco-customer-churn` (7 043 lignes × 21 colonnes).
- Amplification : `AMPLIFY_FACTOR` (100 ≈ 700 k lignes, 1000 ≈ 7 M lignes),
  c'est ce volume qui justifie le traitement distribué Spark.
- Notez aussi que ici les `make silver`, `make gold` sont ici pour tester
  et montrer que la création des environnements marche bien, il faudra
  évidemment les ajuster pendant les phases suivantes du projet

---

## 4. Conventions de nommage et règles par zone

| Zone   | Format   | Contenu                                   | Qui écrit                     |
|--------|----------|-------------------------------------------|-------------------------------|
| Bronze | CSV brut | Source telle quelle, **jamais modifiée**  | Data Engineer (ingestion)     |
| Silver | Parquet  | Nettoyé, typé, dédupliqué, schéma validé  | Data Engineer (ETL)           |
| Gold   | Parquet  | Features (ML) + KPIs (dashboard)          | Data Scientist / Data Analyst |

Conventions :
- Colonnes en **snake_case** (`monthly_charges`, `total_charges`…).
- Cible : `churn` en **0/1** (et non Yes/No) dès la zone Silver.
- Préfixes S3 définis **uniquement** dans `src/config.py`, ne pas coder un
  chemin `s3a://...` en dur ailleurs, passer par `config.s3_path("silver")`.
- Tables Hive : base `churn_db`, tables `silver_churn_clean`,
  `gold_churn_features`, `gold_churn_kpis`.

---

## 5. Gestion des credentials S3

- **Jamais** de token ou de clé en dur dans le code ou les notebooks.
- Les credentials S3 (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
  `AWS_SESSION_TOKEN`…) sont **injectés automatiquement par Onyxia** quand
  le service est lancé avec la connexion S3 activée.
- Hors Onyxia : utiliser un fichier `.env` (déjà dans `.gitignore`).
- Les clés Kaggle passent aussi par variables d'env (`KAGGLE_USERNAME`, `KAGGLE_KEY`).
- Spark sur le SSP Cloud utilise des **tokens STS temporaires** : la session
  est configurée avec le `TemporaryAWSCredentialsProvider`
  (voir `src/config.build_spark()`). Sans ça, l'accès S3A échoue.

---

## 6. VS Code (engineering) vs Notebooks (data science)

Séparation imposée — reflétée par l'arborescence du dépôt :

```
src/                      ← VS Code · code de production, modulaire & testable
├── config.py             ← config centrale (S3, Spark, Hive)
├── ingestion/            ← scripts .py : download Kaggle, amplification, Bronze
└── engineering/          ← scripts .py : ETL Bronze→Silver→Gold

notebooks/                ← Jupyter · UNIQUEMENT exploration & modélisation
├── exploration/          ← EDA en lecture sur Silver
└── modeling/             ← itération modèle ML en lecture sur Gold
```

- **VS Code** pour tout l'ETL : les scripts `Bronze → Silver → Gold` sont du
  code de production (modulaires, versionnés, **testés** via `make test`).
- **Jupyter** seulement pour l'exploration/visualisation et l'itération ML,
  en **lecture seule** sur Silver et Gold.

---

## 7. Démarrage rapide (en résumé)

```bash
git clone https://github.com/adamdif/churn-architechture.git && cd churn-project
cp .env.example .env          # renseigner les clés Kaggle
bash setup_bucket.sh          # tout régénérer
```
