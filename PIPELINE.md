# Pipeline Data Engineering — Churn (zones Bronze → Silver → Gold)

Documentation du pipeline ETL et **guide de relance**.
Périmètre Data Engineer : ingestion, nettoyage et préparation des données.

---

## 1. Relancer le pipeline de A à Z

> Prérequis : service **Vscode-pyspark** lancé sur Onyxia avec **S3 activé**
> (Enabled = ON) et **Path style access = ON**. Java + Spark sont fournis par
> ce service. Renseigner le `.env` (clés Kaggle + `S3_BUCKET` = votre login
> Onyxia).

```bash
# 0. (une fois) cloner + configurer
git clone https://github.com/adamdif/churn-architechture.git
cd churn-architechture
cp .env.example .env        # renseigner KAGGLE_USERNAME / KAGGLE_KEY / S3_BUCKET

# 1. Créer les zones S3 + dérouler tout le pipeline
bash setup_bucket.sh

# --- ou, étape par étape (utile en debug) ---
python -m src.ingestion.create_buckets       # zones S3 (idempotent)
python -m src.ingestion.load_to_bronze        # download Kaggle + amplif -> Bronze
python -m src.engineering.bronze_to_silver    # nettoyage -> Silver
python -m src.engineering.silver_to_gold      # features + KPIs -> Gold

# 2. Rapport de qualité (Bronze ET Silver)
python -m src.engineering.data_quality
```

**Important — cohérence des zones :** après toute modification d'une étape
amont (ex. `load_to_bronze`), il faut **relancer les étapes aval** pour que
Bronze, Silver et Gold restent cohérents. Le rapport de qualité permet de
détecter une désynchronisation (types ou valeurs incohérents entre zones).

Le pipeline est **idempotent** (`mode("overwrite")`) : on peut le relancer
sans dupliquer les données. Onyxia réinitialisant le stockage tous les 7
jours, on **régénère** les données plutôt que de les versionner.

---

## 2. Étapes et choix techniques

### 2.1 Ingestion + amplification (`load_to_bronze.py`)

Telco = 7 043 lignes : trop petit pour justifier Spark. On **amplifie** pour
atteindre une volumétrie Big Data (x100 ≈ 700 k, x1000 ≈ 7 M lignes).

**Amplification distribuée.** La réplication se fait en une seule
transformation Spark (`array_repeat` + `posexplode`) : chaque ligne source
est dupliquée `factor` fois, en parallèle sur le cluster. On évite ainsi une
boucle Python qui empilerait `factor` `union` successifs — un plan logique
qui sature le driver et ne passe pas à l'échelle.

**Jitter maîtrisé.** Un bruit ±2 % est appliqué **uniquement** à
`MonthlyCharges` (seule variable réellement continue, montant), arrondi à 2
décimales. Sont **volontairement exclues** du bruit :
- `tenure` : entier borné (0–72 mois) ; le bruiter le rendrait fractionnaire
  et dépasserait le maximum réel (anomalie repérée via le rapport qualité) ;
- `SeniorCitizen` : binaire 0/1 ;
- les variables catégorielles (`Contract`, `PaymentMethod`…) ;
- `TotalCharges` : non traitée en Bronze (voir principe Medallion ci-dessous).

L'unicité des lignes est garantie par un suffixe `-rN` sur `customerID`.

> **Principe Medallion respecté** : Bronze = copie fidèle de la source,
> défauts compris. Aucun nettoyage/cast n'y est fait ; cela appartient à
> Silver. (C'est pourquoi `TotalCharges`, qui contient des chaînes vides dans
> le Telco brut, est laissée intacte ici.)

### 2.2 Nettoyage (`bronze_to_silver.py`)

| Transformation | Justification |
|---|---|
| Noms en `snake_case` | Convention de schéma homogène, lisible en SQL/ML |
| `total_charges` : `''` → `NULL` → `double` | Chaînes vides du Telco brut converties proprement |
| `churn` : Yes/No → 1/0 | Cible exploitable directement par le ML |
| Imputation `total_charges` ← `tenure × monthly_charges` | Estimation cohérente pour les clients très récents (tenure faible) |
| `dropDuplicates()` + `na.drop("all")` | Suppression des doublons et lignes vides |

### 2.3 Features + KPIs (`silver_to_gold.py`)

Base de référence (extensible par les Data Scientists / Analysts) :
`tenure_years`, `tenure_bucket`, `avg_monthly_spend`, `num_services`, et un
agrégat KPI par type de contrat (taux de churn, revenu moyen).

### 2.4 Rapport de qualité (`data_quality.py`)

Diagnostic par zone : nb lignes/colonnes/doublons, taux de manquants
(**null OU chaîne vide**), cardinalité (`approx_count_distinct`, scalable),
stats descriptives des numériques, et **anomalies signalées** (nulls élevés,
numérique stockée en texte, colonne constante, quasi-identifiant). Sortie
console + Parquet sur S3 (`logs/quality_report/<zone>`).

---

## 3. Optimisations appliquées

- **`cache()` + un seul `count()`** par script : sans cache, chaque action
  (count, write) relit la source depuis S3. On matérialise une fois.
- **Écriture unique en table Hive externe** (`option("path", ...)`) : évite le
  double stockage (un write Parquet *puis* un `saveAsTable` managé écrivaient
  la donnée deux fois). La table Hive pointe directement sur le Parquet S3.
- **`approx_count_distinct`** dans le rapport qualité : pas de shuffle global
  coûteux pour un simple diagnostic de cardinalité.

---

## 4. Règles de sécurité

- Aucun credential en dur dans le code. Clés Kaggle dans `.env` (gitignoré) ;
  credentials S3 = tokens STS **temporaires** injectés par Onyxia.
- Données brutes jamais versionnées : régénérées via les scripts.

---

## 5. Limites connues / pistes

- Le jitter sur `MonthlyCharges` peut légèrement dépasser le maximum réel ;
  acceptable pour une variable monétaire continue (sinon : bornage explicite).
- `AMPLIFY_FACTOR` réglable via `.env` (10 pour tester, 100–1000 en charge).