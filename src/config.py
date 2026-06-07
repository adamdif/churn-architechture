"""
Configuration centrale du projet churn-prediction.

RÈGLE DE SÉCURITÉ (cf. ONBOARDING.md) :
    Aucun credential n'est écrit en dur dans ce fichier.
    Tout est lu depuis les variables d'environnement injectées par Onyxia.
"""
import os

try:
    from dotenv import load_dotenv
    load_dotenv()  # charge le .env du dossier courant dans l'environnement
except ImportError:
    pass

# ---------------------------------------------------------------------------
# 1. Stockage S3 / MinIO
# ---------------------------------------------------------------------------
# Bucket racine du projet. Sur le SSP Cloud, c'est généralement votre nom
# d'utilisateur. Surchargez via la variable d'env S3_BUCKET si besoin.
S3_BUCKET = os.environ.get("S3_BUCKET", "churn-project")

# Endpoint MinIO du SSP Cloud (hostname sans schéma dans AWS_S3_ENDPOINT).
_RAW_ENDPOINT = os.environ.get("AWS_S3_ENDPOINT", "minio.lab.sspcloud.fr")
S3_ENDPOINT = _RAW_ENDPOINT if _RAW_ENDPOINT.startswith("http") else f"https://{_RAW_ENDPOINT}"

# Préfixes des trois zones Medallion + annexes.
PREFIXES = {
    "bronze":      "bronze/churn_raw",
    "silver":      "silver/churn_clean",
    "gold_feat":   "gold/churn_features",
    "gold_pred":   "gold/churn_predictions",
    "gold_kpi":    "gold/churn_kpis",
    "models":      "models",
    "logs":        "logs",
}


def s3_path(zone: str, *parts: str) -> str:
    """Construit un chemin s3a:// complet pour une zone donnée.

    >>> s3_path("bronze")
    's3a://churn-project/bronze/churn_raw'
    """
    base = f"s3a://{S3_BUCKET}/{PREFIXES[zone]}"
    return "/".join([base, *parts]) if parts else base


# ---------------------------------------------------------------------------
# 2. Source de données (Kaggle)
# ---------------------------------------------------------------------------
# Slug Kaggle et nom de fichier. Pour un AUTRE dataset de churn,
# changez uniquement ces deux lignes.
KAGGLE_DATASET = os.environ.get("KAGGLE_DATASET", "blastchar/telco-customer-churn")
SOURCE_FILENAME = os.environ.get("SOURCE_FILENAME", "WA_Fn-UseC_-Telco-Customer-Churn.csv")

# Répertoire local temporaire (jamais versionné, jamais sur S3 en dur).
LOCAL_DATA_DIR = os.environ.get("LOCAL_DATA_DIR", "/tmp/churn_data")

# Facteur d'amplification : Telco = 7 043 lignes -> trop petit pour justifier
# Spark. On réplique le dataset pour atteindre une volumétrie Big Data.
# 100 -> ~700 k lignes ; 1000 -> ~7 M lignes. Réglable via env.
AMPLIFY_FACTOR = int(os.environ.get("AMPLIFY_FACTOR", "100"))

# Colonne cible.
TARGET_COL = "Churn"


# ---------------------------------------------------------------------------
# 3. Session Spark
# ---------------------------------------------------------------------------
def build_spark(app_name: str = "churn-pipeline", enable_hive: bool = True):
    """Crée une SparkSession configurée pour le S3 du SSP Cloud + Hive.

    Le point clé : sur Onyxia les credentials sont des tokens STS
    TEMPORAIRES (présence de AWS_SESSION_TOKEN), il faut donc le
    TemporaryAWSCredentialsProvider, sinon l'auth S3A échoue.
    """
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", ""))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
        .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
        )
    )

    if enable_hive:
        builder = (
            builder
            .config("spark.sql.warehouse.dir", s3_path("gold_feat"))
            .enableHiveSupport()
        )

    return builder.getOrCreate()


if __name__ == "__main__":
    # Petit diagnostic : affiche la config résolue (sans exposer les secrets).
    print(f"Bucket        : {S3_BUCKET}")
    print(f"Endpoint S3   : {S3_ENDPOINT}")
    print(f"Bronze        : {s3_path('bronze')}")
    print(f"Silver        : {s3_path('silver')}")
    print(f"Gold features : {s3_path('gold_feat')}")
    print(f"Dataset       : {KAGGLE_DATASET} ({SOURCE_FILENAME})")
    print(f"Amplification : x{AMPLIFY_FACTOR}")
    creds_ok = bool(os.environ.get("AWS_ACCESS_KEY_ID"))
    print(f"Credentials S3 détectés : {'oui' if creds_ok else 'NON - lancez le service avec S3 activé'}")
