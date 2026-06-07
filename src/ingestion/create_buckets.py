"""
ÉTAPE 0 — Création / vérification des zones S3 (idempotent).

Onyxia réinitialise le stockage tous les 7 jours : ce script recrée la
structure de zones à la demande. Il utilise boto3 avec les credentials
STS injectés par Onyxia (variables d'environnement).

Usage :
    python -m src.ingestion.create_buckets
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from src import config  # noqa: E402


def main():
    client = boto3.client(
        "s3",
        endpoint_url=config.S3_ENDPOINT,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=os.environ.get("AWS_SESSION_TOKEN"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        # MinIO impose le path-style ; timeouts courts pour ne pas bloquer.
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            connect_timeout=10,
            read_timeout=30,
            retries={"max_attempts": 2},
        ),
    )

    # Sur le SSP Cloud, le bucket perso existe déjà et la politique STS
    # n'autorise PAS list_buckets / create_bucket (d'où le timeout précédent).
    # On crée donc uniquement les préfixes de zones dans le bucket existant.
    print(f"[s3] Bucket cible : {config.S3_BUCKET}")
    for zone, prefix in config.PREFIXES.items():
        key = f"{prefix}/.keep"
        client.put_object(Bucket=config.S3_BUCKET, Key=key, Body=b"")
        print(f"[s3] Zone prête : s3a://{config.S3_BUCKET}/{prefix}")


if __name__ == "__main__":
    main()
