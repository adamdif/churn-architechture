"""
ÉTAPE 1 — Téléchargement de la source depuis Kaggle.

Re-télécharge le dataset à la demande (reproductibilité). Aucune donnée
brute n'est versionnée dans Git : seul CE script l'est.

Credentials Kaggle attendus en variables d'environnement (jamais en dur) :
    KAGGLE_USERNAME, KAGGLE_KEY
À définir dans Onyxia (onglet "Vault"/variables d'env du service) ou via .env.

Usage :
    python -m src.ingestion.download_data
"""
import os
import sys
import zipfile
from pathlib import Path

# On ajoute la racine du projet au PYTHONPATH pour permettre `import src.*`
# que le script soit lancé via `python -m` ou directement.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src import config  # noqa: E402


def download() -> Path:
    """Télécharge et décompresse le dataset. Retourne le chemin du CSV local."""
    if not os.environ.get("KAGGLE_USERNAME") or not os.environ.get("KAGGLE_KEY"):
        raise EnvironmentError(
            "KAGGLE_USERNAME / KAGGLE_KEY manquants. "
            "Définissez-les en variables d'environnement (voir .env.example), "
            "ne les écrivez jamais en dur dans le code."
        )

    # Import retardé : la lib lit les credentials env au moment de l'import.
    from kaggle.api.kaggle_api_extended import KaggleApi

    dest = Path(config.LOCAL_DATA_DIR)
    dest.mkdir(parents=True, exist_ok=True)

    csv_path = dest / config.SOURCE_FILENAME
    if csv_path.exists():
        print(f"[ingestion] Fichier déjà présent : {csv_path}")
        return csv_path

    api = KaggleApi()
    api.authenticate()
    print(f"[ingestion] Téléchargement de {config.KAGGLE_DATASET} ...")
    api.dataset_download_files(config.KAGGLE_DATASET, path=str(dest), quiet=False)

    # L'API dépose un .zip ; on le décompresse.
    for z in dest.glob("*.zip"):
        with zipfile.ZipFile(z) as zf:
            zf.extractall(dest)
        z.unlink()

    if not csv_path.exists():
        found = [p.name for p in dest.glob("*.csv")]
        raise FileNotFoundError(
            f"{config.SOURCE_FILENAME} introuvable après extraction. "
            f"CSV trouvés : {found}. Ajustez SOURCE_FILENAME dans config.py."
        )

    print(f"[ingestion] OK -> {csv_path}")
    return csv_path


if __name__ == "__main__":
    download()
