"""Télécharge les fichiers sources dans s3://raw s'ils n'y sont pas déjà.

Le DAG est actif dès sa création et tourne une fois par jour : au premier
`docker compose up` il récupère tout, ensuite il ne fait rien tant que les
fichiers sont présents (supprimer un fichier du bucket le fait re-télécharger).
"""

import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq
import requests
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task

BUCKET = "raw"

SOURCES = {
    # Annonces BODACC de radiation (export généré à la volée, ~0,9 Go)
    "annonces-commerciales_radiations.parquet": (
        "https://www.bodacc.fr/api/explore/v2.1/catalog/datasets/annonces-commerciales/exports/parquet"
        "?lang=fr&refine=familleavis_lib%3A%22Radiations%22&timezone=Europe%2FBerlin"
    ),
    # Données financières détaillées des entreprises (data.gouv.fr, ~2,8 Go)
    "export-detail-bilan.parquet": "https://www.data.gouv.fr/api/1/datasets/r/c4ac8f98-2c97-4417-9070-0cbb9de03875",
}


@dag(
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["ingestion"],
)
def telecharger_sources():
    @task(retries=3, retry_delay=timedelta(minutes=2))
    def telecharger(nom: str):
        hook = S3Hook(aws_conn_id="garage_s3")
        if hook.check_for_key(nom, bucket_name=BUCKET):
            raise AirflowSkipException(f"déjà présent : s3://{BUCKET}/{nom}")

        with tempfile.TemporaryDirectory() as dossier:
            chemin = Path(dossier) / nom
            print(f"téléchargement de {SOURCES[nom]}")
            with requests.get(SOURCES[nom], stream=True, timeout=(30, 300)) as r:
                r.raise_for_status()
                with open(chemin, "wb") as f:
                    for bloc in r.iter_content(chunk_size=8 * 1024 * 1024):
                        f.write(bloc)

            # Un export coupé en route n'a pas de footer Parquet : on le rejette,
            # la tâche échoue et Airflow la relance.
            fichier = pq.ParquetFile(chemin)
            print(f"{chemin.stat().st_size / 1e9:.2f} Go, {fichier.metadata.num_rows:,} lignes")

            hook.load_file(str(chemin), key=nom, bucket_name=BUCKET, replace=True)
            print(f"envoyé : s3://{BUCKET}/{nom}")

    telecharger.expand(nom=list(SOURCES))


telecharger_sources()
