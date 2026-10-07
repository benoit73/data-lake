"""Télécharge les fichiers sources dans s3://raw s'ils n'y sont pas déjà.

Le DAG est actif dès sa création et tourne une fois par jour : au premier
`docker compose up` il récupère tout, ensuite il ne fait rien tant que les
fichiers sont présents (supprimer un fichier du bucket le fait re-télécharger).
Quand au moins un fichier a été téléchargé, il met à jour l'asset `s3://raw`,
ce qui lance le DAG `chargement_dw`.
"""

import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq
import requests
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import Asset, dag, task

BUCKET = "raw"
RAW = Asset("s3://raw")

BODACC = (
    "https://www.bodacc.fr/api/explore/v2.1/catalog/datasets/annonces-commerciales/exports/parquet"
    "?lang=fr&timezone=Europe%2FBerlin&refine=familleavis_lib%3A%22{famille}%22"
)

SOURCES = {
    # Annonces BODACC de radiation (export généré à la volée, ~0,9 Go)
    "annonces-commerciales_radiations.parquet": BODACC.format(famille="Radiations"),
    # Données financières détaillées des entreprises (data.gouv.fr, ~2,8 Go)
    "export-detail-bilan.parquet": "https://www.data.gouv.fr/api/1/datasets/r/c4ac8f98-2c97-4417-9070-0cbb9de03875",
}
# Annonces BODACC de création : un fichier par année, car le site coupe les exports
# de plus de ~1,5 Go sans prévenir (et toutes les créations dépassent cette taille).
for annee in range(2008, date.today().year + 1):
    SOURCES[f"annonces-commerciales_creations_{annee}.parquet"] = (
        BODACC.format(famille="Cr%C3%A9ations") + f"&refine=dateparution%3A%22{annee}%22"
    )


@dag(
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["ingestion"],
)
def telecharger_sources():
    @task(retries=3, retry_delay=timedelta(minutes=2), max_active_tis_per_dagrun=3)
    def telecharger(nom: str) -> bool:
        """Renvoie True si le fichier a été téléchargé, False s'il était déjà là."""
        hook = S3Hook(aws_conn_id="garage_s3")
        if hook.check_for_key(nom, bucket_name=BUCKET):
            print(f"déjà présent : s3://{BUCKET}/{nom}")
            return False

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
            return True

    @task(outlets=[RAW])
    def signaler(telecharges: list[bool]):
        if not any(telecharges):
            raise AirflowSkipException("aucun nouveau fichier : pas de rechargement du data warehouse")
        print(f"{sum(telecharges)} nouveau(x) fichier(s) : chargement_dw va démarrer")

    signaler(telecharger.expand(nom=list(SOURCES)))


telecharger_sources()
