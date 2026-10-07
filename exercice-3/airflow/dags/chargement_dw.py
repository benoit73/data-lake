"""Du data lake au data warehouse : lance les pipelines Apache Hop de l'exercice 2.

Chaque tâche exécute un pipeline Hop (`hop-dw <fichier>`, voir airflow/hop-dw) : le graphe Airflow
montre donc le flux de l'exercice 2, étape par étape, avec les logs de Hop dans chaque tâche.

Le DAG démarre tout seul quand `telecharger_sources` a déposé de nouveaux fichiers dans s3://raw
(asset `s3://raw`). On peut aussi le lancer à la main (bouton Trigger).
"""

import os
from datetime import datetime

import psycopg2
from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Asset, dag, task

RAW = Asset("s3://raw")


def hop(fichier: str) -> BashOperator:
    return BashOperator(task_id=fichier.split(".")[0], bash_command=f"hop-dw {fichier}")


@dag(
    schedule=[RAW],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,  # chaque tâche lance une JVM Hop de 3 Go
    is_paused_upon_creation=False,
    tags=["hop", "data warehouse"],
)
def chargement_dw():
    @task
    def verifier_sources():
        hook = S3Hook(aws_conn_id="garage_s3")
        cles = hook.list_keys(bucket_name="raw") or []
        manquants = [c for c in ("annonces-commerciales_radiations.parquet", "export-detail-bilan.parquet")
                     if c not in cles]
        creations = [c for c in cles if c.startswith("annonces-commerciales_creations_")]
        if manquants or not creations:
            raise AirflowFailException(
                f"sources manquantes dans s3://raw : {manquants or 'créations'} "
                "(le DAG telecharger_sources est-il terminé ?)")
        print(f"{len(creations)} années de créations, radiations et bilans présents")

    @task
    def controler():
        """Échoue si une table est vide ; affiche les volumes et l'indicateur de l'exercice 1."""
        with psycopg2.connect(host="dw", dbname=os.environ["DW_DB"], user=os.environ["DW_USER"],
                              password=os.environ["DW_PASSWORD"]) as con, con.cursor() as cur:
            for table in ("dim_temps", "dim_departement", "dim_entreprise", "fait_annonce", "fait_bilan"):
                cur.execute(f"SELECT count(*) FROM {table}")
                n = cur.fetchone()[0]
                print(f"{table:16} {n:>12,}".replace(",", " "))
                if n == 0:
                    raise AirflowFailException(f"{table} est vide")

            cur.execute("""
                SELECT t.annee,
                       count(*) FILTER (WHERE a.type_annonce = 'creation')  AS creations,
                       count(*) FILTER (WHERE a.type_annonce = 'radiation') AS radiations
                FROM fait_annonce a JOIN dim_temps t USING (jour)
                GROUP BY t.annee ORDER BY t.annee""")
            print("\nannée   créations  radiations")
            for annee, creations, radiations in cur.fetchall():
                print(f"{annee}  {creations:>10,}  {radiations:>10,}".replace(",", " "))

    nettoyage = [hop("nettoyer_annonces.hpl"), hop("nettoyer_bilans.hpl")]
    vider = hop("vider_dw.hwf")
    dimensions = [hop("charger_dim_temps.hpl"), hop("charger_dim_departement.hpl"),
                  hop("charger_dim_entreprise.hpl")]
    faits = [hop("charger_fait_annonce.hpl"), hop("charger_fait_bilan.hpl")]

    verifier_sources() >> nettoyage >> vider >> dimensions
    for d in dimensions:
        d >> faits
    faits >> controler()


chargement_dw()
