# Data lake local : Garage (S3) + Airflow + Jupyter

Croise les annonces BODACC de radiation avec les données financières des entreprises (bilans),
puis calcule un ratio d'impôt (`HK / FL × 100`) et son évolution annuelle.

## Démarrage

```bash
cp .env.example .env   # une seule fois
docker compose up -d
```

C'est tout. Au premier démarrage :

1. **Garage** (stockage S3) s'initialise : clé d'accès et buckets `raw` et `processed`.
2. **Airflow** lance le DAG `telecharger_sources`, qui télécharge les fichiers sources dans `s3://raw`
   (environ 3,7 Go, compter quelques minutes). Il vérifie que chaque fichier est complet et réessaie sinon.
3. **Jupyter** est prêt avec le notebook de traitement.

Suis le téléchargement sur Airflow. Une fois le DAG terminé (en vert), ouvre le notebook
`work/traitement_annonces_bilans.ipynb` dans Jupyter et lance **Kernel → Restart Kernel and Run All Cells…**

## Services

| Service | URL | Accès |
|---|---|---|
| Jupyter | http://localhost:8888 | mot de passe `jupyter` |
| Airflow | http://localhost:8080 | sans login |
| Garage, interface web | http://localhost:3909 | sans login |
| Garage, API S3 | http://localhost:3900 | clés dans `.env`, région `garage` |

Les ports n'écoutent que sur `127.0.0.1`. Les valeurs de `.env.example` sont publiques : modifie-les dans
ton `.env` si besoin, **avant** le premier démarrage. La clé S3 n'est importée qu'à l'initialisation de Garage.

## Sources

| Fichier dans `s3://raw` | Source |
|---|---|
| `annonces-commerciales_radiations.parquet` | [BODACC](https://www.bodacc.fr), annonces de radiation |
| `export-detail-bilan.parquet` | [data.gouv.fr](https://www.data.gouv.fr), données financières détaillées des entreprises |

Pour forcer un nouveau téléchargement, supprime le fichier du bucket `raw` : le DAG, qui passe une fois
par jour, le récupère au passage suivant (ou déclenche-le à la main dans Airflow).

## Organisation

```
docker-compose.yml
garage/          configuration et initialisation de Garage
airflow/dags/    DAG de téléchargement des sources
jupyter/         image Jupyter (DuckDB, boto3, s3fs) avec mot de passe
notebooks/       notebook de traitement (monté dans Jupyter sous work/)
```

`docker compose down` arrête tout en gardant les données. `docker compose down -v` supprime aussi les volumes :
les fichiers seront retéléchargés au prochain démarrage.
