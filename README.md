# Processus décisionnel et modèles de données

Question de départ : une baisse d'impôts stimulerait-elle la création d'entreprises, et comment en mesurer l'effet ?
Les données viennent du BODACC (annonces de création et de radiation) et des bilans déposés par les entreprises
(data.gouv.fr).

| Dossier | Contenu |
|---|---|
| [`exercice-2/`](exercice-2/) | Traitements no code avec **Apache Hop**, du data lake au data warehouse, et modèle du data warehouse (MCD / MLD) |
| [`exercice-3/`](exercice-3/) | Environnement Docker : data lake S3 (Garage), Airflow, Jupyter, Hop et data warehouse PostgreSQL |
| `data/` | Copies locales des fichiers sources, non versionnées (le data lake se remplit tout seul, voir exercice 3) |

Démarrage rapide :

```bash
cd exercice-3
cp .env.example .env
docker compose up -d --build
```

Le détail est dans [`exercice-3/README.md`](exercice-3/README.md).
