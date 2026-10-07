-- Data warehouse : schéma en étoile (MLD de l'exercice 2).
-- Exécuté par PostgreSQL au premier démarrage du service "dw" (volume vide).
-- Les tables sont remplies par les pipelines Apache Hop (exercice-2/hop).

CREATE TABLE dim_temps (
    jour       date     PRIMARY KEY,
    annee      smallint NOT NULL,
    trimestre  smallint NOT NULL,
    mois       smallint NOT NULL
);

CREATE TABLE dim_departement (
    departement_code varchar(3) PRIMARY KEY,
    departement_nom  varchar(64),
    region_code      smallint,
    region_nom       varchar(64)
);

CREATE TABLE dim_entreprise (
    siren         char(9) PRIMARY KEY,
    type_personne varchar(2)            -- pm = société, pp = personne physique, NULL = inconnu
);

-- Une annonce BODACC (création ou radiation)
CREATE TABLE fait_annonce (
    annonce_id       varchar     PRIMARY KEY,
    type_annonce     varchar(16) NOT NULL,   -- creation | radiation
    jour             date        NOT NULL REFERENCES dim_temps,
    departement_code varchar(3)  REFERENCES dim_departement,
    siren            char(9)     REFERENCES dim_entreprise
);

-- Un bilan déposé (comptes annuels)
CREATE TABLE fait_bilan (
    siren            char(9)     NOT NULL REFERENCES dim_entreprise,
    date_cloture     date        NOT NULL REFERENCES dim_temps,
    type_bilan       char(1)     NOT NULL,   -- C complet, S simplifié, K consolidé
    confidentialite  varchar(64),
    chiffre_affaires bigint,                 -- code FL de la liasse
    impot_benefices  bigint,                 -- code HK de la liasse
    PRIMARY KEY (siren, date_cloture, type_bilan)
);

CREATE INDEX ON fait_annonce (jour);
CREATE INDEX ON fait_annonce (siren);
CREATE INDEX ON fait_bilan (date_cloture);

-- Ratio d'impôt (HK / FL × 100) : calculé à la lecture, pas stocké, car un ratio ne s'additionne pas.
CREATE VIEW v_ratio_impot AS
SELECT b.*,
       t.annee,
       100.0 * b.impot_benefices / nullif(b.chiffre_affaires, 0) AS ratio_impot
FROM fait_bilan b
JOIN dim_temps t ON t.jour = b.date_cloture;
