# Airflow + Apache Hop : les tâches du DAG "chargement_dw" lancent les pipelines Hop avec hop-run.
FROM apache/hop:2.19.0 AS hop

FROM apache/airflow:3.1.0
USER root
# Hop a besoin de Java 17+ ; l'image Airflow est sous Debian (glibc), compatible avec le pilote DuckDB.
COPY --from=eclipse-temurin:21-jre /opt/java/openjdk /opt/java/openjdk
COPY --from=hop --chown=airflow:root /opt/hop /opt/hop
# Pilote JDBC DuckDB (absent de l'image Hop) : lit les fichiers Parquet du data lake.
ADD --chown=airflow:root \
    --checksum=sha256:04ae3375d7c40dc88dc9bf0014b82ba5e66715564584af4e3abea39c7ba144d3 \
    https://repo1.maven.org/maven2/org/duckdb/duckdb_jdbc/1.5.6.0/duckdb_jdbc-1.5.6.0.jar /opt/hop/lib/jdbc/
COPY --chmod=755 hop-dw /usr/local/bin/hop-dw
RUN mkdir /opt/hop-projet && chown airflow:root /opt/hop-projet
ENV JAVA_HOME=/opt/java/openjdk \
    PATH=/opt/java/openjdk/bin:$PATH
USER airflow
# Enregistre le projet Hop "dw" une fois pour toutes ; docker compose monte le vrai dossier par-dessus.
RUN cd /opt/hop && ./hop-conf.sh --project=dw --project-create --project-home=/opt/hop-projet >/dev/null
