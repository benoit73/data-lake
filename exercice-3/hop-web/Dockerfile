# Hop Web : l'interface graphique de Hop dans le navigateur, avec le pilote JDBC DuckDB en plus.
FROM apache/hop-web:2.19.0
ADD --chown=hop:hop \
    --checksum=sha256:04ae3375d7c40dc88dc9bf0014b82ba5e66715564584af4e3abea39c7ba144d3 \
    https://repo1.maven.org/maven2/org/duckdb/duckdb_jdbc/1.5.6.0/duckdb_jdbc-1.5.6.0.jar /usr/local/tomcat/jdbc-drivers/
