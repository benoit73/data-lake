FROM quay.io/jupyter/scipy-notebook:latest
RUN pip install --no-cache-dir boto3 s3fs pyarrow duckdb
COPY --chmod=755 start-with-password.sh /usr/local/bin/start-with-password.sh
CMD ["start-with-password.sh"]
