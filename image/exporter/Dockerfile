# Build container
FROM radondb/radondb-promethues-postgres-exporter:v0.12.1

COPY autofailover_queries.yaml /etc/autofailover_queries.yaml
COPY queries.yaml /etc/queries.yaml

ENV TZ=Asia/Shanghai
USER root
