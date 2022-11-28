# Build container
FROM prometheuscommunity/postgres-exporter

COPY autofailover_queries.yaml /etc/autofailover_queries.yaml
COPY queries.yaml /etc/queries.yaml

USER root
