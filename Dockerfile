FROM postgres:14.0 AS builder

RUN apk add --no-cache gettext

COPY create_schema.sql \
    create_functions.sql \
    drop_schema.sql \
    partitions.sql.template \
    /opt/scripts/

COPY deploy.sh /docker-entrypoint-initdb.d/

RUN chmod a+r -R /opt/scripts
