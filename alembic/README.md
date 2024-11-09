# Wobbly Alembic configuration

This directory contains the Alembic configuration for managing the Wobbly UWS database.
It is installed into the Wobbly Docker image and is used to check whether the schema is up-to-date at startup of the Wobbly web service.
It is also used by the Helm hook that updates the Wobbly UWS schema if `config.updateSchema` is enabled.

## Generating new migrations

For detailed instructions on how to generate a new Alembic migration, see [the Safir documentation](https://safir.lsst.io/user-guide/database/schema#create-migration).

One of the files in this directory is here only to support creating migrations.
`docker-compose.yaml` is a [docker-compose](https://docs.docker.com/compose/) configuration file that starts a PostgreSQL instance suitable for generating schema migrations.
This file is not used at runtime.
It is used by the tox environment described in the above documentation.
