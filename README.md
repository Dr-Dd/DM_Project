# DM_Project
Comparison of a RDBMS and Triplestore in the representation of IMDb's datasets.

## Premise

The project takes the IMDb's Non-Commercial datasets, converts them in the two formats required respectively for each database and serves a PostgreSQL and Apache Jena Fuseki local endpoint. All of this is orchestrated via docker compose. The apache jena fuseki dataset has been built via tdb2.xloader and needs to be loaded separately in the named volume after local ingestion. You could modify the compose to include your own loading image. The PostgreSQL dataset is generated and piped via the COPY protocol during conversion at startup, this step is ignored if the database is already populated.

To explore the SPARQL endpoint feel free to spin up yasgui.org on localhost:3030/ds. To explore the SQL endpoint an adminer docker image is used and server on port 8080. The credentials for the SQL connection are by default "postgres:postgres", the name of the SQL database is "imdb".
