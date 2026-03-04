#!/bin/sh
mkdir -p /fuseki/databases/DB2
chmod -R 777 /fuseki/databases
exec "$@"
