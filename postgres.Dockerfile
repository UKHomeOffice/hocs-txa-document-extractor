FROM postgres:14.2

# .sql or .sh scripts in /docker-entrypoint-initdb.d/ are executed after initdb
# allowing additional initialization
COPY ./src/test/resources/configure_postgres.sql /docker-entrypoint-initdb.d/
