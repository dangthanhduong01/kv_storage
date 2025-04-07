#!/bin/bash
set -e

# # Check if PGPASSWORD is set
# if [[ -z "${PGPASSWORD}" ]]; then
#   echo "Error: PGPASSWORD is not set." >&2
#   exit 1
# fi

# Start Postgres in a local Docker container
docker run --rm --name=dbos-db --env=POSTGRES_PASSWORD="1234" --env=PGDATA=/var/lib/postgresql/data --volume=/var/lib/postgresql/data -p 5432:5432 -d postgres:latest

# Wait for PostgreSQL to start
echo "Waiting for PostgreSQL to start..."
for i in {1..30}; do
  if docker exec dbos-db psql -U postgres -c "SELECT 1;" > /dev/null 2>&1; then
    echo "PostgreSQL started!"
    # docker exec dbos-db psql -U postgres -c "CREATE DATABASE storage;"
    # docker exec -it dbos-db psql -U postgres -d storage -c "\c storage"
    # docker exec dbos-db psql -U postgres -d storage -c "CREATE TABLE wallet (key varchar(255) PRIMARY KEY,value TEXT);"
    # echo "Database started successfully!"
    
    break
  fi
  sleep 1
done
# Schema: storage
# Table: wallet
# Create a database in Postgres.
#check data exsit
# for i in {1..30}; do
#   if docker exec dbos-db psql -U postgres -d storage -c "SELECT * FROM wallet;" > /dev/null 2>&1; then
#     echo "PostgreSQL Data exist!"
#     docker exec dbos-db psql -U postgres -d storage -c "INSERT INTO wallet (key, value) VALUES ('1', 'my_value');"
#     docker exec dbos-db psql -U postgres -d storage -c "INSERT INTO wallet (key, value) VALUES ('2', 'my_value2');"
#     docker exec dbos-db psql -U postgres -d storage -c "INSERT INTO wallet (key, value) VALUES ('3', 'my_value3');"
#     break
#   fi
#   sleep 1
# done



# sudo docker exec -it dbos-db psql -U postgres -d storage -c "\dt storage.*"
