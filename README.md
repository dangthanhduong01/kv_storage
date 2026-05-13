# KV Storage

Key-value storage written in Go. The project uses Apache Thrift for RPC layer, PostgreSQL as persistent storage


## Require

- Go `1.24.5`
- PostgreSQL 16
- Apache Thrift compiler.

## Environment

```bash
export DNS="user=postgres dbname=storage sslmode=disable password=1234 host=localhost port=5432"
export HOST="localhost" # host server
export PORT="9090" # port server
```

## Database

Start PostgreSQL local with Docker:

```bash
./startdb.sh
```

Create data:

```bash
docker exec dbos-db psql -U postgres -c "CREATE DATABASE storage;"
docker exec dbos-db psql -U postgres -d storage -c "CREATE TABLE wallet (key varchar(255) PRIMARY KEY, value TEXT);"
```

## Run Thrift server

`go run ./server`

## Run HTTP API gateway

`go run ./client`

API client listen to `localhost:8080`. Swagger UI:

```text
http://localhost:8080/docs/index.html
```

## Generate Thrift code

`./gen_thrift.sh`

Or

```bash
thrift -r --gen go ./kv.thrift
```

## Run test in all project

```bash
go test ./...
```

### Run test in dir `test`:

```bash
go test ./test
```
