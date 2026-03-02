# Optikk Backend

The Go/Gin backend service for the Optikk observability platform. It provides a REST API, JWT authentication, and multi-tenant telemetry ingestion (OTLP/HTTP) backed by MySQL and ClickHouse.

## Prerequisites

Before running the application, you must set up the required databases (MySQL and ClickHouse) and execute their respective schema scripts to initialize the tables.

### 1. Execute Database Schemas

**MySQL** (Used for user auth, teams, alerts, and dashboard configs):
```bash
# Log into the MySQL container and create the database if it doesn't exist:
docker exec -it <mysql_container_name> mysql -h 127.0.0.1 -P 3306 -u root -p<your_password> -e "CREATE DATABASE IF NOT EXISTS observability;"

# Execute the MySQL schema from the host machine into the container:
docker exec -i <mysql_container_name> mysql -h 127.0.0.1 -P 3306 -u root -p<your_password> observability < db/mysql_schema.sql
```

**ClickHouse** (Used for telemetry data: spans, logs, metrics):
```bash
# Execute the ClickHouse schema from the host machine into the container:
docker exec -i <clickhouse_container_name> clickhouse-client -h 127.0.0.1 --port 9000 -u default --password <your_password> -n < db/clickhouse_schema.sql
```

*(Adjust host, port, user, and password parameters as needed for your database connections).*

## Running the Application with Docker

Once the databases are set up and the schemas are applied, you can run the backend application using Docker.

### 1. Build the Docker Image
From the root of the project directory, run:
```bash
docker build -t optikk-backend:latest .
```

### 2. Run the Docker Container
Run the container and expose port `8080`. You will need to pass the appropriate environment variables so the container can connect to your databases. 

*Note: If your databases are running locally on your host machine (e.g., via Docker Desktop), use `host.docker.internal` for the hosts.*

```bash
docker run -d \
  --name optikk-backend \
  -p 8080:8080 \
  -e GO_ENV=production \
  -e PORT=8080 \
  -e MYSQL_HOST=host.docker.internal \
  -e MYSQL_PORT=3306 \
  -e MYSQL_DATABASE=observability \
  -e MYSQL_USERNAME=root \
  -e MYSQL_PASSWORD=root123 \
  -e CLICKHOUSE_HOST=host.docker.internal \
  -e CLICKHOUSE_PORT=9000 \
  -e CLICKHOUSE_DATABASE=observability \
  -e CLICKHOUSE_USERNAME=default \
  -e CLICKHOUSE_PASSWORD=clickhouse123 \
  -e JWT_SECRET=your_super_secret_jwt_key_that_is_long_enough \
  -e REDIS_ENABLED=false \
  -e KAFKA_ENABLED=false \
  optikk-backend:latest
```

**Environment Variables Overview:**
- `GO_ENV=production`: Enforces secure defaults (requires non-default passwords and secrets).
- `MYSQL_*`: Credentials to connect to the MySQL database.
- `CLICKHOUSE_*`: Credentials to connect to the ClickHouse database.
- `JWT_SECRET`: A secure randomly generated secret used for signing JWT auth tokens.
- `REDIS_ENABLED` / `KAFKA_ENABLED`: Toggle depending on whether you're using Redis for pub/sub (SSE) and token blacklisting, and Kafka for telemetry queuing. (Set to `true` and define target hosts/ports if using them).
