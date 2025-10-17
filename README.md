# Retail ETL Pipeline with Spark and PostgreSQL

This project implements an ETL (Extract, Transform, Load) pipeline using Apache Spark to process retail data and load it into a PostgreSQL data warehouse. The pipeline reads user, product, and transaction data, transforms it, and stores the results in a `fact_sales` table in a PostgreSQL database. The entire setup runs in Docker containers for easy deployment.

## Prerequisites

- **Docker** and **Docker Compose** installed on your system.
- **DBeaver** (or another SQL client) for database access.
- Input data files (`users.json`, `products.csv`, `transactions.csv`) in the `book_data/` directory.
- Basic familiarity with Spark, PostgreSQL, and Docker.

## Setup Instructions

1. **Clone the Repository**
   ```bash
   git clone <repository_url>
   cd <repository_directory>
   ```

2. **Prepare Input Data**
   Ensure the following files are in the `./book_data/` directory:
   - `users.json`: User data in JSON format.
   - `products.csv`: Product data in CSV format.
   - `transactions.csv`: Transaction data in CSV format.

3. **Create Environment File**
   Ensure the `.env.spark` file exists with the following content:
   ```
   SPARK_NO_DAEMONIZE=true
   JDBC_POSTGRES_PATH=jar/postgresql-42.2.23.jar
   JDBC_POSTGRES_URL=jdbc:postgresql://postgres:5432/data_warehouse
   POSTGRES_USER=root
   POSTGRES_PASSWORD=password
   TABLE_NAME=fact_sales
   ```

4. **Build and Start Docker Containers**
   ```bash
   # Build the custom Spark image
   docker-compose build --no-cache

   # Start all services (Postgres, Spark master, worker, history server)
   docker-compose up -d
   ```

5. **Verify Services**
   - Check that all containers are running:
     ```bash
     docker-compose ps
     ```
   - Verify PostgreSQL is ready:
     ```bash
     docker-compose logs postgres | grep "database system is ready"
     ```
     Expected output: `database system is ready to accept connections`

## Running the ETL Pipeline

1. **Execute the Spark Job**
   ```bash
   docker-compose exec spark-master spark-submit /opt/spark/apps/scripts/main.py
   ```

2. **Expected Output**
   ```
   ✅ Data loaded to fact_sales
   ✅ Finished ETL pipeline load to database
   ```

3. **Stop Containers (when done)**
   ```bash
   docker-compose down
   ```

## Connecting PostgreSQL to DBeaver

To explore the `data_warehouse` database and the `fact_sales` table, follow these steps to connect PostgreSQL to DBeaver:

1. **Open DBeaver**
   - Launch DBeaver on your machine.

2. **Create a New Database Connection**
   - Click **Database** > **New Connection** in the top menu.
   - Select **PostgreSQL** from the list of database types and click **Next**.

3. **Enter Connection Details**
   - **Host**: `localhost` (since PostgreSQL is exposed on port 5434 on your host machine).
   - **Port**: `5434` (as defined in `docker-compose.yaml` with `ports: - 5434:5432`).
   - **Database**: `data_warehouse` (as specified in `POSTGRES_DB`).
   - **Username**: `root` (as specified in `POSTGRES_USER`).
   - **Password**: `password` (as specified in `POSTGRES_PASSWORD`).

   Example settings:
   ```
   Host: localhost
   Port: 5434
   Database: data_warehouse
   Username: root
   Password: password
   ```

4. **Test the Connection**
   - Click **Test Connection** in DBeaver.
   - If prompted, download the PostgreSQL JDBC driver (DBeaver will handle this automatically).
   - You should see a success message if the connection is valid.

5. **Explore the Database**
   - Once connected, expand the `data_warehouse` database in DBeaver’s Database Navigator.
   - Navigate to **Schemas** > **public** > **Tables** to see the `fact_sales` table.
   - Right-click `fact_sales` and select **View Data** to inspect the loaded data.

6. **Troubleshooting Connection Issues**
   - **Error: Connection refused**:
     - Ensure the Postgres container is running (`docker-compose ps`).
     - Verify the port mapping (`5434:5432`) in `docker-compose.yaml`.
   - **Error: Database "root" does not exist**:
     - This should not occur with the provided configuration. If it does, double-check the database name in DBeaver (`data_warehouse`).
   - **Error: Authentication failed**:
     - Confirm the username (`root`) and password (`password`) match the `docker-compose.yaml` settings.

## Project Details

### Data Flow
1. **Extract**: Reads `users.json`, `products.csv`, and `transactions.csv` using Spark (`extract.py`).
2. **Transform**: Cleans and joins data:
   - Converts price strings to integers and extracts currency.
   - Formats transaction dates to `yyyy-MM-dd`.
   - Handles null emails.
   - Joins transactions, users, and products into a single DataFrame (`transform.py`).
3. **Load**: Writes the transformed data to the `fact_sales` table in PostgreSQL (`load.py`).

### Database Schema
The `fact_sales` table is created by `init.sql` with the following schema:
```sql
CREATE TABLE fact_sales (
    transaction_id   BIGINT PRIMARY KEY,
    product_id       INT NOT NULL,
    user_id          INT NOT NULL,
    quantity         INT NOT NULL,
    transaction_date DATE NOT NULL,
    email            VARCHAR(100),
    join_date        DATE,
    name             VARCHAR(50),
    product_name     VARCHAR(50),
    category         VARCHAR(30),
    price            BIGINT NOT NULL,
    currency         VARCHAR(5)
);
```

### Dependencies
- **Python Libraries** (from `requirements.txt`):
  - `openpyxl`, `pandas`, `pyspark`, `psycopg2-binary`, `python-dotenv`, `ua_parser[re2]`
- **Docker Images**:
  - `postgres:12`
  - Custom Spark image (`da-spark-image`) built from `Dockerfile`
- **Spark**: Version 3.5.7 with Hadoop 3
- **PostgreSQL JDBC Driver**: `postgresql-42.2.23.jar`

## Troubleshooting

- **Spark Job Fails with "database root does not exist"**:
  - Verify `JDBC_POSTGRES_URL` in `setting.py` and `.env.spark` is `jdbc:postgresql://postgres:5432/data_warehouse`.
  - Check the debug output in `main.py` to confirm the URL at runtime.
- **Ping Error: `ping: postgres: No address associated with hostname`**:
  - Ensure all services are on the `spark-net` network (`docker network inspect spark-net`).
  - Confirm Postgres is healthy: `docker-compose logs postgres`.
- **JDBC Driver Missing**:
  - Verify `/opt/spark/jars/postgresql-42.2.23.jar` exists in the Spark container.
- **Data Not Loading**:
  - Check input files in `./spark_apps/scripts/data`.
  - Inspect Spark logs: `docker-compose logs spark-master`.
