# MariaDB to BigQuery ETL with Airflow

This project provides an **ETL (Extract, Transform, Load) pipeline** to transfer data from **MariaDB** to **Google BigQuery**, orchestrated using **Apache Airflow**. It uses Docker for containerization and includes custom operators for data ingestion.

## 📂 Project Structure

```
├── config
│   └── tables.yaml              # Table mapping and configuration
│
├── credentials
│   └── service-account.json      # GCP Service Account key for BigQuery
│
├── dags
│   └── daily_etl_dag.py          # Main Airflow DAG for daily ETL
│
├── logs                          # Airflow logs
│
├── scripts
│   └── setup.py                  # Setup / initialization script
│
├── sql                           # SQL query files
│
├── src
│   └── operators
│       └── mariadb_to_bigquery_operator.py  # Custom Airflow operator
│
├── utils
│   └── config_loader.py          # Utility for loading configs
│
├── .env.example                  # Example environment variables
├── docker-compose.yml            # Docker services definition
├── Dockerfile                    # Custom Airflow image definition
├── README.md                     # Project documentation
└── requirements.txt              # Python dependencies
```

## ⚙️ Prerequisites

- **Docker & Docker Compose** installed on your machine
- **Google Cloud Service Account Key** with BigQuery access
  - Save the JSON key inside `credentials/service-account.json`
- **MariaDB Database** with accessible host/port
- Update `.env` file with your configuration

## 🚀 Setup & Run

### 1. Clone the repository

```bash
git clone <repo_url>
cd mariadb-bigquery-etl
```

### 2. Copy environment file

```bash
cp .env.example .env
```

### 3. Update environment variables inside `.env`

Example:
```bash
MARIADB_HOST=mariadb
MARIADB_USER=root
MARIADB_PASSWORD=example
MARIADB_DB=mydb

GCP_PROJECT_ID=my-gcp-project
BIGQUERY_DATASET=my_dataset
```

### 4. Start Airflow with Docker

```bash
docker-compose up -d --build
```

### 5. Access Airflow UI

- **URL**: http://localhost:8080
- **Default login**: `airflow / airflow`

## 📊 Workflow

1. **Extract** data from MariaDB
2. **Transform** if necessary (via SQL / Python scripts)
3. **Load** data into BigQuery tables

The main DAG is defined in:
```
dags/daily_etl_dag.py
```

Custom operator for MariaDB → BigQuery is located at:
```
src/operators/mariadb_to_bigquery_operator.py
```

## 📌 Notes

- Table configurations (e.g., source → destination mapping) are in `config/tables.yaml`
- BigQuery authentication requires the service account key stored in `credentials/service-account.json`

## 👤 Author

**Rafli Rizkya Sakti Nugraha** | Data Analyst Entry
