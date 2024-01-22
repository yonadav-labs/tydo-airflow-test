# Tydo Apache Airflow Test Task

An Airflow DAG for a Tydo test task.

## Module Structure

* dags
* airflow.cfg
* webserver_config.py


### Installation

This package is supposed to run on [Airflow 2.8.1](https://airflow.apache.org/docs/apache-airflow/stable/start.html). (the latest version as of 01/22/2024)

```
python3 -m venv venv
source venv/bin/activate

AIRFLOW_VERSION=2.8.1
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
pip install psycopg2
pip install pandas
```

### Configuration

It uses some configuration variables (Airflow Variable).

* `CSV_FILE_PATH`: input csv file path
* `DATA_WAREHOUSE_CONNECTION_URI`: data warehouse uri
* `OUTPUT_CSV_FILE_NAME`: output csv file path

### Test on local

* Please update `sql_alchemy_conn` with a proper db connection string in `airflow.cfg`. I am using Postgresql instead of the default Sqlite for better performance.

* Run the following commands:

```
source venv/bin/activate
export AIRFLOW_HOME=<the project path>
airflow standalone
```

### Workflow

The DAG consists of 3 tasks (ingestion, processing, and final). It leverages XComs and Apache Variable.

#### Ingestion

It mimics API ingestion in the production.

* It reads the input CSV into a pandas data frame and filters out records with missing ages.
* It saves specific columns into a database table (virtual data warehouse).

#### Processing

It processes data ingested in the previous task. 

* It reads data from the data warehouse.
* It generates the year of birth (`YOB`) column by processing the `DOB` column.
* It saves the processed data into a new database table.
* It calculates some simple stats.
    * Male age mean
    * Male age median
    * Female with child age mean
    * Female with child age median

#### Final

It reads data from the final database table and exposes it as a new CSV.


### Scalability

The DAG is 100% configurable via Airflow Variables and highly scalable with proper input files.


### Logging and Monitoring

It uses the default Python logging module. Airflow supports comprehensive status tracking and logging systems. We can make wise use of them.
