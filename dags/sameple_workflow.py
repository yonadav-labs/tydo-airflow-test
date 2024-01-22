import datetime
import logging

import pandas as pd
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from sqlalchemy import create_engine

logger = logging.getLogger('Tydo')


def ingestion(**kwargs):
    '''
    puts data in a database (certain columns) after removing missing values
    '''

    file_path = kwargs['file_path']
    logger.info('**** Ingesting %s', file_path)

    df = pd.read_csv(file_path)
    logger.info(df.describe())

    # handle missing ages
    df_clean = df.dropna(subset=['AGE'])
    logger.info(df_clean.describe())

    table_cols = ['PID', 'GENDER', 'AGE', 'DOB', 'CITY', 'STATE', 'ZIP', 'CHILD']
    df_table = df_clean.loc[:, table_cols]
    logger.info(df_table.describe())

    # save to a virtual data warehouse
    connection_uri = Variable.get("DATA_WAREHOUSE_CONNECTION_URI")
    engine = create_engine(connection_uri)

    table_name = 'customers_raw'
    df_table.to_sql(name=table_name, con=engine, if_exists='replace')

    ti = kwargs["ti"]
    ti.xcom_push("table_name", table_name)


def processing(**kwargs):
    '''
    generates the YOB column and calculates some stats mimicking transformation
    '''
    ti = kwargs["ti"]
    table_name = ti.xcom_pull(task_ids="ingestion", key="table_name")
    logger.info('**** Processing table: %s', table_name)

    connection_uri = Variable.get("DATA_WAREHOUSE_CONNECTION_URI")
    engine = create_engine(connection_uri)

    # simple processing - getting YOB from DOB
    df = pd.read_sql(table_name, engine)
    df['YOB'] = df.apply(lambda row: str(row.DOB)[:4], axis=1)
    df = df.drop('DOB', axis=1)

    table_name = 'customers_clean'
    df.to_sql(name=table_name, con=engine, if_exists='replace')
    ti.xcom_push("table_name", table_name)

    # simple stats
    df_male = df[df.GENDER == 'M']
    male_age_mean = df_male.loc[:, 'AGE'].mean()
    male_age_median = df_male.loc[:, 'AGE'].median()

    logger.info('***** Male age mean %f', male_age_mean)
    logger.info('***** Male age median %f', male_age_median)

    df_female = df[(df.GENDER == 'F') & (df.CHILD == 'Y')]
    female_age_mean = df_female.loc[:, 'AGE'].mean()
    female_age_median = df_female.loc[:, 'AGE'].median()

    logger.info('***** Female with child age mean %f', female_age_mean)
    logger.info('***** Female with child age median %f', female_age_median)


def final(**kwargs):
    '''
    exposes the processed data as a new csv file
    '''
    ti = kwargs["ti"]
    table_name = ti.xcom_pull(task_ids="processing", key="table_name")
    logger.info('**** Exporting table: %s', table_name)

    connection_uri = Variable.get("DATA_WAREHOUSE_CONNECTION_URI")
    engine = create_engine(connection_uri)

    df = pd.read_sql(table_name, engine)
    csv_file_path = Variable.get("OUTPUT_CSV_FILE_NAME")
    df.to_csv(csv_file_path, index=False)


default_args = {
    'owner': 'Yonadav',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5)
}

with DAG(
    dag_id='sample_data_workflow',
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule='0 0 * * *',
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['test']
) as dag:
    csv_file_path = Variable.get("CSV_FILE_PATH")

    ingestion_task = PythonOperator(
        task_id="ingestion",
        python_callable=ingestion,
        op_kwargs={ 'file_path': csv_file_path }
    )

    processing_task = PythonOperator(
        task_id="processing",
        python_callable=processing,
    )

    final_task = PythonOperator(
        task_id="final",
        python_callable=final,
    )
    ingestion_task >> processing_task >> final_task
