from airflow.decorators import dag, task  # Importing required decorators and classes
from datetime import datetime  # Importing datetime module
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator  # Importing operator for uploading file to Google Cloud Storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator  # Importing operator for creating BigQuery dataset
from astro import sql as aql  # Importing SQL module
from astro.files import File  # Importing File class
from airflow.models.baseoperator import chain  # Importing chain function for task dependencies
from astro.sql.table import Table, Metadata  # Importing Table and Metadata classes for defining SQL table
from astro.constants import FileType  # Importing FileType for defining file type
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG  # Importing DBT configuration
from cosmos.airflow.task_group import DbtTaskGroup  # Importing DBT task group
from cosmos.constants import LoadMode  # Importing LoadMode for defining DBT load method
from cosmos.config import ProjectConfig, RenderConfig  # Importing ProjectConfig and RenderConfig for DBT configuration

# Defining the Airflow DAG for retail data pipeline
@dag(
    start_date=datetime(2024, 1, 1),  # Setting start date for the DAG
    schedule=None,  # Disabling scheduling for the DAG
    catchup=False,  # Disabling catchup for the DAG
    tags=['retail'],  # Adding tags to the DAG
)
def retail():
    # Task to upload CSV file to Google Cloud Storage
    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',  # Task ID
        src='/usr/local/airflow/include/dataset/Online_Retail.csv',  # Source file path
        dst='raw/online_retail.csv',  # Destination file path in GCS
        bucket='larrysaavedra_online_retail',  # GCS bucket name
        gcp_conn_id='gcp',  # Google Cloud connection ID
        mime_type='text/csv',  # MIME type of the file
    )

    # Task to create BigQuery dataset
    create_retail_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_retail_dataset',  # Task ID
        dataset_id='retail',  # Dataset ID
        gcp_conn_id='gcp',  # Google Cloud connection ID
    )

    # Task to load CSV file from GCS to BigQuery table
    gcs_to_raw = aql.load_file(
        task_id='gcs_to_raw',  # Task ID
        input_file=File(
            'gs://ls/raw/online_retail.csv',  # GCS file path
            conn_id='gcp',  # Google Cloud connection ID
            filetype=FileType.CSV,  # File type
        ),
        output_table=Table(
            name='raw_invoices',  # Table name
            conn_id='gcp',  # Google Cloud connection ID
            metadata=Metadata(schema='retail')  # Table metadata
        ),
        use_native_support=False,  # Disabling native support
    )

    # Task to run data quality checks after loading
    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_load(scan_name='check_load', checks_subpath='sources'):
        from include.soda.checks.sources.check_function import check
        return check(scan_name, checks_subpath)
    
    # Task group for data transformation using DBT
    transform = DbtTaskGroup(
        group_id='transform',  # Task group ID
        project_config=DBT_PROJECT_CONFIG,  # DBT project configuration
        profile_config=DBT_CONFIG,  # DBT profile configuration
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,  # DBT load method
            select=['path:models/transform']  # Path to DBT models for transformation
        )
    )

    # Task to run data quality checks after transformation
    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_transform(scan_name='check_transform', checks_subpath='transform'):
        from include.soda.checks.sources.check_function import check
        return check(scan_name, checks_subpath)
    
    # Task group for generating report using DBT
    report = DbtTaskGroup(
        group_id='report',  # Task group ID
        project_config=DBT_PROJECT_CONFIG,  # DBT project configuration
        profile_config=DBT_CONFIG,  # DBT profile configuration
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,  # DBT load method
            select=['path:models/report']  # Path to DBT models for report generation
        )
    )

    # Task to run data quality checks on generated report
    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_report(scan_name='check_report', checks_subpath='report'):
        from include.soda.checks.sources.check_function import check
        return check(scan_name, checks_subpath)
    
    # Defining task dependencies using chain function
    chain(
        upload_csv_to_gcs,
        create_retail_dataset,
        gcs_to_raw,
        check_load(),
        transform,
        check_transform(),
        report,
        check_report()
    )

# Calling the retail DAG function
retail()
