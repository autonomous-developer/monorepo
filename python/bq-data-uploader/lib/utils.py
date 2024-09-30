from google.cloud import bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "creds.json"
client = bigquery.Client()

def is_bq_table(project_id, table_id):
    """
    Confirms existence of a BigQuery table returning true or false.
    Args:
        project_id: Bigquery project ID
        table_id: BigQuery table to check for existence
    """
    dml_statement = f"SELECT 1 FROM `{project_id}.{table_id}`"
    try:
        query_job = client.query(dml_statement)  # API request
        query_job.result()  # Waits for statement to finish
    except:
        return bool(0)
    return bool(1)


def del_bq_table_data(project_id, table_id, start_date):
    """
    Function to delete data from BQ table based on a start_date
    Args:
        project_id: Bigquery project ID
        table_id: BigQuery table to delete data from
        start_date: date from which to start deleting data
    """

    dml_statement = f"DELETE FROM `{project_id}.{table_id}` WHERE report_date >= '{start_date}'"
    query_job = client.query(dml_statement)  # API request
    query_job.result()  # Waits for statement to finish


def write_to_bq(project_id, table_id, df):
    """
    Function to write data to BQ tables
    Args:
        project_id: Bigquery project ID
        table_id: BigQuery table to write data to
        df: Dataframe to write
    """
    job_config = bigquery.LoadJobConfig(
        time_partitioning=bigquery.table.TimePartitioning(type_="DAY", field="report_date")
    )

    table_id = f"{project_id}.{table_id}"
    write_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    write_job.result()
