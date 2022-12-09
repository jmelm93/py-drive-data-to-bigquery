#api config
from lib.scopes import scoped_credentials
from google.cloud import bigquery
import google.api_core.exceptions as exceptions 
#utils
import logging 

logging.basicConfig(level=logging.INFO)


######## ----- BigQuery Service ------ ###########

def bq_service(credentials=scoped_credentials(['gcp'])):
    return bigquery.Client(credentials=credentials)

######## ----- QUERY ------ ###########

def bq_query_job(query_string, bigq=bq_service()):
    dataframe = (
        bigq.query(query_string)
        .result()
        .to_dataframe(
            create_bqstorage_client=True,
        )
    )
    return dataframe

def simple_query_all(table_id):
    query_string = f"""
        SELECT *
        FROM `{table_id}`
    """
    return bq_query_job(query_string)


######## ----- UPDATE ------ ###########

def write_to_bigquery_tables(data, bigq=bq_service()):
    table_id = data.get("table_id") 
    
    # check if table exists
    if not table_exists(table_id):
        # if not, create it
        create_table_with_table_id_and_schema(table_id, data.get("data").columns)

    df = data.get("data")

    job = bigq.load_table_from_dataframe(
        df, table_id, job_config=bq_job_config(dict_object=data)
    )  # Make an API request.
    
    job.result()  # Wait for the job to complete.
    
    table = bigq.get_table(table_id)  # Make an API request.
        
    load_info = (
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )
    
    logging.info(load_info)
    
    return load_info

def bq_job_config(dict_object):
    job_config = bigquery.LoadJobConfig(
        schema=dict_object.get("table_schema"),
        write_disposition=dict_object.get("write_disposition"),
        create_disposition='CREATE_IF_NEEDED', 
        allow_jagged_rows=True,
        allow_quoted_newlines=True,
        autodetect=False,
        source_format = "CSV", 
    )
    return job_config


######## ----- DELETE ------ ###########

def delete_bigquery_table(table_id, bigq=bq_service()):
    
    bigq.delete_table(
        table_id, 
        not_found_ok=True # if table does not exist, do not raise an error
    )
        
    return True

######## ----- CREATE ------ ###########

def create_table_with_table_id_and_schema(table_id, table_schema, bigq=bq_service()):
    # check if table_id exists in BigQuery. If not, create it.
    if not check_if_table_id_exists(table_id, bigq):
        schema = convert_list_of_column_names_to_schema_object(table_schema)
        table = bigquery.Table(table_id, schema=schema)
        table = bigq.create_table(table)
        return True
    
    else:
        logging.error(f"Table {table_id} already exists")


######## ----- UTILS ------ ###########

def load_schema_builder(
    data, 
    cols, 
    table_id,
    write_disposition="WRITE_TRUNCATE"
):
    write_disposition=write_disposition
    
    table_schema = []
    
    for col in cols: # iterate through the cols
        table_schema.append(
            bigquery.SchemaField(
                col,
                bigquery.enums.SqlTypeNames.STRING
            )
        )

    final_dict = { 
        "data": data,
        "write_disposition": write_disposition,
        "table_id": table_id,
        "table_schema": table_schema
    }

    return final_dict


def table_exists(table_id, bigq=bq_service()):
    try:
        bigq.get_table(table_id)
        return True
    except exceptions.NotFound:
        return False


def convert_list_of_column_names_to_schema_object(column_names):
    schema = []
    for column_name in column_names:
        schema.append({"name": column_name, "type": "STRING"})
    return schema


def check_if_table_id_exists(table_id, bigq=bq_service()):
    try:
        bigq.get_table(table_id)
        return True
    except Exception as e:
        return False
