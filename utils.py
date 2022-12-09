import logging 
import os
#env
from dotenv import load_dotenv
#data processing
from io import StringIO
import pandas as pd
#api config
from lib import (
    get_files_from_folder_id, 
    download_file,
    write_to_bigquery_tables,
    load_schema_builder
)

#load env
load_dotenv()

#set logging
logging.basicConfig(level=logging.INFO)

#variables
folder = os.getenv('FOLDER_ID')
table = os.getenv('BQ_TABLE_ID') 

def decode_csv(file):
    return StringIO(file.decode('utf-8'))


def get_files(folder_id):
    files = get_files_from_folder_id(folder_id)
    return files


def file_name(file):
    return file.get('name')


def get_file(file_id):
    file = download_file(file_id)
    return file


def read_file(file, file_name):
    if file_name.endswith('.csv'):
        return pd.read_csv(decode_csv(file))
    elif file_name.endswith('.xlsx'):
        return pd.read_excel(file, sheet_name=0, engine="openpyxl")


def data_qa(df):
    df = df.drop_duplicates()
    return df


def upload_to_bq(table_id, df):
    df_final, cols = clean_column_names(df)
    
    logging.info('cols: {}'.format(cols))
    logging.info('df_final.columns: {}'.format(df_final.columns))
    
    load_schema = load_schema_builder(
        data = df_final,
        cols = cols,
        table_id = table_id
    )

    write_to_bigquery_tables(load_schema)

    return "Job complete."


def clean_column_names(df):
    
    list_to_replace_with_nothing = ['[', ']', '(', ')', '{', '}', '.', '/', '-', '?', '!', ';', ':', '\'', '\"', '`', '~', '@', '#', '$', '%', '^', '&', '*', '+', '=', '|', '\\', '<', '>', ',']
    
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    
    for item in list_to_replace_with_nothing:
        # remove chars not allowed in bq column names
        df.columns = df.columns.str.replace(item, '', regex=False)
    
    return df, df.columns.tolist()
