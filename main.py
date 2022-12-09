import os
#env
from dotenv import load_dotenv
#data processing
import pandas as pd
#utils
from utils import (
    get_files,
    file_name,
    get_file,
    read_file,
    data_qa,
    upload_to_bq
)

#load env
load_dotenv()

#variables
folder = os.getenv('FOLDER_ID')
table = os.getenv('BQ_TABLE_ID') 

def job(folder_id):
    
    files = get_files(folder_id)
    file_name_list = list(map(file_name, files))
    file_data_list = list(map(get_file, files))
    df_list = list(map(read_file, file_data_list, file_name_list))
    final = data_qa(pd.concat(df_list))
    
    return upload_to_bq(table, final)

if __name__ == '__main__':
    output = job(folder)
