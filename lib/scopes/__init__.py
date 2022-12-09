from lib.scopes.get_scopes import get_scopes
from google.oauth2 import service_account
import pathlib
#env
from dotenv import load_dotenv
import os

#load env
load_dotenv()

#get root dir
root = pathlib.Path(__file__).parent.parent.parent.absolute()

#get full path
sa_path = os.getenv('SERVICE_ACCOUNT_PATH')
full_path = f'{root}/{sa_path}'

def scoped_credentials(data_source_list):
    """get scoped credentials based on data source list

    Args:
        data_source_list (list): list of data sources with options of 'drive', 'gcp', 'bigquery', 'sheets'

    Returns:
        list: list of scopes
    """
    
    service_account_credentials = service_account.Credentials.from_service_account_file(full_path)
    scoped_credentials = service_account_credentials.with_scopes(scopes=get_scopes(data_source_list))
    
    return scoped_credentials
