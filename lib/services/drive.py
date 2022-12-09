#api config
from lib.scopes import scoped_credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
#utils
import logging 
import io 

logging.basicConfig(level=logging.INFO)


######## ----- Google Drive Service ------ ###########

def drive_service(credentials=scoped_credentials(['drive'])):
    return build('drive', 'v3', credentials=credentials)


def get_files_from_folder_id(folder_id, service = drive_service()):
    """Call the Drive v3 API"""
    results = service.files().list(
        q=f"'{folder_id}' in parents",
        pageSize=1000, 
        fields="nextPageToken, files(id, name, mimeType, parents, modifiedTime, createdTime)"
    ).execute()
    items = results.get('files', [])
    logging.info(f'Found {len(items)} files in folder {folder_id}.')
    return items


def download_file(fileObj, service = drive_service()):
    """Downloads a file
    Args:
        file_id: ID of the file to download
    Returns : IO object with location.
    """
    try:
        request = service.files().get_media(fileId=fileObj.get('id'))
        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            file_name = fileObj.get('name')
            logging.info(F'Drive Download Progress: {int(status.progress() * 100)}%. File Name: {file_name}')

    except HttpError as error:
        logging.info(F'An error occurred: {error}')
        file = None

    return file.getvalue()
