## Quick Data Ingestion from Google Drive Folder

**All files in the Google Drive folder must have the same column names in order for the Bigquery upload to work
**The use case for this, for example, is a client dropping some recurring datasets in a drive folder, then this script getting + ingesting that data

### Create a .env File (ROOT DIRECTORY)
> Add `FOLDER_ID='{{YOUR FOLDER ID FROM DRIVE}}'`  
> Add `SERVICE_ACCOUNT_PATH='{{PATH TO YOUR SERVICE ACCOUNT}}'`  
> Add `BQ_TABLE_ID='{{YOUR BIGQUERY TABLE ID}}'`  

### Download the Requirements

### Share Google Drive Folder w/ Service Account Email

### Run Job
