def get_scopes(data_source_list):
    
    scopes = []
    
    for data_source in data_source_list:
        if data_source == 'drive':
            scopes.append('https://www.googleapis.com/auth/drive')
        elif data_source == 'gcp':
            scopes.append('https://www.googleapis.com/auth/cloud-platform')
        elif data_source == 'bigquery':
            scopes.append('https://www.googleapis.com/auth/bigquery')
        elif data_source == 'sheets':
            scopes.append('https://www.googleapis.com/auth/spreadsheets')
        else:
            raise ValueError(f'"{data_source}" is not a valid data source.')
    
    return scopes