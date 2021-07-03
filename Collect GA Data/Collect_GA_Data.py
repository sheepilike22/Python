# -*- coding: utf-8 -*-
"""
Created on Fri Dec  4 10:28:31 2020

@author: angela
"""

# -*- coding: utf-8 -*-

import pandas as pd
import warnings,datetime
warnings.simplefilter("ignore")
import argparse,httplib2,glob,os,logging
from apiclient.discovery import build
from oauth2client import client,file,tools

write_path = r'C:\Users\angela\<path>'
file_path = glob.glob(f'{write_path}/<file name>.csv')
logging.basicConfig(level=logging.INFO, format=' %(asctime)s %(message)s')

print('Find data')
if glob.glob(f'{write_path}/<file name>.csv'):
    web_data = pd.read_csv(file_path[0])
    start =  f'{str(web_data.date.max())[:4]}-{str(web_data.date.max())[4:6]}-{str(web_data.date.max())[6:8]}'
    os.remove(file_path[0])
else:
    web_data = pd.DataFrame({'A' : []})
    start = 'yyyy-MM-dd'

# ga
def get_service(api_name, api_version, scope, client_secrets_path):
    # Parse command-line arguments.
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        parents=[tools.argparser])
    flags = parser.parse_args([])
    # Set up a Flow object to be used if we need to authenticate.
    flow = client.flow_from_clientsecrets(
        client_secrets_path, scope=scope,
        message=tools.message_if_missing(client_secrets_path))
    
    storage = file.Storage(api_name + '.dat')
    credentials = storage.get()
    print(credentials)
    if credentials is None or credentials.invalid:
        credentials = tools.run_flow(flow, storage, flags)
    http = credentials.authorize(http=httplib2.Http())
    # Build the service object.
    service = build(api_name, api_version, http=http)
    return service

# =============================================================================
# Reference: https://ga-dev-tools.appspot.com/query-explorer/
# only 7 dimensions are allowed.    
# =============================================================================
now = datetime.datetime.now()
end = f'{now.year}-{now.month:02d}-{now.day:02d}'
print('Merge data')
while start!=end:
    start_format = datetime.datetime.strptime(start,"%Y-%m-%d")
    start = f'{start_format.year}-{start_format.month:02d}-{start_format.day:02d}'
    scope = ['https://www.googleapis.com/auth/analytics.readonly']
    service = get_service('analytics', 'v3', scope, write_path +'/client_secrets.json')
    web_query = service.data().ga().get(
               samplingLevel="HIGHER_PRECISION",
               ids= 'ga:' + '<id>',    
               start_date= start,
               end_date= start,
               max_results=100000,
               dimensions = "ga:<dimension>",
               metrics = "ga:<metric>").execute()
    try:
        colnames = web_query['query']['dimensions'].split(',') + web_query['query']['metrics']
        tmp_data = pd.DataFrame(web_query['rows'],columns = [x.split(':')[1] for x in colnames])
        web_data = pd.concat([web_data,tmp_data],axis=0)
        print(start,tmp_data.shape[0])
    except :print(f'missing web data -- day: {start}')
    
    start_format = datetime.datetime.strptime(start,"%Y-%m-%d") + datetime.timedelta(days=1)
    start = f'{start_format.year}-{start_format.month:02d}-{start_format.day:02d}'


print('Write data')
web_data.to_csv(f'{write_path}/<file name>.csv',encoding='utf_8_sig',index=False)
