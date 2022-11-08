from zisson_utils import filter_blank_columns
import pandas as pd
import requests
import xml.etree.ElementTree as ET
import pymssql
from sqlalchemy import create_engine
import os
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
import logging
logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S') # add time of creation of LogRecord

# create engine, see: https://learn.microsoft.com/en-us/azure/key-vault/secrets/quick-create-python?tabs=azure-cli
keyVaultName = 'pndk-digital-credentials'
KVUri = f"https://{keyVaultName}.vault.azure.net"
client = SecretClient(vault_url=KVUri, credential=DefaultAzureCredential())
connectionSecret = client.get_secret('AzureSQL-BjornHansen-Datawarehouse')
username = connectionSecret.properties.tags['username']
password = connectionSecret.value
server = connectionSecret.properties.tags['server']
# port = connectionSecret.tags['port']
database = connectionSecret.properties.tags['database']
dialect_driver = 'mssql+pymssql'
# driver = connectionSecret.tags['driver'].replace(' ', '+')
engine = create_engine(f'{dialect_driver}://{username}:{password}@{server}/{database}')

# api authentication
# keyVaultName = 'pndk-digital-credentials'
# KVUri = f"https://{keyVaultName}.vault.azure.net"
# client = SecretClient(vault_url=KVUri, credential=DefaultAzureCredential())
connectionSecret = client.get_secret('Zisson-api-key')
# authuser = connectionSecret.properties.tags['authuser']
# authpass = connectionSecret.properties.tags['authpass']
b64_auth_str = connectionSecret.value
api_url = "https://api.zisson.se/api/simple/XmlExport"
# get last callsessionid from sql table
q = '''
SELECT top 1 CallSessionId FROM [fact].[CallSession] order by cast(CallTimestamp as datetime) desc
'''
lastcallsessionid = pd.read_sql(q, con=engine)
lastcallsessionid = lastcallsessionid['CallSessionId'].iloc[0]
headers = {'Authorization': 'Basic %s' % b64_auth_str}
params = {'LastCallSessionId': lastcallsessionid}
response = requests.get(api_url, headers=headers, params=params)
logging.info(lastcallsessionid)
logging.info(response)

df = pd.DataFrame()
dict_list = []
children = ['CallChannels', 'CallChannel', 'ServiceNumbers', 'ServiceNumber', 'TimeModules', 'TimeModule', 'NumberListMatchModules', 'NumberListMatchModule']
# children = ['CallChannel', 'ServiceNumber', 'TimeModule', 'NumberListMatchModule']
myroot = ET.fromstring(response.content) # str(response.content)
for call_session in myroot.iter('CallSession'):
    d = {}
    for x in call_session:
        # if len(x.text.split('  ')) >= 1:
            # print(x.tag, x.text, type(x.text))
            # continue
        d[str(x.tag)] = x.text
    for child in children:
        for i in myroot.iter(child):
            for x in i:
                # if x.text is None: 
                #     continue
                # if len(x.text.split('  ')) >= 1:
                #     # print(x.tag, x.text, type(x.text))
                #     continue
                d[str(x.tag)] = x.text
    for subelem in call_session:
      if len(subelem.text.split('  ')) >= 1:
        for s in subelem:
          for subsubelem in s:
            # print(f'subsubelem.tag: {subsubelem.tag}, subsubelem.text: {subsubelem.text}')     
             d[subsubelem.tag] = str(subsubelem.text)     
    dict_list.append(d)  

df = pd.DataFrame(dict_list)
df = filter_blank_columns(df)
# df.head()   
df.to_sql('CallSession_Delta', con=engine, if_exists='append', schema='StageDW', index=False)     