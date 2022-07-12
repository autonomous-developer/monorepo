'''ALL IMPORTS'''
import ast
import json
import pandas as pd
from pytrends.request import TrendReq
from datetime import date
import os

pytrends = TrendReq(hl='en-US', tz=360)

kw_list = ["Commercial use Suspected", 'Commercial use detected', 'TeamViewer block', 'TeamViewer Commercial Use', 'teamviewer kommerzielle','teamviewer kommerzielle nutzung', 'Teamviewer usocomercial detectado','uso commercial detectado', 'suspeita de uso commercial','utilisation commerciale détectée','usage commercial suspecté']

i=0
new_list=[]
while i<len(kw_list):
    new_list.append(kw_list[i:i+3])
    i+=3

df3 = pd.DataFrame()

for key_list in new_list:
    pytrends.build_payload(key_list, cat=0 , timeframe='now 1-d', geo='', gprop='')
    df = pytrends.interest_by_region(resolution='COUNTRY', inc_low_vol=True, inc_geo_code = True)
    df['date'] = str(date.today())
    df.reset_index(inplace=True)
    result_df = pd.melt(df,id_vars =['geoName', 'geoCode','date'],value_vars=key_list, var_name='Keyword')
    df3=df3.append(result_df)

df3['Keyword'].value_counts()
del df3['geoCode']

df3.rename(columns={'geoName': 'Country', 'date': 'Date', 'value': 'Value'}, inplace=True)
df3 = df3[['Date', 'Keyword', 'Country', 'Value']]

result = df3.to_dict(orient="records")

import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="creds.json"

from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of table to append to.
table_id = "integral-magnet-286317.anydesk.google_trends_daily"

errors = client.insert_rows_json(table_id,result)  # Make an API request.
if errors == []:
    print("New rows have been added.")
else:
    print("Encountered errors while inserting rows: {}".format(errors))
