# -*- coding: utf-8 -*-
from simple_salesforce import Salesforce
import salesforce_reporting
import pandas as pd
import sys
import io,sys
import io,sys
'''
This file is to download data from a salesforce object.
SOQL is given both by salesforce_SOQL.py for selecting columns and by salesforce_param.py for apllying WHERE (though only one operant applicable)
'''

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
import salesforce_param as D
import salesforce_SOQL as DD
Object_NAME = D.Object_NAME
SOQLs_main = DD.SOQLs
str_SOQL_filter = D.SOQL
Report_ID = D.Report_ID
path_name = sys.path[0] + '/'

def df_str(x):
    return str(x)
sf = Salesforce(username='ikeda@zenfoods-grp.com', password='toro2003', security_token='5IoyjJNaki9CTSxDTAbi2S5E')

#sf = Salesforce(username='', password='', security_token='')
str_SOQL_main = SOQLs_main[Object_NAME]
sf_data = sf.query_all(str_SOQL_main + str_SOQL_filter)
object = pd.DataFrame(sf_data['records']).drop(columns='attributes')

df_columns = object.columns
print(df_columns[0])
for d_col in range(len(object.columns)):
    object[df_columns[d_col]] = object[df_columns[d_col]].map(df_str)
    print(object[df_columns[d_col]])
object = object.applymap(lambda x: x.replace(',', ''))
object = object.applymap(lambda x: x.replace('￥', ''))
object = object.applymap(lambda x: x.replace('None', ''))
object = object.applymap(lambda x: x.replace('nan', ''))

object[object == '-'] = ''
object.to_csv(path_name + 'SFdata.csv', index=False, mode='w')
