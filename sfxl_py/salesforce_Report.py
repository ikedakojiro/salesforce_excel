# -*- coding: utf-8 -*-
from simple_salesforce import Salesforce
import salesforce_reporting
import pandas as pd
import sys
import io,sys
import io,sys
#from datetime import datetime as dt

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
# siteモジュールの読み込む．
import salesforce_param as D
import salesforce_SOQL as DD
Object_NAME = D.Object_NAME
SOQLs_main = DD.SOQLs

str_SOQL_filter = D.SOQL

Report_ID = D.Report_ID
print(type(D.Filter))
f_Col = 'f_Col'
f_Val = 'f_Val'
f_Val_Type = 'f_Val_Type'
drop_list = []
str_query = ''
#Filter_1_Val =D.Filter_1_Val
path_name = sys.path[0] + '/'
#print(f_Param)



def bool_str(x):
    return str(x)

def no_change(df, col):
    df[col+'_f'] = df[col]
    drop_list.append(col+'_f')#filter用のColなので、最後に削除
    return df

def str_to_month(df, col):
    val = 0
    if len(df) == 0:
        val = int(col)
    else:
        df[col+'_f'] = pd.to_datetime(df[col],format='%Y/%m/%d')
        df[col+'_f'] = df[col+'_f'].dt.month
        print(df[col+'_f'])
        drop_list.append(col+'_f')#filter用のColなので、最後に削除
    return df, val

def datestring(df, col):
    val = 0
    if  len(df) == 0:
        val = col.replace('/', '')
    else:
        #df[col+'_f'] = pd.to_datetime(df[col])
        df[col+'_f'] = df[col].str.replace('/', '')
        drop_list.append(col+'_f')#filter用のColなので、最後に削除
    return df, val


def operant_conv(Opt):
    if Opt == 'AND':
        Opt_Conv = '&'
    elif Opt == 'OR':
        Opt_Conv = '|'
    elif Opt == 'NOT':
        Opt_Conv = '~'
    else:
        Opt_Conv = Op
    return Opt_Conv

def operator_conv(Opr):
    if Opr == '=':
        Opr_Conv = '=='
    else:
        Opr_Conv = Opr
    return Opr_Conv

sf = salesforce_reporting.Connection(username='ikeda@zenfoods-grp.com', password='toro2003', security_token='5IoyjJNaki9CTSxDTAbi2S5E')
    #my_sf.get_report('00O2s000000SaeCEAS')
report = sf.get_report(Report_ID)
parser = salesforce_reporting.ReportParser(report)
report = parser.records_dict()
print(report)

keys = []
for key in report[0].keys():
    keys.append(key)
report = pd.DataFrame(report)
report = report.reindex(columns=keys)
report = report.applymap(lambda x: x.replace(',', ''))
report = report.applymap(lambda x: x.replace('￥', ''))
report = report.applymap(lambda x: x.replace('None', ''))
report = report.applymap(lambda x: x.replace('nan', ''))

report[report == '-'] = ''

if D.Filter != '':
    f_Op = operant_conv(D.Filter['Operant'])
    f_Param = D.Filter['param']

    #print(report)
    #フィルタ用に特別なデータ型があれば
    for i in range(len(f_Param)):
        #report = (report[report[Filter_1_Col].isin(['2020/04/16'])]) | (report[report[Filter_1_Col].isin(['2020/04/16'])])    #report = (report[report[Filter_1_Col].isin(['2020/04/16'])]) or (report[report['加工日'].isin(['2020/07/01'])] )
        if f_Param[i][f_Val_Type] == 'month':
            report = str_to_month(report, f_Param[i][f_Col])[0]#, format='%YYYY/%mm/%dd'))
        elif f_Param[i][f_Val_Type] == 'datestring':
            report = datestring(report, f_Param[i][f_Col])[0]#, format='%YYYY/%mm/%dd'))
        else:
            report = no_change(report, f_Param[i][f_Col])
            #report = report[pd.DatetimeIndex(report[f_Param[i][f_Col]]).month] == f_Param[i][f_Val]]
    #query文を作成
    for j in range(len(f_Param)):
        operator = operator_conv(f_Param[j][operator])
        if f_Param[j][f_Val_Type] == 'month':
            print(f_Param[j][f_Val])
            val = str_to_month('', f_Param[j][f_Val])[1]#, format='%YYYY/%mm/%dd'))
            print(val)
            if j == 0:
                str_query  = f_Param[j][f_Col] +'_f' + operator + str(val)
            else:
                str_query  = str_query + ' ' + f_Op + ' ' + f_Param[j][f_Col] +'_f' + operator + str(val)

        elif f_Param[j][f_Val_Type] == 'datestring':
            val = datestring('', f_Param[j][f_Val])[1]#, format='%YYYY/%mm/%dd'))
            if j == 0:
                str_query  = f_Param[j][f_Col] +'_f' + operator + '"' + str(val) + '"'
            else:
                str_query  = str_query + ' ' + f_Op + ' ' + f_Param[j][f_Col]  +'_f' + operator + '"' + str(val) + '"'
        else:
            val = f_Param[j][f_Val]
            if j == 0:
                str_query  = f_Param[j][f_Col] +'_f' + operator + str(val)
            else:
                str_query  = str_query + ' ' + f_Op + ' ' + f_Param[j][f_Col] +'_f' + operator + str(val)
    #if j == 0:
        #    str_query  = f_Param[j][f_Col] +'_f == ' + str(val)
        #else:

    print(str_query)
    report = report.query(str_query)
    report.drop(drop_list, axis='columns', inplace=True)

report.to_csv(path_name + 'SFdata.csv', index=False, mode='w')
