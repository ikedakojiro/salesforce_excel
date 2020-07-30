# -*- coding: utf-8 -*-
Object_NAME = ''
SOQL = ""
Report_ID = ''
Filter = ''

'''
examples
SOQL = " WHERE CALENDAR_MONTH(CreatedDate) = 10 AND CALENDAR_YEAR(CreatedDate) = 2020"
Filter = {'Operant' : 'AND', 'param' : [{'f_Col' : 'CreateDate', 'f_Val' : 10, 'operator' : '=', 'f_Val_Type' : 'month'},{'f_Col' : 'CreateDate', 'f_Val' : 2020/01/01, 'operator' : '>', 'f_Val_Type' : 'datestring'}]}
'''