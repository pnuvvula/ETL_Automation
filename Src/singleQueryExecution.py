import os
import sys
from datetime import datetime
import pandas as pd
import numpy as np

from Util.databaseConnection import establishConnection
from Util.fileProcessor import returnQueryValues
from Util.queryExecution import execute_query_FetchAllVal

script_dir = os.path.dirname(__file__)
parent_dir = os.path.abspath(os.path.join(script_dir, '..'))
sys.path.append(parent_dir)

from Util.fileHandling import createReportBody, createReportBodyForOptions, createReportHeader, createReportHeaderForOptions, write_end_result, write_to_file

def processSingleQueryExecution():
    write_to_file('Start single query execution','Logger', 'Log.txt')
    current_time = datetime.now()
    formatted_time = current_time.strftime('%b%d%y%H%M%S') 
    formatted_date ="SingleQueryExecution" + '-' + formatted_time
    passR=0
    failR=0
    qryNum, qryexe = returnQueryValues('Single Query Execution')    

    print("Do you want run all queries? Y/N")
    enterYourInput = input("Enter your choice:")
    if(enterYourInput == 'Y' or enterYourInput == 'y'):
            print("Executing all queries in the file")
            createReportHeaderForOptions('Reports', 'Bulk-'+formatted_date + '.csv','processSingleQueryExecution','Single Query Execution')
            for i in range(len(qryNum)):                          
                query = qryexe[i].split(':')
                desc = qryNum[i]  
                sresult = execute_query_FetchAllVal(establishConnection("Source-Database"),query[0])   
                sdata = str(sresult[0]).replace('(','').replace(')','').replace(',','')
                if(str(sdata)==str(query[1])):
                 count = "Pass"
                else:
                 count = "Fail"
                createReportBodyForOptions('Reports', 'Bulk-'+formatted_date + '.csv','processSingleQueryExecution','Single Query Execution',desc,str(sdata), str(query[1]),count)
                if(count == "Pass"):
                    passR += 1
                else:
                    failR += 1
            write_end_result('Reports', 'Bulk-'+formatted_date + '.csv','processSingleQueryExecution','Single Query Execution',f"\n\nTotal test Cases Executed : {len(qryNum)}\nTotal Matching Record(s): {passR}\nTotal Mismatching Record(s): {failR}\n")
    if(enterYourInput == 'N' or enterYourInput == 'n'):
             print("Enter the query number to execute:")
             for i in range(len(qryNum)):
                print(f"{i+1} : {qryNum[i]}")
             enterYourInput = input("Enter your choice:")
             createReportHeaderForOptions('Reports', formatted_date + '.csv','processSingleQueryExecution','Single Query Execution')
             index = int(enterYourInput) - 1
             desc = qryNum[index]
             query = str(qryexe[index]).split(':')
             sresult = execute_query_FetchAllVal(establishConnection("Source-Database"),query[0])
             sdata = str(sresult[0]).replace('(','').replace(')','').replace(',','')
             if(str(sdata)==str(query[1])):
                 count = "Pass"
             else:
                 count = "Fail"
             createReportBodyForOptions('Reports', 'Bulk-'+formatted_date + '.csv','processSingleQueryExecution','Single Query Execution',desc,str(sdata), str(query[1]),count)
    write_to_file('End single query execution','Logger', 'Log.txt')