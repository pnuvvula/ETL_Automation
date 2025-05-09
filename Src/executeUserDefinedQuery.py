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

def processUserDefinedQuery():
    write_to_file('Start User defined query execution','Logger', 'Log.txt')
    current_time = datetime.now()
    formatted_time = current_time.strftime('%b%d%y%H%M%S') 
    formatted_date ="UserDefinedQueryExecution" + '-' + formatted_time
    passR=0
    failR=0
    qryNum, qryexe = returnQueryValues('User defined query execution')
    print("Do you want run all queries? Y/N")
    enterYourInput = input("Enter your choice:")
    if(enterYourInput == 'Y' or enterYourInput == 'y'):
            print("Executing all queries in the file")
            createReportHeaderForOptions('Reports', 'Bulk-'+formatted_date + '.csv','processUserDefinedQuery','User defined query execution')
            for i in range(len(qryNum)):
                  desc = qryNum[i] 
                  diffSourceDest=  qryexe[i].split(":")
                  sresult = execute_query_FetchAllVal(establishConnection("Source-Database"),diffSourceDest[0])
                  tresult = execute_query_FetchAllVal(establishConnection("Target-Database"),diffSourceDest[1])
                  createReportBodyForOptions('Reports', 'Bulk-'+formatted_date + '.csv','processUserDefinedQuery','User defined query execution',desc, '','', '')
                  if not sresult or not tresult:
                    createReportBodyForOptions('Reports', 'Bulk-'+formatted_date + '.csv','processUserDefinedQuery','User defined query execution','', "No data returned","No data returned", "Fail")
                    continue
                  sdataFrame = pd.DataFrame(sresult)
                  tdataFrame = pd.DataFrame(tresult)            
                  for row in sdataFrame.iterrows():
                    matchFound = False                    
                    sd=str(row[1].to_dict()).split(",")
                    skey = sd[0].split(":")[1].strip()
                    for rowt in tdataFrame.iterrows():
                        td=str(rowt[1].to_dict()).split(",")
                        tkey = td[0].split(":")[1].strip()
                        if skey == tkey:
                            matchFound = True
                            if row[1].equals(rowt[1]):
                                createReportBodyForOptions('Reports', 'Bulk-'+formatted_date + '.csv','processUserDefinedQuery','User defined query execution','', {str(row[1].to_dict()).replace(',',';')}, {str(rowt[1].to_dict()).replace(',',';')}, 'Pass')
                                count = "Pass"
                            else:
                                createReportBodyForOptions('Reports', 'Bulk-'+formatted_date + '.csv','processUserDefinedQuery','User defined query execution','', {str(row[1].to_dict()).replace(',',';')}, {str(rowt[1].to_dict()).replace(',',';')}, 'Fail')
                                count = "Fail"
                    if( not matchFound):
                        createReportBodyForOptions('Reports', 'Bulk-'+formatted_date + '.csv','processUserDefinedQuery','User defined query execution','',  {str(row[1].to_dict()).replace(',',';')} , {str(rowt[1].to_dict()).replace(',',';')}, "Fail")
                        count = "Fail"
                    if(count == "Pass"):
                        passR += 1
                    else:
                        failR += 1
            write_end_result('Reports', 'Bulk-'+formatted_date + '.csv','processUserDefinedQuery','User defined query execution',f"\n\nTotal test Cases Executed : {len(qryNum)}\nTotal Matching Record(s): {passR}\nTotal Mismatched Record(s): {failR}\n")
    if(enterYourInput == 'N' or enterYourInput == 'n'):
        print("Enter the query number to execute:")
        for i in range(len(qryNum)):
            print(f"{i+1} : {qryNum[i]}")
        enterYourInput = input("Enter your choice:")
        createReportHeaderForOptions('Reports', formatted_date + '.csv','processUserDefinedQuery','User defined query execution')
        index = int(enterYourInput) - 1
        desc = qryNum[index]
        diffSourceDest = str(qryexe[index]).split(':')
        sresult = execute_query_FetchAllVal(establishConnection("Source-Database"),diffSourceDest[0])
        tresult = execute_query_FetchAllVal(establishConnection("Target-Database"),diffSourceDest[1])
        createReportBodyForOptions('Reports', formatted_date + '.csv','processUserDefinedQuery','User defined query execution',desc, '','', '')
        if not sresult or not tresult:
            createReportBodyForOptions('Reports', formatted_date + '.csv','processUserDefinedQuery','User defined query execution','', "No data returned","No data returned", "Fail")
            return        
        sdataFrame = pd.DataFrame(sresult)
        tdataFrame = pd.DataFrame(tresult)            
        for row in sdataFrame.iterrows():
            matchFound = False                    
            sd=str(row[1].to_dict()).split(",")
            skey = sd[0].split(":")[1].strip()
            for rowt in tdataFrame.iterrows():
                td=str(rowt[1].to_dict()).split(",")
                tkey = td[0].split(":")[1].strip()
                if skey == tkey:
                    matchFound = True
                    if row[1].equals(rowt[1]):
                        createReportBodyForOptions('Reports', formatted_date + '.csv','processUserDefinedQuery','User defined query execution','', {str(row[1].to_dict()).replace(',',';')}, {str(rowt[1].to_dict()).replace(',',';')}, 'Pass')
                    else:
                        createReportBodyForOptions('Reports', formatted_date + '.csv','processUserDefinedQuery','User defined query execution','', {str(row[1].to_dict()).replace(',',';')}, {str(rowt[1].to_dict()).replace(',',';')}, 'Fail')
            if( not matchFound):
                createReportBodyForOptions('Reports', formatted_date + '.csv','processUserDefinedQuery','User defined query execution','', {str(row[1].to_dict()).replace(',',';')} ,  {str(rowt[1].to_dict()).replace(',',';')} , "Fail")
            
write_to_file('End User defined query execution','Logger', 'Log.txt')