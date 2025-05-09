import os
import sys
from datetime import datetime
import pandas as pd

from Util.databaseConnection import establishConnection
from Util.fileProcessor import returnQueryValues
from Util.queryExecution import execute_query_FetchAllVal

script_dir = os.path.dirname(__file__)
parent_dir = os.path.abspath(os.path.join(script_dir, '..'))
sys.path.append(parent_dir)

from Util.fileHandling import createReportBody, createReportHeader, write_to_file

def processNullValuesAcrossTables():
    write_to_file('Start matching for null values','Logger', 'Log.txt')
    current_time = datetime.now()
    formatted_time = current_time.strftime('%b%d%y%H%M%S') 
    formatted_date ="NullValueChecking" + '-' + formatted_time
    qryNum, qryexe = returnQueryValues('Checking for Null Values')
    createReportHeader('Reports', formatted_date + '.csv','processNullValuesAcrossTables','Checking for Null Values')
    for i in range(len(qryNum)):
            diffSourceDest=  qryexe[i].split(":")
            sresult = execute_query_FetchAllVal(establishConnection("Source-Database"),diffSourceDest[0])
            tresult = execute_query_FetchAllVal(establishConnection("Target-Database"),diffSourceDest[1])
            createReportBody('Reports', formatted_date + '.csv','processNullValuesAcrossTables','Checking for Null Values',str(qryNum[i]).replace(',',' '), "", "")
            if not sresult or not tresult:
                  createReportBody('Reports', formatted_date + '.csv','processNullValuesAcrossTables','Checking for Null Values','', "No data returned", "Fail")
                  continue
            sdataFrame = pd.DataFrame(sresult)
            tdataFrame = pd.DataFrame(tresult)            
            for row in sdataFrame.iterrows():
                matchFound = False                    
                sd=str(row[1].to_dict()).split(",")
                skey = sd[0].split(":")[1].strip()
                for rowt in tdataFrame.iterrows():
                    count = 0
                    td=str(rowt[1].to_dict()).split(",")
                    tkey = td[0].split(":")[1].strip()
                    if skey == tkey:
                        matchFound = True                        
                        if row[1].equals(rowt[1]):
                            #createReportBody('Reports', formatted_date + '.csv','processNullValuesAcrossTables','Checking for Null Values','', f"Row {str(row[1].to_dict()).replace(',',';')} in Source matches Row {str(rowt[1].to_dict()).replace(',',';')} in Target", "Pass")
                            count= count + 1
                        else:
                            createReportBody('Reports', formatted_date + '.csv','processNullValuesAcrossTables','Checking for Null Values','', f"Row {str(row[1].to_dict()).replace(',',';')} in Source does not match Row {str(rowt[1].to_dict()).replace(',',';')} in Target", "Fail")
                if( not matchFound):
                     createReportBody('Reports', formatted_date + '.csv','processNullValuesAcrossTables','Checking for Null Values','', f"No match found for Row {row[0]} in Source", "Fail")
    write_to_file('End matching for null values','Logger', 'Log.txt')                              
