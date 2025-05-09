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

from Util.fileHandling import createReportBody, createReportHeader, write_to_file

def processColumnandDataTypeAcrossTables():
    write_to_file('Start matching of Columns and Data Types','Logger', 'Log.txt')
    current_time = datetime.now()
    formatted_time = current_time.strftime('%b%d%y%H%M%S') 
    formatted_date ="ColumnValMatching" + '-' + formatted_time
    qryNum, qryexe = returnQueryValues('Checking for Columns and Data Types')
    createReportHeader('Reports', formatted_date + '.csv','matchingColumnandDataTypes','Checking for Columns and Data Types')
    for i in range(len(qryNum)):
        diffSourceDest=  qryexe[i].split(":")
        splitdiffSource = diffSourceDest[0].split("&")
        splitdiffDest = diffSourceDest[1].split("&")
        scolnames = np.array(splitdiffSource[1].split(","))
        tcolnames = np.array(splitdiffDest[1].split(","))
        createReportBody('Reports', formatted_date + '.csv','matchingColumnandDataTypes','Checking for Columns and Data Types',str(qryNum[i]).replace(',',' '), "", "")
        if(len(scolnames) == len(tcolnames)):       
                query = "SELECT column_name,data_type FROM information_schema.columns WHERE table_name = '"
                sresult = execute_query_FetchAllVal(establishConnection("Source-Database"),query + splitdiffSource[0] + "'" +"and column_name in (" + splitdiffSource[1] + ")")
                tresult = execute_query_FetchAllVal(establishConnection("Target-Database"),query + splitdiffDest[0] + "'" +"and column_name in (" + splitdiffDest[1] + ")")
                
                sdataFrame = pd.DataFrame(sresult)
                tdataFrame = pd.DataFrame(tresult)
                
                if(len(sresult)==len(scolnames)):
                    if(len(tcolnames)==len(tresult)):
                        if(len(sresult)==len(tresult)):
                            output=False
                            for i in range(len(sdataFrame)):
                                if str(sdataFrame.iloc[i,1]).replace('"','') != str(tdataFrame.iloc[i,1]).replace('"',''):
                                    output=True
                                    createReportBody('Reports', formatted_date + '.csv','matchingColumnandDataTypes','Checking for Columns and Data Types','',f"The Data Type of source column mane: {str(sdataFrame.iloc[i,0])} is {str(sdataFrame.iloc[i,1]).replace('"','')} and is not matching with the datatype {str(tdataFrame.iloc[i,1]).replace('"','')} in target table", "Fail")
                            if output==False:
                                createReportBody('Reports', formatted_date + '.csv','matchingColumnandDataTypes','Checking for Columns and Data Types','',f"The Column values and Data Types matches when compared to Source and Target", "Pass")
                        else:
                            createReportBody('Reports', formatted_date + '.csv','matchingColumnandDataTypes','Checking for Columns and Data Types','', f"The count of column names returned from the database are not matching", "Fail")
                    else:
                        createReportBody('Reports', formatted_date + '.csv','matchingColumnandDataTypes','Checking for Columns and Data Types','', f"The count of column names returned from the database are not matching with the input file for Target database", "Fail")
                else:
                    createReportBody('Reports', formatted_date + '.csv','matchingColumnandDataTypes','Checking for Columns and Data Types','', f"The count of column names returned from the database are not matching with the input file for Source database", "Fail")
        else:
            createReportBody('Reports', formatted_date + '.csv','matchingColumnandDataTypes','Checking for Columns and Data Types','', f"The count of column names to be verified in the test case are not matching", "Fail")
    write_to_file('End matching of Columns and Data Types','Logger', 'Log.txt')