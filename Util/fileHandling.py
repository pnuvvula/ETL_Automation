import os
from datetime import datetime

def setpath(folder_path, file_name):
    output_folder = os.path.join(os.getcwd(), folder_path)
    file_path = os.path.join(output_folder, file_name)
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    return file_path

def write_to_file(content,folder_path, file_name):
        file_path = setpath(folder_path, file_name)
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        with open(file_path, 'a') as file:
            file.write(now + '\t' + content + '\n')

def readDataFromFile(folder_path, file_name,message):
        file_path = setpath(folder_path, file_name)
        write_to_file('Start readDataFromFile function in ReadFile class','Logger', 'Log.txt')
        try:
            with open(file_path, 'r') as file:
                content = file.read()
            write_to_file('End readDataFromFile function in ReadFile class','Logger', 'Log.txt')
            write_to_file('End openAndRead function '+message, 'Logger', 'Log.txt')         
            return content
        except FileNotFoundError:
            print(f"File not found: {file_path}")
            write_to_file('File Not Found Exception occured','Logger', 'Log.txt')
            write_to_file('File not found','Exception', 'ExceptionList')
        except Exception as e:
            print(f"An error occurred: {e}")
            write_to_file('General Exception occured','Logger', 'Log.txt')
            write_to_file(str(e),'Exception', 'ExceptionList')

def createReportHeader(folder_path, file_name,funcName,message):     
     write_to_file('Start report header generation for '+message,'Logger', 'Log.txt')
     file_path = setpath(folder_path, file_name)
     if(funcName == 'processNullValuesAcrossTables' or funcName == 'processDuplicateValuesAcrossTables' or funcName == 'matchingColumnandDataTypes'):
        if not os.path.exists(file_path):
            with open(file_path, 'w') as file:
                file.write("Description of the Test Case" + ',' + "Execution Summary" + ',' + "Status" + '\n')                             
     write_to_file('End report header generation for '+message,'Logger', 'Log.txt')

def createReportBody(folder_path, file_name,funcName,message,query,exeStatus, status):
     write_to_file('Start report body generation for '+message,'Logger', 'Log.txt')
     file_path = setpath(folder_path, file_name)
     if(funcName == 'processNullValuesAcrossTables' or funcName == 'processDuplicateValuesAcrossTables' or funcName == 'matchingColumnandDataTypes'):
        with open(file_path, 'a') as file:            
                file.write(query + ',' + exeStatus + ',' + status + '\n')            
     write_to_file('End report body generation for '+message,'Logger', 'Log.txt')

def createReportHeaderForOptions(folder_path, file_name,funcName,message):
     write_to_file('Start report header generation for '+message,'Logger', 'Log.txt')
     file_path = setpath(folder_path, file_name)
     if(funcName == 'processSingleQueryExecution' or funcName == 'processUserDefinedQuery'):
           if not os.path.exists(file_path):
            with open(file_path, 'w') as file:
                file.write("Description of the Test Case" + ',' + "Source Value" +',' + "Target Value" + ',' + "Status" + '\n')
     write_to_file('End report header generation for '+message,'Logger', 'Log.txt')

def createReportBodyForOptions(folder_path, file_name,funcName,message,query,source,target, status):
     write_to_file('Start report body generation for '+message,'Logger', 'Log.txt')
     file_path = setpath(folder_path, file_name)
     if(funcName == 'processSingleQueryExecution' or funcName == 'processUserDefinedQuery'):
           with open(file_path, 'a') as file:            
                file.write(query + ',' + str(source) + ',' + str(target) + ',' + status + '\n')
     write_to_file('End report body generation for '+message,'Logger', 'Log.txt')

def write_end_result(folder_path, file_name,funcName,message,desc):
        write_to_file('Start end result generation for '+message,'Logger', 'Log.txt')
        file_path = setpath(folder_path, file_name)
        if(funcName == 'processSingleQueryExecution'or funcName == 'processUserDefinedQuery'):
            with open(file_path, 'a') as file:            
                file.write(desc + '\n')
        write_to_file('End end result generation for '+message,'Logger', 'Log.txt')  