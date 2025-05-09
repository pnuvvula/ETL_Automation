from Util.fileHandling import write_to_file, readDataFromFile

def openAndRead(message):
    if(message == 'Checking for Null Values'):
         write_to_file('Start openAndRead function '+message, 'Logger', 'Log.txt')
         return readDataFromFile('QueriesForNull','queries.txt',message)
    if(message == 'Checking for Duplicate Values'):
         write_to_file('Start openAndRead function '+message, 'Logger', 'Log.txt')
         return readDataFromFile('QueriesForDuplicate','queries.txt',message)
    if(message == 'Checking for Columns and Data Types'):
         write_to_file('Start openAndRead function '+message, 'Logger', 'Log.txt')
         return readDataFromFile('QueriesForColumnValues','queries.txt',message)
    if(message == 'Single Query Execution'):
         write_to_file('Start openAndRead function '+message, 'Logger', 'Log.txt')
         return readDataFromFile('QuerySpanning2db','queries.txt',message)
    if(message == 'User defined query execution'):
         write_to_file('Start openAndRead function '+message, 'Logger', 'Log.txt')
         return readDataFromFile('QuerybyUser','queries.txt',message)

def parseData(message):
    write_to_file('Start parseData function '+message, 'Logger', 'Log.txt')      
    splitquery=[]
    data = openAndRead(message)
    datasplit=[]
    datasplit = data.split('#')
    for line in datasplit:                
            if(line != ''):                   
                splitquery.append(line.replace('/n','').strip())
    write_to_file('End parseData function '+message, 'Logger', 'Log.txt')       
    return splitquery

def returnQueryValues(message):
    write_to_file('Start returnQueryValues function '+message, 'Logger', 'Log.txt')
    qryNum = []
    qryexe =[]
    data = parseData(message) 
    for i in range(len(data)):
        qryNum.append(data[i].split("^")[0])
        qryexe.append(data[i].split("^")[1])
    write_to_file('End returnQueryValues function '+message, 'Logger', 'Log.txt')
    return qryNum, qryexe