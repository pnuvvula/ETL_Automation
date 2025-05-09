from Src.findNull import processNullValuesAcrossTables
from Src.findDuplicates import processDuplicateValuesAcrossTables
from Src.compareColumnAndDataytpe import processColumnandDataTypeAcrossTables
from Src.executeUserDefinedQuery import processUserDefinedQuery
from Src.singleQueryExecution import processSingleQueryExecution


def main():
    
    #processNullValuesAcrossTables()
    #processDuplicateValuesAcrossTables()
    #processUserDefinedQuery()
    #processSingleQueryExecution()

    print("Please select the option you want to execute:")
    print("1. Process Null Values Across Tables")
    print("2. Process Duplicate Values Across Tables")      
    print("3. Process Column and Data Type Across Tables")
    print("4. Process User Defined Query")
    print("5. Process Single Query Execution")      
    enterYourInput = input("Enter your choice:")  
    if(enterYourInput == '1'):
        processNullValuesAcrossTables()
    elif(enterYourInput == '2'):
        processDuplicateValuesAcrossTables()
    elif(enterYourInput == '3'):
        processColumnandDataTypeAcrossTables()
    elif(enterYourInput == '4'):
        processUserDefinedQuery()
    elif(enterYourInput == '5'):
        processSingleQueryExecution()
    else:
        print("Invalid choice. Please select a valid option.")
        return  
if __name__ == "__main__":
   main()