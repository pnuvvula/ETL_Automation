from Util.fileHandling import write_to_file


def execute_query_FetchAllVal(connection, query):
    write_to_file('Start execute query to fetch all results','Logger', 'Log.txt')
    cursor = connection.cursor()
    cursor.execute(query)
    data = cursor.fetchall() 
    cursor.close()
    write_to_file('End execute query to fetch all results','Logger', 'Log.txt')
    return data