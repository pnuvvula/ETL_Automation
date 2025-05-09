from configparser import ConfigParser
import os

import psycopg2
from Util.fileHandling import write_to_file


def establishConnection(database):
        
        write_to_file('Start Execution of establishConnection method in dataBaseConnection class.','Logger', 'Log.txt')
        config = ConfigParser()
        script_dir = os.path.dirname(__file__)
        config_path = os.path.join(script_dir, '..', 'Config', 'Config.ini')
        if not os.path.exists(config_path):
            write_to_file('Configuration file not found.','Exception', 'ExceptionList')
            raise FileNotFoundError(f"Configuration file not found: {config_path}")            
        config.read(config_path)
        dbname = config[database]['dbname'].strip("'\"")
        user = config[database]['user'].strip("'\"")
        password = config[database]['password'].strip("'\"")
        host = config[database]['host'].strip("'\"")
        port = config.getint(database, 'port')
        
        db_params = {
        'dbname': dbname,
        'user': user,
        'password': password,
        'host': host,
        'port': port
        }
        try:
            connection = psycopg2.connect(**db_params)            
            write_to_file('Connection established successfully.','Logger', 'Log.txt')
            write_to_file('End Execution of establishConnection method in dataBaseConnection class.','Logger', 'Log.txt')
            return connection
        except Exception as e:
            write_to_file(str(e),'Exception', 'ExceptionList')
            write_to_file("Exception occured in establishConnection method. See ExceptionList for details.",'Logger', 'Log.txt')            
            return None