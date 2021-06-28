from pandas import read_excel 
import datetime
import os
import time
import sys
import re

import config
import MySQLdb

MAX_FLASH = 100


def get_database_connection():
    """connects to the MySQL database and returns the connection"""
    return MySQLdb.connect(host=config.MYSQL_HOST,
                           user=config.MYSQL_USERNAME,
                           passwd=config.MYSQL_PASSWORD,
                           db=config.MYSQL_DB_NAME,
                           charset='utf8')


def import_to_database_from_excel(filepath):
    '''Get a excel file name and imports data from it.'''

    db = get_database_connection()
    cur = db.cursor()

    total_flashes = 0
    output = []

    try:
        cur.execute('DROP TABLE IF EXISTS logs;')
        cur.execute("""CREATE TABLE logs (            
            log_name CHAR(200),
            log_value MEDIUMTEXT);
            """)
        db.commit()
    except Exception as e:
        print("dropping logs")
        output.append(
            f'problem dropping and creating new table for logs in database; {e}')

    # remove the serials table if exists, then create the new one
    try:
        cur.execute('DROP TABLE IF EXISTS excel;')
        cur.execute("""CREATE TABLE excel (
            id INTEGER PRIMARY KEY,
            ref VARCHAR(200),
            description VARCHAR(200),
            first CHAR(30),
            last CHAR(30),
            date DATETIME);""")
        db.commit()
    except Exception as e:
        print("problem dropping excel")
        output.append(
            f'problem dropping and creating new table excel in database; {e}')
    
    cur.execute("INSERT INTO logs VALUES ('db_filename', %s)", (filepath, ))
    db.commit()

    df = read_excel(filepath)
    excel_counter = 1
    line_number = 1
    for _, (line, ref, description, FirstName, LastName, date) in df.iterrows():
        line_number += 1
        if not ref or (ref != ref):
            ref = ""
        if not description or (description != description):
            description = ""
        if not date or (date != date):
            date = "7/2/12"
        try:
            cur.execute("INSERT INTO excel VALUES (%s, %s, %s, %s, %s, %s);", (
                line, ref, description, FirstName, LastName, date)
            )
            excel_counter += 1
        except Exception as e:
            total_flashes += 1
            if total_flashes < MAX_FLASH:                
                output.append(
                    f'Error inserting line {line_number} from excel sheet , {e}')
            elif total_flashes == MAX_FLASH:
                output.append(f'Too many errors!')
        if line_number % 1000 == 0:
            try:
                db.commit()
            except Exception as e:
                output.append(
                    f'Problem commiting serials into db at around record {line_number} (or previous 1000 ones); {e}')
    db.commit()

     # save the logs
    output.append(f'Inserted {excel_counter} values')
    output.reverse()
    cur.execute("UPDATE logs SET log_value = %s WHERE log_name = 'import'", ('\n'.join(output), ))
    db.commit()

    db.close()

    return



import_to_database_from_excel('test.xls')
