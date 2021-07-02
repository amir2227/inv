from pandas import read_excel
import numpy as np
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

    # remove the E_SCORE table if exists, then create the new one
    try:
        cur.execute('DROP TABLE IF EXISTS E_SCORE;')
        cur.execute("""CREATE TABLE E_SCORE (
            id INTEGER PRIMARY KEY,
            d_date VARCHAR(200),
            USD FLOAT,EUR FLOAT,GBP FLOAT,JPY FLOAT,NZD FLOAT,AUD FLOAT,
            CHF FLOAT,CAD FLOAT,TRY FLOAT,MXN FLOAT,ZAR FLOAT,SEK FLOAT,
            DKK FLOAT,PLN FLOAT,SGD FLOAT,CZK FLOAT,HKD FLOAT,HUF FLOAT,
            NOK FLOAT,RUB FLOAT,THB FLOAT,CHN FLOAT);""")
        db.commit()
    except Exception as e:
        print("problem dropping excel")
        output.append(
            f'problem dropping and creating new table excel in database; {e}')
    
    cur.execute("INSERT INTO logs VALUES ('db_filename', 'E_SCORE CREATED')")
    db.commit()

    try:
        cur.execute('DROP TABLE IF EXISTS GDP;')
        cur.execute("""CREATE TABLE GDP (
            id INTEGER PRIMARY KEY,
            d_date VARCHAR(200),
            USD FLOAT,EUR FLOAT,GBP FLOAT,JPY FLOAT,NZD FLOAT,AUD FLOAT,
            CHF FLOAT,CAD FLOAT,TRY FLOAT,MXN FLOAT,ZAR FLOAT,SEK FLOAT,
            DKK FLOAT,PLN FLOAT,SGD FLOAT,CZK FLOAT,HKD FLOAT,HUF FLOAT,
            NOK FLOAT,RUB FLOAT,THB FLOAT,CHN FLOAT);""")
        db.commit()
    except Exception as e:
        print("problem dropping excel")
        output.append(
            f'problem dropping and creating new table excel in database; {e}')
    
    cur.execute("INSERT INTO logs VALUES ('db_filename', 'GDP CREATED')")
    db.commit()

    df = read_excel(filepath, 0)
    excel_counter = 1
    line_number = 1
    for _, (d_date, USD, EUR, GBP, JPY, NZD, AUD,
            CHF, CAD, TRY, MXN, ZAR, SEK, DKK, PLN, SGD,
            CZK, HKD, HUF, NOK, RUB, THB, CHN) in df.iterrows():
        line_number += 1
        try:
            cur.execute(f'''INSERT INTO E_SCORE VALUES ({line_number}, '{d_date}',
                {USD},{EUR}, {GBP}, {JPY}, {NZD}, {AUD}, {CHF}, {CAD}, {TRY},
                {MXN}, {ZAR}, {SEK}, {DKK}, {PLN}, {SGD}, {CZK}, {HKD},
                {HUF}, {NOK}, {RUB}, {THB}, {CHN});''')
            excel_counter += 1
        except Exception as e:
            total_flashes += 1
            if total_flashes < MAX_FLASH:                
                output.append(
                    f'Error inserting line {line_number} from E_SCORE sheet , {e}')
            elif total_flashes == MAX_FLASH:
                output.append(f'Too many errors!')
        if line_number % 1000 == 0:
            try:
                db.commit()
            except Exception as e:
                output.append(
                    f'Problem commiting data into db at around record {line_number} (or previous 1000 ones); {e}')
    db.commit()
    df = read_excel(filepath, 1)
    df1 = df.replace(np.nan, 'NULL', regex=True)
    excel_counter = 1
    line_number = 1
    for _, (d_date, USD, EUR, GBP, JPY, NZD, AUD,
            CHF, CAD, TRY, MXN, ZAR, SEK, DKK, PLN, SGD,
            CZK, HKD, HUF, NOK, RUB, THB, CHN) in df1.iterrows():
        line_number += 1
        try:
            print(f'''INSERT INTO E_SCORE VALUES ({line_number}, '{d_date}',
                {USD},{EUR}, {GBP}, {JPY}, {NZD}, {AUD}, {CHF}, {CAD}, {TRY},
                {MXN}, {ZAR}, {SEK}, {DKK}, {PLN}, {SGD}, {CZK}, {HKD},
                {HUF}, {NOK}, {RUB}, {THB}, {CHN});''')
            cur.execute(f'''INSERT INTO GDP VALUES ({line_number}, '{d_date}',
                {USD},{EUR}, {GBP}, {JPY}, {NZD}, {AUD}, {CHF}, {CAD}, {TRY},
                {MXN}, {ZAR}, {SEK}, {DKK}, {PLN}, {SGD}, {CZK}, {HKD},
                {HUF}, {NOK}, {RUB}, {THB}, {CHN});''')
            excel_counter += 1
        except Exception as e:
            total_flashes += 1
            if total_flashes < MAX_FLASH:                
                output.append(
                    f'Error inserting line {line_number} from GDP sheet , {e}')
            elif total_flashes == MAX_FLASH:
                output.append(f'Too many errors!')
        if line_number % 1000 == 0:
            try:
                db.commit()
            except Exception as e:
                output.append(
                    f'Problem commiting data into db at around record {line_number} (or previous 1000 ones); {e}')
    db.commit()

     # save the logs
    output.append(f'Inserted {excel_counter} values')
    output.reverse()
    cur.execute("INSERT INTO logs VALUES ('import', %s)", ('\n'.join(output), ))
    db.commit()

    db.close()

    return


import_to_database_from_excel('test.xls')
