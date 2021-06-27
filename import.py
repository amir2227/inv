from pandas import read_excel 
from sqlalchemy import create_engine


def import_to_database_from_excel(filepath):
    '''Get a excel file name and imports data from it.'''
    df = read_excel(filepath)
    print(df)
import_to_database_from_excel('test.xls')
