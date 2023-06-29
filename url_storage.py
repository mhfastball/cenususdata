import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import psycopg2

home = create_engine("postgresql://macbook:password@localhost/postgres")
conn = psycopg2.connect(host='localhost', dbname='postgres', user='macbook', password='password')

def url_query(qurl):
    cursor = conn.cursor()
    cursor.execute(qurl)
    result_str = cursor.fetchone()[0]
    return result_str

def store_data(url, table_name, var_geo=None):#input 'variables'/'geography' or None
    if var_geo is not None:
        var_tbl = url + '/' + var_geo + '.html'
    else:
        var_tbl = url
    print(var_tbl)
    data = pd.read_html(var_tbl)
    df = data[0]
    redone_col = df.columns
    redone_col = [x.lower().replace(' ', '_') for x in redone_col]
    df.columns = redone_col
    df.to_sql(table_name, con=home, if_exists='replace')