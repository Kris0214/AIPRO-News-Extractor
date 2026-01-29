import time
import pandas as pd
import oracledb

def get_SQL_raw_data(account, pwd, query, ods_type = 'ods'): # oracledb
    ACCOUNT = account # user name for ods
    PASSWORD = pwd # password 
    HOST = 'sndmp-scan.fbs100.campus'
    PORT = '5211'
    SERVICE_NAME = 'SNDMP_USER_DS'
    
    t0 = time.time()

    oracledb.init_oracle_client(lib_dir=r"D:\instantclient_23_9")
    conn = oracledb.connect(user = account, password = pwd, dsn = HOST + ':' + PORT + '/' + SERVICE_NAME)
    cursor = conn.cursor()

    query = f"""{query}"""
    cursor = cursor.execute(query)

    df_columns =[]
    for i in range(0, len(cursor.description)):
        temp = cursor.description[i][0]
        df_columns.append(temp)
        

    df_sql = pd.DataFrame(cursor.fetchall())
    df_sql.columns = df_columns

    t1 = time.time()
    # print(f"Running time of: {t1 - t0} sec".format('.0f'))
    # conn.close()
    return df_sql


def get_SQL_raw_data_clob(account, pwd, query, ods_type = 'clob'): # oracledb
    ACCOUNT = account # user name for ods
    PASSWORD = pwd # password 
    HOST = 'sndmp-scan.fbs100.campus'
    PORT = '5211'
    SERVICE_NAME = 'SNDMP_USER_DS'
    
    t0 = time.time()

    oracledb.init_oracle_client(lib_dir=r"D:\instantclient_23_9")
    conn = oracledb.connect(user = account, password = pwd, dsn = HOST + ':' + PORT + '/' + SERVICE_NAME)
    cursor = conn.cursor()

    query = f"""{query}"""
    cursor = cursor.execute(query)
    cursor = cursor.fetchall()
    
    news_date = []
    news = []
    related_product = []

    for row in cursor:
        if row[0] is not None:
            txt = row[0].strftime("%Y%m%d")
            news_date.append(txt)
            print('此筆資料已成功寫入!')
        
        else:
            print('此筆資料為nonetype!')


    for row in cursor:
        if row[1] is not None:
            txt = row[1].read()
            news.append(txt)
            print('此筆資料已成功寫入!')
        
        else:
            print('此筆資料為nonetype!')


    for row in cursor:
        if row[2] is not None:
            txt = row[2]
            related_product.append(txt)
            print('此筆資料已成功寫入!')
        
        else:
            print('此筆資料為nonetype!')


    df_sql = pd.concat([pd.DataFrame(news_date), pd.DataFrame(news), pd.DataFrame(related_product)], axis=1)
    df_sql.columns = ['snap_yyyymm', 'news', 'related_product']


    t1 = time.time()
    # print(f"Running time of: {t1 - t0} sec".format('.0f'))
    # conn.close()
    return df_sql