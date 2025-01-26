import requests
import json
import re
from bs4 import BeautifulSoup
import cloudscraper
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pymysql


def gcpl_product_page(today_date):
    today_date = (datetime.today()-timedelta(days = 1)).strftime('%Y-%m-%d')

    DB_HOST = "tr-wp-database.cdq264akgdm2.us-east-1.rds.amazonaws.com"
    DB_USER = "Python"
    DB_PASSWORD = "Trailytics@7899"
    DB_DATABASE = "ey_ads_auto"
    DB_PORT = 3306

    connection = pymysql.connect(host = DB_HOST,
                                    user = DB_USER,
                                    password = DB_PASSWORD,
                                    db = DB_DATABASE,
                                    port = DB_PORT,
                                    connect_timeout = 1000,
                                    autocommit = True)

    cursor = connection.cursor()

    def report(today_date):
        sql = f"SELECT * FROM blinkit_product_page_data_gcpl WHERE Date = '{today_date}' AND Brand_Name = 'GCPL'"
        cursor.execute(sql)
        data = cursor.fetchall()
        columns = [i[0] for i in cursor.description]
        df = pd.DataFrame(data, columns = columns)

        return df

    df1 = report(today_date)
    print(df1)

    DB_HOST = "tr-wp-database.cdq264akgdm2.us-east-1.rds.amazonaws.com"
    DB_USER = "Python"
    DB_PASSWORD = "Trailytics@7899"
    DB_DATABASE = "Godrej"
    DB_PORT = 3306

    connection = pymysql.connect(host = DB_HOST,
                                    user = DB_USER,
                                    password = DB_PASSWORD,
                                    db = DB_DATABASE,
                                    port = DB_PORT,
                                    connect_timeout = 1000,
                                    autocommit = True)

    cursor = connection.cursor()


    sql = f"SELECT pdp_title_value as Product_Name, price_rp as MRP, price_sp AS SP, osa as OSA, osa_remark as OSA_Remark FROM rb_pdp WHERE DATE(created_on) = '{today_date}' AND pf_id = 4"
    cursor.execute(sql)
    data = cursor.fetchall()
    columns = [i[0] for i in cursor.description]
    df2 = pd.DataFrame(data, columns = columns)
    print(df2)

    df = pd.merge(df1, df2, on = 'Product_Name', how = 'left')
    df.replace(np.nan, 0, inplace = True)

    print(df)

    df['SP'] = df['SP'].apply(float)
    df['MRP'] = df['MRP'].apply(float)

    df = df.sort_values(by=['OSA'], ascending=False)
    df = df.reset_index(drop=True)

    # print(df)
    # print(df.columns)

    merged_data = {}

    merged_data['product_name'] = df['Product_Name'].values.tolist()
    merged_data['mrp'] = df['MRP'].values.tolist()
    merged_data['sp'] = df['SP'].values.tolist()
    merged_data['osa'] = df['OSA'].values.tolist()
    merged_data['osa_remark'] = df['OSA_Remark'].values.tolist()
    merged_data['campaign_name'] = df['Campaign_Name'].values.tolist()
    merged_data['campaign_id'] = df['Campaign_ID'].values.tolist()

    new_list = []
    for data in range(len(merged_data['campaign_id'])):
        new_list.append({ i:merged_data[i][data] for i in merged_data.keys()})

    # print('--------', new_list)
    # print(len(new_list))

    return new_list

# blnkt_product_page("2024-09-14")

import requests
import json
import re
from bs4 import BeautifulSoup
import cloudscraper
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pymysql

def blnkt_prod_nivea(start_date):
    print(start_date, '**')

    start_date = "2024-09-14"

    DB_HOST = "tr-wp-database.cdq264akgdm2.us-east-1.rds.amazonaws.com"
    DB_USER = "tanuj"
    DB_PASSWORD = "Trailytics@83701"
    DB_DATABASE =  "amazon_ads_api"
    DB_PORT = 3306
    
    connection = pymysql.connect(host = DB_HOST,
                                    user = DB_USER,
                                    password = DB_PASSWORD,
                                    db = DB_DATABASE,
                                    port = DB_PORT,
                                    connect_timeout = 1000,
                                    autocommit = True)

    cursor = connection.cursor()

    def report(start_date):
        sql = f"SELECT * FROM blinkit_product_page_data_kelloggs WHERE Date = '{start_date}' AND Brand_Name = 'Kelloggs'"
        cursor.execute(sql)
        data = cursor.fetchall()
        columns = [i[0] for i in cursor.description]
        df = pd.DataFrame(data, columns = columns)

        return df

    df1 = report(start_date)
    # print(df1)

    DB_HOST = "tr-wp-database.cdq264akgdm2.us-east-1.rds.amazonaws.com"
    DB_USER = "tanuj"
    DB_PASSWORD = "Trailytics@83701"
    DB_DATABASE =  "kelloggs"
    DB_PORT = 3306
    
    connection = pymysql.connect(host = DB_HOST,
                                    user = DB_USER,
                                    password = DB_PASSWORD,
                                    db = DB_DATABASE,
                                    port = DB_PORT,
                                    connect_timeout = 1000,
                                    autocommit = True)

    cursor = connection.cursor()


    sql = f"SELECT pdp_title_value as Product_Name, price_rp as MRP, price_sp AS SP, osa as OSA, osa_remark as OSA_Remark FROM rb_pdp WHERE DATE(created_on) = '{start_date}' AND pf_id = 1"
    cursor.execute(sql)
    data = cursor.fetchall()
    columns = [i[0] for i in cursor.description]
    df2 = pd.DataFrame(data, columns = columns)
    # print(df2)

    df = pd.merge(df1, df2, on = 'Product_Name', how = 'left')
    df.replace(np.nan, 0, inplace = True)

    df['SP'] = df['SP'].apply(float)
    df['MRP'] = df['MRP'].apply(float)

    df = df.sort_values(by=['OSA'], ascending=False)
    df = df.reset_index(drop=True)

    # print(df)
    # print(df.columns)

    merged_data = {}

    merged_data['product_name'] = df['Product_Name'].values.tolist()
    merged_data['mrp'] = df['MRP'].values.tolist()
    merged_data['sp'] = df['SP'].values.tolist()
    merged_data['osa'] = df['OSA'].values.tolist()
    merged_data['osa_remark'] = df['OSA_Remark'].values.tolist()
    merged_data['campaign_name'] = df['Campaign_Name'].values.tolist()
    merged_data['campaign_id'] = df['Campaign_ID'].values.tolist()

    new_list = []
    for data in range(len(merged_data['campaign_id'])):
        new_list.append({ i:merged_data[i][data] for i in merged_data.keys()})

    # print('--------', new_list)
    # print(len(new_list))

    return new_list

# blnkt_prod_nivea("2024-09-14")