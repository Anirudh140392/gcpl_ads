import requests
import json
import re
from bs4 import BeautifulSoup
import cloudscraper
import pandas as pd
import numpy as np
from datetime import datetime,timedelta
# import datetime,timedelta
import pymysql

def blnkt_neg_kw(start_date='2025-01-22',end_date='2025-01-22'):
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

    start_date = datetime.strptime(start_date , "%Y-%m-%d")
    end_date = datetime.strptime(end_date , "%Y-%m-%d")

    previous_end_date = start_date - timedelta(days=1)
    previous_start_date = previous_end_date - timedelta(days=(end_date - start_date).days)
    
    start_date = start_date.strftime("%Y-%m-%d")
    end_date = end_date.strftime("%Y-%m-%d")

    previous_end_date = previous_end_date.strftime("%Y-%m-%d")
    previous_start_date = previous_start_date.strftime("%Y-%m-%d")
    
    print(start_date,end_date,previous_end_date,previous_start_date)
    def report(date1, date2):
        sql1 = f"SELECT * FROM blinkit_keywords_data_gcpl WHERE Direct_Sales < 1 AND Estimated_Budget_Consumed >= 100 AND Targeting_Type = 'Keyword' AND Date BETWEEN '{date1}' AND '{date2}'"
        cursor.execute(sql1)
        data = cursor.fetchall()
        columns = [i[0] for i in cursor.description]
        df = pd.DataFrame(data, columns = columns)
        df['Estimated_Budget_Consumed'] = df['Estimated_Budget_Consumed'].astype(float)
        df['Direct_Sales'] = df['Direct_Sales'].astype(float)
        df['Indirect_Sales'] = df['Indirect_Sales'].astype(float)
        df['Orders'] = df['Direct_Quantities_Sold'] + df['Indirect_Quantities_Sold']
        df['ACoS'] = (df['Estimated_Budget_Consumed'] / df['Direct_Sales']) * 100
        df['CPATC'] = df['Estimated_Budget_Consumed'] / (df['Direct_ATC'] + df['Indirect_ATC'])
        df['CR'] = ((df['Direct_ATC'] + df['Indirect_ATC']) / df['Impressions']) * 100
        df['CPA'] = df['Estimated_Budget_Consumed'] / df['New_Users_Acquired']
        df.replace([np.inf, -np.inf], 0, inplace = True)

        sql2 = f"SELECT Campaign_ID, Campaign_Name from blinkit_campaigns_data_gcpl where Date BETWEEN '{date1}' AND '{date2}'"
        cursor.execute(sql2)
        data = cursor.fetchall()
        columns = [i[0] for i in cursor.description]
        df_ = pd.DataFrame(data, columns = columns)

        result_dict = dict(zip(df_['Campaign_Name'], df_['Campaign_ID']))

        df['Campaign_ID'] = df['Campaign_Name'].map(result_dict)
        
        df = df.round(2)

        return df

    df1 = report(previous_start_date, previous_end_date)
    df2 = report(end_date, start_date)
    # df1 = report("2025-01-21", "2025-01-21")
    # df2 = report("2025-01-22", "2025-01-22")

    # print(df1)
    # print(df2)

    columns = ['Campaign_ID', 'Campaign_Name', 'Targeting_Value', 'Impressions', 'Orders', 'Direct_ATC', 'Estimated_Budget_Consumed', 'Direct_Sales', 'Direct_RoAS', 'ACoS', 'CPATC', 'CR', 'CPA']

    df1 = df1[columns]

    df1 = df1.groupby(['Campaign_Name', 'Targeting_Value', 'Campaign_ID']).agg({
        'Impressions' : 'sum',
        'Orders':'sum',
        'Direct_ATC': 'sum',
        'Estimated_Budget_Consumed': 'sum',
        'Direct_Sales': 'sum',
        'Direct_RoAS':'mean',
        'ACoS': 'mean',
        'CPATC':'mean',
        'CPA':'mean',
        'CR':'mean'
    })

    df1 = df1.round(2)
    df1.reset_index(inplace = True)

    df2 = df2[columns]

    df2 = df2.groupby(['Campaign_Name', 'Targeting_Value', 'Campaign_ID']).agg({
        'Impressions' : 'sum',
        'Orders':'sum',
        'Direct_ATC': 'sum',
        'Estimated_Budget_Consumed': 'sum',
        'Direct_Sales': 'sum',
        'Direct_RoAS':'mean',
        'ACoS': 'mean',
        'CPATC':'mean',
        'CPA':'mean',
        'CR':'mean'
    })

    df2 = df2.round(2)
    df2.reset_index(inplace = True)

    merged_df = pd.merge(df2, df1, on = ['Campaign_ID', 'Campaign_Name', 'Targeting_Value'], how = 'left')
    merged_df.replace('null', np.nan, inplace = True)

    columns_list = ['Impressions','Orders', 'Direct_ATC', 'Estimated_Budget_Consumed', 'Direct_Sales', 'Direct_RoAS', 'ACoS', 'CPATC', 'CPA', 'CR']

    for column in columns_list:
        try:
            merged_df[f'{column}_diff'] = ((merged_df[f'{column}_x'] - merged_df[f'{column}_y']) / merged_df[f'{column}_y']) * 100
        except ZeroDivisionError:
            merged_df[f'{column}_diff'] = 0

    merged_df['Performance_Type'] = 'Performance'

    merged_df.replace(np.inf, 0, inplace = True)
    merged_df.replace(np.nan, 0, inplace = True)

    # print(merged_df)
    # merged_df.to_excel('hfshhs.xlsx')

    data = {}

    data['search_term'] = merged_df['Targeting_Value'].values.tolist()
    data['type'] = merged_df['Performance_Type'].values.tolist() #AD TYPE
    data['campaign_name'] = merged_df['Campaign_Name'].values.tolist()

    data['impressions'] = merged_df['Impressions_x'].values.tolist()
    data['impressions_change'] = merged_df['Impressions_diff'].values.tolist()
    
    data['direct_atc'] = merged_df['Direct_ATC_x'].values.tolist()
    data['direct_atc_change'] = merged_df['Direct_ATC_diff'].values.tolist()
    
    data['spends'] = merged_df['Estimated_Budget_Consumed_x'].values.tolist()
    data['spends_change'] = merged_df['Estimated_Budget_Consumed_diff'].values.tolist()
    
    data['sales'] = merged_df['Direct_Sales_x'].values.tolist()  
    data['sales_change'] = merged_df['Direct_Sales_diff'].values.tolist()
    
    data['acos'] = merged_df['ACoS_x'].values.tolist()
    data['acos_change'] = merged_df['ACoS_diff'].values.tolist()

    new_list = []
    for d in range(len(data['campaign_name'])):
        new_dict = {}

        for i in data.keys():
            try:
                new_dict[i] = float(data[i][d])
            except ValueError:
                new_dict[i] = data[i][d]
        new_list.append(new_dict)

    # print(new_list)
    return new_list
    

# blnkt_neg_kw()



