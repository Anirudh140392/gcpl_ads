import requests
import json
import re
from bs4 import BeautifulSoup
import cloudscraper
import pandas as pd
import numpy as np
from datetime import datetime
import pymysql

def blinkit_campaign():
    DB_HOST = "tr-wp-database.cdq264akgdm2.us-east-1.rds.amazonaws.com"
    DB_USER = "tanuj"
    DB_PASSWORD = "Trailytics@83701"
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

    today_date = datetime.today().strftime('%Y-%m-%d')

    def report(today_date):
        sql = f"SELECT * FROM blinkit_campaigns_data_gcpl WHERE (Date = '{today_date}' OR Date = DATE_SUB('{today_date}', INTERVAL 1 DAY)) AND Brand_Name = 'GCPL';"
        cursor.execute(sql)
        data = cursor.fetchall()
        columns = [i[0] for i in cursor.description]
        df = pd.DataFrame(data, columns = columns)
        df['Campaign_Budget'] = df['Campaign_Budget'].astype(float)
        
        return df

    df = report(today_date)
    print(df)

    def report(date1, date2):
        sql = f"SELECT * FROM blinkit_keywords_data_gcpl WHERE Date BETWEEN '{date1}' AND '{date2}' AND Brand_Name = 'GCPL'"
        cursor.execute(sql)
        data = cursor.fetchall()
        columns = [i[0] for i in cursor.description]
        df = pd.DataFrame(data, columns = columns)
        df['Direct_Sales'] = df['Direct_Sales'].astype(float)
        df['Indirect_Sales'] = df['Indirect_Sales'].astype(float)
        df['Estimated_Budget_Consumed'] = df['Estimated_Budget_Consumed'].astype(float)
        df['Direct_RoAS'] = df['Direct_RoAS'].astype(float)
        df['Total_RoAS'] = df['Total_RoAS'].astype(float)

        return df

    df1 = report("2025-01-21", "2025-01-21")
    df2 = report("2025-01-22", "2025-01-22")

    columns = ['Date', 'Campaign_Name', 'Targeting_Value', 'Impressions', 'Direct_ATC', 'Indirect_ATC', 'Direct_Quantities_Sold', 'Indirect_Quantities_Sold',
            'Direct_Sales', 'Indirect_Sales', 'Estimated_Budget_Consumed']

    df1 = df1[columns]
    df1 = df1.replace('', np.nan)
    df1['Total_Sales'] = df1['Direct_Sales'] + df1['Indirect_Sales']
    df1['CPM'] = (df1['Impressions'] / df1['Estimated_Budget_Consumed']) * 1000
    try:
        df1['ROAS'] = df1['Direct_Sales'] / df1['Estimated_Budget_Consumed']
    except ZeroDivisionError:
        df1['ROAS'] = 0
    try:
        df1['TROAS'] = df1['Total_Sales'] / df1['Estimated_Budget_Consumed']
    except ZeroDivisionError:
        df1['TROAS'] = 0

    df1 = df1.groupby(['Campaign_Name', 'Targeting_Value']).agg({
        'Impressions': 'sum',
        'Direct_ATC': 'sum',
        'Indirect_ATC': 'sum',
        'Direct_Quantities_Sold': 'sum',
        'Indirect_Quantities_Sold':'sum',
        'Direct_Sales' : 'sum',
        'Indirect_Sales': 'sum',
        'Total_Sales': 'sum',
        'Estimated_Budget_Consumed': 'sum',
        'CPM': 'mean',
        'ROAS': 'mean',
        'TROAS': 'mean'
    }).reset_index()

    df2 = df2[columns]
    df2 = df2.replace('', np.nan)
    df2['Total_Sales'] = df2['Direct_Sales'] + df2['Indirect_Sales']
    df2['CPM'] = (df2['Impressions'] / df2['Estimated_Budget_Consumed']) * 1000
    try:
        df2['ROAS'] = df2['Direct_Sales'] / df2['Estimated_Budget_Consumed']
    except ZeroDivisionError:
        df2['ROAS'] = 0
    try:
        df2['TROAS'] = df2['Total_Sales'] / df2['Estimated_Budget_Consumed']
    except ZeroDivisionError:
        df2['TROAS'] = 0

    df2 = df2.groupby(['Campaign_Name', 'Targeting_Value']).agg({
        'Impressions': 'sum',
        'Direct_ATC': 'sum',
        'Indirect_ATC': 'sum',
        'Direct_Quantities_Sold': 'sum',
        'Indirect_Quantities_Sold':'sum',
        'Direct_Sales' : 'sum',
        'Indirect_Sales': 'sum',
        'Total_Sales': 'sum',
        'Estimated_Budget_Consumed': 'sum',
        'CPM': 'mean',
        'ROAS': 'mean',
        'TROAS': 'mean'
    }).reset_index()

    merged_df = pd.merge(df2, df1, on = ['Campaign_Name', 'Targeting_Value'], how = 'left')
    merged_df.replace('null', np.nan, inplace = True)

    columns_list = ['Impressions', 'Direct_ATC', 'Indirect_ATC', 'Direct_Quantities_Sold', 'Indirect_Quantities_Sold', 
                    'Direct_Sales', 'Indirect_Sales', 'Estimated_Budget_Consumed', 'Total_Sales', 'CPM', 'ROAS', 'TROAS']

    for column in columns_list:
        try:
            merged_df[f'{column}_diff'] = ((merged_df[f'{column}_y'] - merged_df[f'{column}_x']) / merged_df[f'{column}_x']) * 100
        except ZeroDivisionError:
            merged_df[f'{column}_diff'] = 0

    merged_df.replace(np.inf, 0, inplace = True)

    combined_df = pd.merge(df, merged_df, on = ['Campaign_Name', 'Targeting_Value'], how = 'left')
    combined_df.replace(np.nan, 0, inplace = True)

    # print(combined_df)
    # combined_df.to_excel('jfjsj.xlsx')

    status_mapping = {'ACTIVE': 1, 'STOPPED': 0, 'ON_HOLD': 0, 'COMPLETED': 1}
    combined_df['status'] =  combined_df['Campaign Status'].map(status_mapping)


    merged_data = {}

    merged_data['campaign_id'] = combined_df['Campaign ID'].values.tolist()
    merged_data['campaign_name'] = combined_df['Campaign Name'].values.tolist()
    merged_data['campaign_status'] = combined_df['Campaign Status'].values.tolist()
    merged_data['campaign_start_date'] = combined_df['Campaign Start Date'].values.tolist()
    merged_data['campaign_end_date'] = combined_df['Campaign End Date'].values.tolist()
    merged_data['type'] = combined_df['Campaign Objective Type'].values.tolist()
    merged_data['campaign_budget'] = combined_df['Campaign Budget'].values.tolist()
    merged_data['market_place'] = combined_df['Marketplace'].values.tolist()
    merged_data['impressions'] = combined_df['Impressions_x'].values.tolist()
    merged_data['direct_atc'] = combined_df['Direct ATC_x'].values.tolist()
    merged_data['orders'] = combined_df['Direct Quantities Sold_x'].values.tolist()
    merged_data['sales'] = combined_df['Direct Sales_x'].values.tolist()
    merged_data['spends'] = combined_df['Estimated Budget Consumed_x'].values.tolist()
    merged_data['total_ad_sales'] = combined_df['Total Sales_x'].values.tolist()
    merged_data['cpm'] = combined_df['CPM_x'].values.tolist()
    merged_data['roas'] = combined_df['ROAS_x'].values.tolist()
    merged_data['troas'] = combined_df['TROAS_x'].values.tolist()
    merged_data['impressions_change'] = combined_df['Impressions_diff'].values.tolist()
    merged_data['direct_atc_change'] = combined_df['Direct ATC_diff'].values.tolist()
    merged_data['orders_change'] = combined_df['Direct Quantities Sold_diff'].values.tolist()
    merged_data['sales_change'] = combined_df['Direct Sales_diff'].values.tolist()
    merged_data['spends_change'] = combined_df['Estimated Budget Consumed_diff'].values.tolist()
    merged_data['total_ad_sales_change'] = combined_df['Total Sales_diff'].values.tolist()
    merged_data['cpm_change'] = combined_df['CPM_diff'].values.tolist()
    merged_data['roas_change'] = combined_df['ROAS_diff'].values.tolist()
    merged_data['troas_change'] = combined_df['TROAS_diff'].values.tolist()
    merged_data['campaign_title'] = combined_df['Camapign Title'].values.tolist()
    merged_data['campaign_tags'] = combined_df['campaign tags'].values.tolist()

    new_list = []
    for data in range(len(merged_data['campaign_id'])):
        new_list.append({ i:merged_data[i][data] for i in merged_data.keys()})

    print('==============', new_list)
    # print(new_list)

    return new_list

blinkit_campaign()







