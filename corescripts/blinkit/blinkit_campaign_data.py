import requests
import json
import re
from bs4 import BeautifulSoup
import cloudscraper
import pandas as pd
import numpy as np
from datetime import datetime,timedelta
import pymysql


def blinkit_campaign(start_date='2025-01-21',end_date='2025-01-22'):
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

    start_date=start_date.replace('/','-')
    end_date=start_date.replace('/','-')
    
    start_date = datetime.strptime(start_date , "%Y-%m-%d")
    end_date = datetime.strptime(end_date , "%Y-%m-%d")

    previous_end_date = start_date - timedelta(days=1)
    previous_start_date = previous_end_date - timedelta(days=(end_date - start_date).days)
    

    start_date = start_date.strftime("%Y-%m-%d")
    end_date = end_date.strftime("%Y-%m-%d")

    previous_end_date = previous_end_date.strftime("%Y-%m-%d")
    previous_start_date = previous_start_date.strftime("%Y-%m-%d")


    today_date = datetime.today().strftime('%Y-%m-%d')
    yesterday = (pd.to_datetime(today_date) - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    # print()

    def report(today_date):
        # sql = f"SELECT * FROM blinkit_campaigns_data_gcpl WHERE Date = '{today_date}'  AND Brand_Name = 'GCPL';"
        sql = f"""
        WITH today_data AS (
            SELECT * 
            FROM blinkit_campaigns_data_gcpl 
            WHERE Date = '{today_date}' AND Brand_Name = 'GCPL'
        ),
        yesterday_data AS (
            SELECT * 
            FROM blinkit_campaigns_data_gcpl 
            WHERE Date = '{yesterday}' AND Brand_Name = 'GCPL'
        )
        SELECT * FROM today_data 
        UNION ALL 
        SELECT * FROM yesterday_data 
        WHERE NOT EXISTS (SELECT 1 FROM today_data);
        """
        cursor.execute(sql)
        data = cursor.fetchall()
        columns = [i[0] for i in cursor.description]
        df = pd.DataFrame(data, columns = columns)
        df['Campaign_Budget'] = df['Campaign_Budget'].astype(float)
        df['CPM_Exact'] = df['CPM_Exact'].astype(float)
        df['CPM_Smart'] = df['CPM_Smart'].astype(float)
        
        df = df.groupby('Campaign_ID').agg({
        'Campaign_Name': 'first', 
        'Campaign_Status': 'first',
        'Campaign_Start_Date': 'first', 
        'Campaign_End_Date': 'first', 
        'Campaign_Type': 'first',
        'Match_Type': 'first', 
        'Type': 'first',
        'Campaign_Title': 'first',
        'Brand_Name': 'first',
        'Campaign_Objective_Type': 'first',    
        'Campaign_Budget': 'mean',
        'CPM_Exact': 'sum',
        'CPM_Smart': 'sum'
        }).reset_index()
            
        return df

    df = report(today_date)
    # print(df)

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

    # df1 = report("2025-01-21", "2025-01-21")
    # df2 = report("2025-01-22", "2025-01-22")
    df1=report(previous_start_date,previous_end_date)
    df2=report(start_date,end_date)

    columns = ['Date', 'Campaign_Name', 'Targeting_Value', 'Impressions', 'Direct_ATC', 'Indirect_ATC', 'Direct_Quantities_Sold', 'Indirect_Quantities_Sold',
            'Direct_Sales', 'Indirect_Sales', 'Estimated_Budget_Consumed']

    df1 = df1[columns]
    df1 = df1.replace('', np.nan)

    
    df1 = df1.groupby('Campaign_Name').agg({
        'Impressions': 'sum',
        'Direct_ATC': 'sum',
        'Indirect_ATC': 'sum',
        'Direct_Quantities_Sold': 'sum',
        'Indirect_Quantities_Sold':'sum',
        'Direct_Sales' : 'sum',
        'Indirect_Sales': 'sum',
        'Estimated_Budget_Consumed': 'sum',
    }).reset_index()

    df1['Total_Sales'] = df1['Direct_Sales'] + df1['Indirect_Sales']
    df1['CPM'] = (df1['Estimated_Budget_Consumed'] / df1['Impressions']) * 1000
    try:
        df1['ROAS'] = df1['Direct_Sales'] / df1['Estimated_Budget_Consumed']
    except ZeroDivisionError:
        df1['ROAS'] = 0
    try:
        df1['TROAS'] = df1['Total_Sales'] / df1['Estimated_Budget_Consumed']
    except ZeroDivisionError:
        df1['TROAS'] = 0

    
    df2 = df2[columns]
    df2 = df2.replace('', np.nan)
    
    df2 = df2.groupby('Campaign_Name').agg({
        'Impressions': 'sum',
        'Direct_ATC': 'sum',
        'Indirect_ATC': 'sum',
        'Direct_Quantities_Sold': 'sum',
        'Indirect_Quantities_Sold':'sum',
        'Direct_Sales' : 'sum',
        'Indirect_Sales': 'sum',
        'Estimated_Budget_Consumed': 'sum',
    }).reset_index()


    df2['Total_Sales'] = df2['Direct_Sales'] + df2['Indirect_Sales']
    
    df2['CPM'] = (df2['Estimated_Budget_Consumed'] / df2['Impressions']) * 1000

    try:
        df2['ROAS'] = df2['Direct_Sales'] / df2['Estimated_Budget_Consumed']
    except ZeroDivisionError:
        df2['ROAS'] = 0
   
    try:
        df2['TROAS'] = df2['Total_Sales'] / df2['Estimated_Budget_Consumed']
    except ZeroDivisionError:
        df2['TROAS'] = 0

    merged_df = pd.merge(df2, df1, on ='Campaign_Name', how = 'left')
    merged_df.replace('null', np.nan, inplace = True)

    columns_list = ['Impressions', 'Direct_ATC', 'Indirect_ATC', 'Direct_Quantities_Sold', 'Indirect_Quantities_Sold', 
                    'Direct_Sales', 'Indirect_Sales', 'Estimated_Budget_Consumed', 'Total_Sales', 'CPM', 'ROAS', 'TROAS']

    for column in columns_list:
        try:
            merged_df[f'{column}_diff'] = ((merged_df[f'{column}_y'] - merged_df[f'{column}_x']) / merged_df[f'{column}_x']) * 100
        except ZeroDivisionError:
            merged_df[f'{column}_diff'] = 0

    merged_df.replace(np.inf, 0, inplace = True)

    combined_df = pd.merge(df, merged_df, on = 'Campaign_Name', how = 'left')
    combined_df.replace(np.nan, 0, inplace = True)

    # print(combined_df)
    combined_df.to_excel('25_289.xlsx')

    status_mapping = {'ACTIVE': 1, 'STOPPED': 0, 'ON_HOLD': 0, 'COMPLETED': 1}
    combined_df['status'] =  combined_df['Campaign_Status'].map(status_mapping)

    # print(combined_df.columns)

    merged_data = {}

    merged_data['campaign_id'] = combined_df['Campaign_ID'].values.tolist()
   
    merged_data['campaign_name'] = combined_df['Campaign_Name'].values.tolist()
   
    merged_data['campaign_status'] = combined_df['Campaign_Status'].values.tolist()
   
    merged_data['campaign_start_date'] = combined_df['Campaign_Start_Date'].values.tolist()
    merged_data['campaign_end_date'] = combined_df['Campaign_End_Date'].values.tolist()
   
    merged_data['type'] = combined_df['Campaign_Objective_Type'].values.tolist()
   
    merged_data['campaign_budget'] = combined_df['Campaign_Budget'].values.tolist()
    # merged_data['market_place'] = combined_df['Marketplace'].values.tolist()
   
    merged_data['impressions'] = combined_df['Impressions_x'].values.tolist()
    merged_data['impressions_change'] = combined_df['Impressions_diff'].values.tolist()

    merged_data['direct_atc'] = combined_df['Direct_ATC_x'].values.tolist()
    merged_data['direct_atc_change'] = combined_df['Direct_ATC_diff'].values.tolist()

    merged_data['orders'] = combined_df['Direct_Quantities_Sold_x'].values.tolist()
    merged_data['orders_change'] = combined_df['Direct_Quantities_Sold_diff'].values.tolist()

    merged_data['sales'] = combined_df['Direct_Sales_x'].values.tolist()
    merged_data['sales_change'] = combined_df['Direct_Sales_diff'].values.tolist()

    merged_data['spends'] = combined_df['Estimated_Budget_Consumed_x'].values.tolist()
    merged_data['spends_change'] = combined_df['Estimated_Budget_Consumed_diff'].values.tolist()

    merged_data['total_ad_sales'] = combined_df['Total_Sales_x'].values.tolist()
    merged_data['total_ad_sales_change'] = combined_df['Total_Sales_diff'].values.tolist()

    merged_data['cpm'] = combined_df['CPM_x'].values.tolist()
    merged_data['cpm_change'] = combined_df['CPM_diff'].values.tolist()

    merged_data['roas'] = combined_df['ROAS_x'].values.tolist()
    merged_data['roas_change'] = combined_df['ROAS_diff'].values.tolist()

    merged_data['troas'] = combined_df['TROAS_x'].values.tolist()
    merged_data['troas_change'] = combined_df['TROAS_diff'].values.tolist()
    
    merged_data['campaign_title'] = combined_df['Campaign_Title'].values.tolist()
    # merged_data['campaign_tags'] = combined_df['campaign tags'].values.tolist()
    # print(merged_data['campaign_status'])

    new_list = []
    for data in range(len(merged_data['campaign_id'])):
        new_list.append({ i:merged_data[i][data] for i in merged_data.keys()})

    # print('==============', new_list)
    # print(new_list)

    return new_list

# blinkit_campaign('2025-01-25','2025-01-28')







