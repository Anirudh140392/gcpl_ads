import requests
import json
import re
from bs4 import BeautifulSoup
import cloudscraper
import pandas as pd
import numpy as np
from datetime import datetime,timedelta
import pymysql
from decimal import Decimal

DB_HOST = "tr-wp-database.cdq264akgdm2.us-east-1.rds.amazonaws.com"
DB_USER = "Python"
DB_PASSWORD = "Trailytics@7899"
DB_DATABASE = "ey_ads_auto"
DB_PORT = 3306

def human_format(num):
    num = float('{:.3g}'.format(num))
    magnitude = 0
    while abs(num) >= 1000:
        magnitude += 1
        num /= 1000.0
    return '{}{}'.format('{:f}'.format(num).rstrip('0').rstrip('.'), ['', 'K', 'M', 'B', 'T'][magnitude])

def blnkt_ov_data(start_date='2025-01-22',end_date='2025-01-22'):

    start_date = datetime.strptime(start_date , "%Y-%m-%d")
    end_date = datetime.strptime(end_date , "%Y-%m-%d")

    previous_end_date = start_date - timedelta(days=1)
    previous_start_date = previous_end_date - timedelta(days=(end_date - start_date).days)
    

    start_date = start_date.strftime("%Y-%m-%d")
    end_date = end_date.strftime("%Y-%m-%d")

    previous_end_date = previous_end_date.strftime("%Y-%m-%d")
    previous_start_date = previous_start_date.strftime("%Y-%m-%d")

    # print(start_date,end_date,previous_start_date,previous_end_date)
    connection = pymysql.connect(host = DB_HOST,
                                user = DB_USER,
                                password = DB_PASSWORD,
                                db = DB_DATABASE,
                                port = DB_PORT,
                                connect_timeout = 1000,
                                autocommit = True)

    cursor = connection.cursor()

    def report(date1, date2):
        sql = f"SELECT * FROM blinkit_keywords_data_gcpl WHERE Date BETWEEN '{date1}' AND '{date2}' AND Match_Type IN('EXACT', '') AND Brand_Name = 'GCPL'"
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
    # Impressions and ATC to figure out in above report
    # df1 = report("2025-01-21", "2025-01-21")
    # df2 = report("2025-01-22", "2025-01-22")

    print("overview page details are ",previous_start_date,previous_end_date,start_date,end_date)
    df1=report(previous_start_date,previous_end_date)
    df2=report(start_date,end_date)

    
    columns = ['Date', 'Campaign_Name', 'Targeting_Type', 'Targeting_Value', 'Impressions', 'Direct_ATC', 'Indirect_ATC',
            'Direct_Quantities_Sold', 'Indirect_Quantities_Sold', 'Direct_Sales', 'Indirect_Sales', 
            'Estimated_Budget_Consumed']

    df1 = df1[columns]

    # ATC is clicks
    # CTR(CLicks/Impressions)
    # CPM(Spends/Impressions)
    
    df1['Total_Sales'] = df1['Direct_Sales'] + df1['Indirect_Sales']

    df1['Total_ATC']=df1['Direct_ATC']+df1['Indirect_ATC']

    df1['Clicks']= df1['Direct_ATC']+df1['Indirect_ATC']

    df1['CPM'] = (df1['Estimated_Budget_Consumed'] / df1['Impressions']) * 1000
    df1['CTR'] = ((df1['Direct_ATC'] + df1['Indirect_ATC']) / df1['Impressions']) * 100
    df1['CVR'] = ((df1['Direct_Quantities_Sold'] + df1['Indirect_Quantities_Sold']) / (df1['Direct_ATC'] + df1['Indirect_ATC']) ) * 100
    df1['CPC'] = (df1['Estimated_Budget_Consumed'] / df1['Direct_ATC'])
    df1['AOV'] = ((df1['Direct_Sales'] + df1['Indirect_Sales']) / df1['Direct_Quantities_Sold'])
    df1['ACOS'] = (df1['Estimated_Budget_Consumed'] / df1['Direct_Sales'])
   
    try:
        df1['ROAS'] = df1['Total_Sales'] / df1['Estimated_Budget_Consumed']
    except ZeroDivisionError:
        df1['ROAS'] = 0
    
    try:
        df1['TROAS'] = df1['Total_Sales'] / df1['Estimated_Budget_Consumed']
    except ZeroDivisionError:
        df1['TROAS'] = 0

    df1 = df1.groupby(['Campaign_Name', 'Targeting_Type', 'Targeting_Value']).agg({
        'Estimated_Budget_Consumed': 'sum',
        'Impressions': 'sum',
        'Direct_Sales': 'sum',
        'Indirect_Sales': 'sum',
        'Total_Sales': 'sum',
        'Direct_ATC': 'sum',
        'Indirect_ATC': 'sum',
        'Direct_Quantities_Sold': 'sum',
        'Indirect_Quantities_Sold': 'sum',
        'CPM': 'mean',
        'CTR': 'mean',
        'CVR': 'mean',
        'CPC': 'mean',
        'AOV': 'mean',
        'ACOS': 'mean',
        'ROAS': 'mean',
        'TROAS': 'mean'
    })
    #rounding to 2 decimal
    df1 = df1.round(2)
    df1.reset_index(inplace = True)

    df2 = df2[columns]
    df2['Total_Sales'] = df2['Direct_Sales'] + df2['Indirect_Sales']
    df2['Clicks']= df2['Direct_ATC']+df2['Indirect_ATC']
    
    df2['Total_ATC']=df2['Direct_ATC']+df2['Indirect_ATC']
    
    df2['CPM'] = (df2['Estimated_Budget_Consumed'] / df2['Impressions']) * 1000
    
    df2['CTR'] = ((df2['Direct_ATC'] + df2['Indirect_ATC']) / df2['Impressions']) * 100
    df2['CVR'] = ((df2['Direct_Quantities_Sold'] + df2['Indirect_Quantities_Sold']) / (df2['Direct_ATC'] + df2['Indirect_ATC']) ) * 100
    
    df2['CPC'] = (df2['Estimated_Budget_Consumed'] / df2['Direct_ATC'])
    
    df2['AOV'] = ((df2['Direct_Sales'] + df1['Indirect_Sales']) / df2['Direct_Quantities_Sold'])
    df2['ACOS'] = (df2['Estimated_Budget_Consumed'] / df2['Direct_Sales'])
    
    try:
        df2['ROAS'] = df2['Total_Sales'] / df2['Estimated_Budget_Consumed']
    except ZeroDivisionError:
        df2['ROAS'] = 0
    
    try:
        df2['TROAS'] = df2['Total_Sales'] / df2['Estimated_Budget_Consumed']
    except ZeroDivisionError:
        df2['TROAS'] = 0

    df2 = df2.groupby(['Campaign_Name', 'Targeting_Type', 'Targeting_Value']).agg({
        'Estimated_Budget_Consumed': 'sum',
        'Impressions': 'sum',
        'Direct_Sales': 'sum',
        'Indirect_Sales': 'sum',
        'Total_Sales': 'sum',
        'Direct_ATC': 'sum',
        'Indirect_ATC': 'sum',
        'Direct_Quantities_Sold': 'sum',
        'Indirect_Quantities_Sold': 'sum',
        'CPM': 'mean',
        'CTR': 'mean',
        'CVR': 'mean',
        'CPC': 'mean',
        'AOV': 'mean',
        'ACOS': 'mean',
        'ROAS': 'mean',
        'TROAS': 'mean'
    })

    df2 = df2.round(2)
    df2.reset_index(inplace = True)

    merged_df = pd.merge(df2, df1, on = ['Campaign_Name', 'Targeting_Type', 'Targeting_Value'], how = 'left')
    #merged both df based on current data

    merged_df.replace(np.inf, 0, inplace=True)
    merged_df.replace(np.nan, 0, inplace=True)

    merged_df.to_excel('sjsjsj.xlsx')

    # print(merged_df)
    # exit()
    columns_list = ['Estimated_Budget_Consumed', 'Impressions', 'Direct_Sales', 'Indirect_Sales', 'Total_Sales', 
                    'Direct_ATC', 'Indirect_ATC', 'Direct_Quantities_Sold', 'Indirect_Quantities_Sold','CPM', 'CTR', 
                    'CPC', 'CVR', 'AOV', 'ACOS', 'ROAS', 'TROAS']

    for column in columns_list:
        try:
            merged_df[f'{column}_diff'] = ((merged_df[f'{column}_x'] - merged_df[f'{column}_y']) / merged_df[f'{column}_y']) * 100
        except ZeroDivisionError:
            merged_df[f'{column}_diff'] = 0

    merged_df.replace(np.inf, 0, inplace = True)
    merged_df.replace(np.nan, 0, inplace = True)

    merged_df.loc[merged_df['Campaign_Name'].str.contains('PA_Liquid|pa_liquids|pa liquids', case=False), 'Campaign_Tags'] = 'PA Liquids'
    merged_df.loc[merged_df['Campaign_Name'].str.contains('PA_Deos|pa_deos|pa_deo|pa deos|pa deo', case=False), 'Campaign_Tags'] = 'PA Deos'
    merged_df.loc[merged_df['Campaign_Name'].str.contains('PA_EDPS|PA_EDP|pa_edps|pa edps', case=False), 'Campaign_Tags'] = 'PA EDPs'
    merged_df.loc[merged_df['Campaign_Name'].str.contains('PA_Signature_Gift|PA_GIFT|pa_gift_sets|pa gift sets|pa_edp_gift_set|pa edp gift set', case=False), 'Campaign_Tags'] = 'PA Gift Sets'

    merged_df.loc[merged_df['Campaign_Name'].str.contains('Aer_Aerosol|aer_aerosol|aer aerosol', case=False), 'Campaign_Tags'] = 'Aer Aerosol'
    merged_df.loc[merged_df['Campaign_Name'].str.contains('Aer_Pocket|aer_pocket|aer pocket', case=False), 'Campaign_Tags'] = 'Aer Pocket'
    merged_df.loc[merged_df['Campaign_Name'].str.contains('Aer_Matic|aer_matic|aer matic', case=False), 'Campaign_Tags'] = 'Aer Matic'
    merged_df.loc[merged_df['Campaign_Name'].str.contains('Aer_Car|aer_car|aer car', case=False), 'Campaign_Tags'] = 'Aer Car'

    merged_df.loc[merged_df['Campaign_Name'].str.contains('Genteel|genteel', case=False), 'Campaign_Tags'] = 'Genteel'
    merged_df.loc[merged_df['Campaign_Name'].str.contains('Ezee|ezee', case=False), 'Campaign_Tags'] = 'Ezee'
    merged_df.loc[merged_df['Campaign_Name'].str.contains('Fab|fab', case=False), 'Campaign_Tags'] = 'Fab'

    # Default 'Others'
    merged_df.loc[~merged_df['Campaign_Name'].str.contains(
        'pa_liquids|pa liquids|pa_deo|pa_deos|pa deos|pa deo|pa_edps|pa edps|pa_gift_sets|pa gift sets|pa_edps_gift_sets|pa edps gift sets|aer_aerosol|aer aerosol|aer_pocket|aer pocket|aer_matic|aer matic|aer_car|aer car|genteel|ezee|fab', 
        case=False, na=False), 'Campaign_Tags'] = 'Others'

    merged_df = merged_df.fillna('')

    # # print(merged_df)
    
    # Graph data
    def graph_data(date1, date2):
        sql = f"SELECT Date, CPM, Impressions, Direct_ATC, Indirect_ATC, Direct_Quantities_Sold, Indirect_Quantities_Sold, Direct_Sales, Indirect_Sales, New_Users_Acquired, Estimated_Budget_Consumed, Direct_RoAS, Total_RoAS FROM blinkit_keywords_data_gcpl WHERE Date BETWEEN '{date1}' AND '{date2}' AND Brand_Name = 'GCPL'"
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

    df = graph_data(start_date, end_date)

    df['Total_Sales'] = df['Direct_Sales'] + df['Indirect_Sales']

    df['Total_Clicks'] = df['Direct_ATC'] + df['Indirect_ATC']

    df['Total_ATC'] = df['Direct_ATC'] + df['Indirect_ATC']
    
    df['Total_Orders'] = df['Direct_Quantities_Sold'] + df['Indirect_Quantities_Sold']
    df['CPM'] = (df['Estimated_Budget_Consumed'] / df['Impressions']) * 1000
    df['CTR'] = ((df['Direct_ATC'] + df1['Indirect_ATC']) / df['Impressions']) * 100
    df['CVR'] = ((df['Direct_Quantities_Sold'] + df['Indirect_Quantities_Sold']) / (df['Direct_ATC'] + df['Indirect_ATC']) ) * 100
    df['CPC'] = (df['Estimated_Budget_Consumed'] / df['Direct_ATC'])
    df['AOV'] = ((df['Direct_Sales'] + df['Indirect_Sales']) / df['Direct_Quantities_Sold'])
    df['ACOS'] = (df['Estimated_Budget_Consumed'] / df['Direct_Sales'])
    try:
        df['ROAS'] = df['Total_Sales'] / df['Estimated_Budget_Consumed']
    except ZeroDivisionError:
        df['ROAS'] = 0

    try:
        df['TROAS'] = df['Total_Sales'] / df['Estimated_Budget_Consumed']
    except ZeroDivisionError:
        df['TROAS'] = 0

    df = df.groupby('Date').agg({
        'Estimated_Budget_Consumed': 'sum',
        'Impressions': 'sum',
        'Direct_Sales': 'sum',
        'Indirect_Sales': 'sum',
        'Total_Sales': 'sum',
        'Total_Clicks': 'sum',
        'Total_ATC':'sum',
        'Total_Orders': 'sum',
        'Direct_ATC': 'sum',
        'Indirect_ATC': 'sum',
        'Direct_Quantities_Sold': 'sum',
        'Indirect_Quantities_Sold': 'sum',
        'CPM': 'mean',
        'CTR': 'mean',
        'CVR': 'mean',
        'CPC': 'mean',
        'AOV': 'mean',
        'ACOS': 'mean',
        'ROAS': 'mean',
        'TROAS': 'mean'
    })
    df = df.round(2)
    df.reset_index(inplace = True)
    df.replace(np.inf, 0, inplace = True)
    df.replace(np.nan, 0, inplace = True)
    # print(df)

    df['Date'] = pd.to_datetime(df['Date'])
    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
    # print(df)
    # exit()

    final_data = {}
    final_data['date'] = df['Date'].values.tolist()
    final_data['ads_pend'] = df['Estimated_Budget_Consumed'].values.tolist()
    final_data['impressions'] = df['Impressions'].values.tolist()
    final_data['clicks'] = df['Total_Clicks'].values.tolist()

    final_data['atc']=df['Total_ATC'].values.tolist()
    final_data['ctr'] = df['CTR'].values.tolist()
    final_data['cdcu'] = df['Total_Orders'].values.tolist()
    final_data['cicu'] = df['AOV'].values.tolist()
    final_data['cvr'] = df['CVR'].values.tolist()
    final_data['cpm']=df['CPM'].values.tolist()
    final_data['cdcr'] = df['Total_Sales'].values.tolist()
    final_data['cicr'] = df['CPC'].values.tolist()
    final_data['roi'] = df['ROAS'].values.tolist()

    for key in final_data:
        final_data[key] = [float(value) if isinstance(value, Decimal) else value for value in final_data[key]]

    # print(final_data)

    return final_data

# blnkt_ov_data('2025-01-24','2025-01-27')