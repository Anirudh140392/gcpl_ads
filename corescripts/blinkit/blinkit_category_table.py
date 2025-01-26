import requests
import json
import re
from bs4 import BeautifulSoup
import cloudscraper
import pandas as pd
import numpy as np
from datetime import datetime,timedelta
import pymysql

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

def blnkt_cat_data(start_date='2025/01/22',end_date='2025/01/22'):
    
    start_date = start_date.replace('/', '-')
    end_date = end_date.replace('/', '-')
    
    start_date = datetime.strptime(start_date , "%Y-%m-%d")
    end_date = datetime.strptime(end_date , "%Y-%m-%d")

    previous_end_date = start_date - timedelta(days=1)
    previous_start_date = previous_end_date - timedelta(days=(end_date - start_date).days)
    

    start_date = start_date.strftime("%Y-%m-%d")
    end_date = end_date.strftime("%Y-%m-%d")

    previous_end_date = previous_end_date.strftime("%Y-%m-%d")
    previous_start_date = previous_start_date.strftime("%Y-%m-%d")

    print(start_date,end_date,previous_start_date,previous_end_date)
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
    df1['Clicks']= df1['Direct_ATC']+df1['Indirect_ATC']
    df1['CPM'] = (df1['Estimated_Budget_Consumed'] / df1['Impressions']) * 1000
    df1['CTR'] = ((df1['Direct_ATC'] + df1['Indirect_ATC']) / df1['Impressions']) * 100
    df1['CVR'] = ((df1['Direct_Quantities_Sold'] + df1['Indirect_Quantities_Sold']) / (df1['Direct_ATC'] + df1['Indirect_ATC']) ) * 100
    df1['CPC'] = (df1['Estimated_Budget_Consumed'] / df1['Direct_ATC'])
    df1['AOV'] = ((df1['Direct_Sales'] + df1['Indirect_Sales']) / df1['Direct_Quantities_Sold'])
    df1['ACOS'] = (df1['Estimated_Budget_Consumed'] / df1['Direct_Sales'])
   
    try:
        df1['ROAS'] = df1['Direct_Sales'] / df1['Estimated_Budget_Consumed']
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
    df2['CPM'] = (df2['Estimated_Budget_Consumed'] / df2['Impressions']) * 1000
    df2['CTR'] = ((df2['Direct_ATC'] + df2['Indirect_ATC']) / df2['Impressions']) * 100
    df2['CVR'] = ((df2['Direct_Quantities_Sold'] + df2['Indirect_Quantities_Sold']) / (df2['Direct_ATC'] + df2['Indirect_ATC']) ) * 100
    df2['CPC'] = (df2['Estimated_Budget_Consumed'] / df2['Direct_ATC'])
    df2['AOV'] = ((df2['Direct_Sales'] + df1['Indirect_Sales']) / df2['Direct_Quantities_Sold'])
    df2['ACOS'] = (df2['Estimated_Budget_Consumed'] / df2['Direct_Sales'])
    
    try:
        df2['ROAS'] = df2['Direct_Sales'] / df2['Estimated_Budget_Consumed']
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

    merged_df.loc[merged_df['Campaign_Name'].str.contains('pa_liquids|pa liquids', case=False), 'Campaign_Tags'] = 'PA Liquids'
    merged_df.loc[merged_df['Campaign_Name'].str.contains('pa_deos|pa_deo|pa deos|pa deo', case=False), 'Campaign_Tags'] = 'PA Deos'
    merged_df.loc[merged_df['Campaign_Name'].str.contains('pa_edps|pa edps', case=False), 'Campaign_Tags'] = 'PA EDPs'
    merged_df.loc[merged_df['Campaign_Name'].str.contains('pa_gift_sets|pa gift sets|pa_edp_gift_set|pa edp gift set', case=False), 'Campaign_Tags'] = 'PA Gift Sets'

    merged_df.loc[merged_df['Campaign_Name'].str.contains('aer_aerosol|aer aerosol', case=False), 'Campaign_Tags'] = 'Aer Aerosol'
    merged_df.loc[merged_df['Campaign_Name'].str.contains('aer_pocket|aer pocket', case=False), 'Campaign_Tags'] = 'Aer Pocket'
    merged_df.loc[merged_df['Campaign_Name'].str.contains('aer_matic|aer matic', case=False), 'Campaign_Tags'] = 'Aer Matic'
    merged_df.loc[merged_df['Campaign_Name'].str.contains('aer_car|aer car', case=False), 'Campaign_Tags'] = 'Aer Car'

    merged_df.loc[merged_df['Campaign_Name'].str.contains('genteel', case=False), 'Campaign_Tags'] = 'Genteel'
    merged_df.loc[merged_df['Campaign_Name'].str.contains('ezee', case=False), 'Campaign_Tags'] = 'Ezee'
    merged_df.loc[merged_df['Campaign_Name'].str.contains('fab', case=False), 'Campaign_Tags'] = 'Fab'

    # Default 'Others'
    merged_df.loc[~merged_df['Campaign_Name'].str.contains(
        'pa_liquids|pa liquids|pa_deo|pa_deos|pa deos|pa deo|pa_edps|pa edps|pa_gift_sets|pa gift sets|pa_edps_gift_sets|pa edps gift sets|aer_aerosol|aer aerosol|aer_pocket|aer pocket|aer_matic|aer matic|aer_car|aer car|genteel|ezee|fab', 
        case=False, na=False), 'Campaign_Tags'] = 'Others'

    merged_df = merged_df.fillna('')

    # print(merged_df.columns)
    # exit()
    grouped_df = merged_df.groupby('Campaign_Tags').agg({
        'Estimated_Budget_Consumed_x': lambda x: f"{x.sum() / 1e6:.2f} M" if x.sum() >= 1e6 else (f"{x.sum() / 1e5:.2f} L" if x.sum() >= 1e5 else f"{x.sum() / 1e3:.2f} K"),
        'Total_Sales_x': lambda x: f"{x.sum() / 1e6:.2f} M" if x.sum() >= 1e6 else (f"{x.sum() / 1e5:.2f} L" if x.sum() >= 1e5 else f"{x.sum() / 1e3:.2f} K"),
        # 'Estimated_Budget_Consumed_x':'sum',
        # 'Total Sales_x':'sum',
        'CVR_x': 'mean',
        'CPC_x': 'mean',
        'ROAS_x': 'mean', ##
    })

    grouped_df = grouped_df.round(2)
    grouped_df.reset_index(inplace = True)

    grouped_df = grouped_df.drop(0)
    grouped_df = grouped_df.reset_index(drop=True)
    # print(grouped_df)
    # exit()
    merged_data = {}
    merged_data['category'] = grouped_df['Campaign_Tags'].values.tolist()
    merged_data['Spends'] = grouped_df['Estimated_Budget_Consumed_x'].values.tolist()
    merged_data['Sales'] = grouped_df['Total_Sales_x'].values.tolist()
    merged_data['CVR'] = grouped_df['CVR_x'].values.tolist()
    merged_data['CPC'] = grouped_df['CPC_x'].values.tolist()
    merged_data['ROAS'] = grouped_df['ROAS_x'].values.tolist() ##

    new_list = []
    for data in range(len(merged_data['Sales'])):
        new_list.append({ i:merged_data[i][data] for i in merged_data.keys()})

    # new_list = [human_format(i) if isinstance(i, float) else i for i in new_list]

    print('==============\n', new_list)

    # print(len(new_list))
    return new_list

# blnkt_cat_data()