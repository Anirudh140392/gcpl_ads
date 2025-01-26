import requests
import json
import re
from bs4 import BeautifulSoup
import cloudscraper
import pandas as pd
import numpy as np
from datetime import datetime
import pymysql

def blinkit_godrej_keywords():
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

    sql = f"SELECT Campaign_ID, Campaign_Name, Campaign_Status, Campaign_Objective_Type, Campaign_Title, Targeting_Value, Type, CPM_Exact, CPM_Smart FROM blinkit_campaigns_data_gcpl WHERE (Date = '{today_date}' OR Date = DATE_SUB('{today_date}', INTERVAL 1 DAY)) AND Brand_Name = 'GCPL'"
    cursor.execute(sql)
    data = cursor.fetchall()
    columns = [i[0] for i in cursor.description]
    df = pd.DataFrame(data, columns = columns)

    df.rename(columns = {'Type': 'Targeting_Type'}, inplace = True)

    # print(df)

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

    df1 = report("2025-01-21", "2025-01-21")
    df2 = report("2025-01-22", "2025-01-22")

    columns = ['Date', 'Campaign_Name', 'Targeting_Type', 'Targeting_Value', 'CPM', 'Direct_Sales', 'Indirect_Sales', 
            'Estimated_Budget_Consumed', 'Direct_RoAS', 'Total_RoAS']

    df1 = df1[columns]
    df1['Total_Sales'] = df1['Direct_Sales'] + df1['Indirect_Sales']

    df1 = df1.groupby(['Campaign_Name', 'Targeting_Type', 'Targeting_Value']).agg({
        'Estimated_Budget_Consumed': 'sum',
        'CPM': 'mean',
        'Direct_Sales': 'sum',
        'Indirect_Sales': 'sum',
        'Total_Sales': 'sum',
        'Direct_RoAS': 'mean',
        'Total_RoAS': 'mean'
    })

    df1 = df1.round(2)
    df1.reset_index(inplace = True)

    # print(df1)

    df2 = df2[columns]
    df2['Total_Sales'] = df2['Direct_Sales'] + df2['Indirect_Sales']

    df2 = df2.groupby(['Campaign_Name', 'Targeting_Type', 'Targeting_Value']).agg({
        'Estimated_Budget_Consumed': 'sum',
        'CPM': 'mean',
        'Direct_Sales': 'sum',
        'Indirect_Sales': 'sum',
        'Total_Sales': 'sum',
        'Direct_RoAS': 'mean',
        'Total_RoAS': 'mean'
    })

    df2 = df2.round(2)
    df2.reset_index(inplace = True)

    # print(df2)

    merged_df = pd.merge(df2, df1, on = ['Campaign_Name', 'Targeting_Type', 'Targeting_Value'], how = 'left')
    merged_df.replace('null', np.nan, inplace = True)

    columns_list = ['Estimated_Budget_Consumed', 'Direct_Sales', 'Total_Sales', 'CPM', 'Direct_RoAS', 'Total_RoAS']

    for column in columns_list:
        try:
            merged_df[f'{column}_diff'] = ((merged_df[f'{column}_x'] - merged_df[f'{column}_y']) / merged_df[f'{column}_y']) * 100
        except ZeroDivisionError:
            merged_df[f'{column}_diff'] = 0

    merged_df.replace(np.inf, 0, inplace = True)
    merged_df.replace(np.nan, 0, inplace = True)
    # print(merged_df)

    combined_df = pd.merge(df, merged_df, on = ['Campaign_Name', 'Targeting_Value', 'Targeting_Type'], how = 'left')
    combined_df.replace(np.nan, 0, inplace = True)
    combined_df = combined_df.round(2)

    combined_df['Campaign_Tags'] = ''

    combined_df['Keyword_Class'] = np.where(combined_df['Targeting_Value'].str.contains('Godrej|godrej', case = False), 'Branded', 'Generic')

    combined_df['Switch'] = np.where(
        combined_df['Targeting_Type'] == 'Category', 'No_Switch',
        np.where((combined_df['Targeting_Type'] == 'Keyword') & (combined_df['CPM_Exact'] != 0) & (combined_df['CPM_Smart'] != 0), 'Switch_On', 'Switch_Off')
    )

    print(combined_df.columns)
    combined_df.to_excel('shshsh.xlsx')
    exit()

    merged_data = {}
    merged_data['campaign_id'] = combined_df['Campaign_ID'].values.tolist()
    merged_data['Placement_Type'] = combined_df['Targeting_Value'].values.tolist()
    merged_data['Absolute_Cost'] = combined_df['CPM_Exact'].values.tolist()

    merged_data['cpm_smart'] = combined_df['CPM_Smart'].values.tolist()
    merged_data['campaign_status'] = combined_df['Campaign_Status'].values.tolist()

    # merged_data['ad_group_name'] = combined_df['Type'].values.tolist() ##
    # spends
    merged_data['spends'] = combined_df['Estimated_Budget_Consumed_x'].values.tolist()
    merged_data['spends_change'] = combined_df['Estimated_Budget_Consumed_diff'].values.tolist()
    # sales
    merged_data['sales'] = combined_df['Direct_Sales_x'].values.tolist()## direct_revenue
    merged_data['sales_change'] = combined_df['Direct_Sales_diff'].values.tolist()
    # cpm
    merged_data['cpm'] = combined_df['CPM_x'].values.tolist()
    merged_data['cpm_change'] = combined_df['CPM_diff'].values.tolist()
    # TOTAL AD SALES
    merged_data['total_ad_sales'] = (combined_df['Total_Sales_x']).values.tolist() ## indirect revenue
    merged_data['total_ad_sales_change'] = (combined_df['Total_Sales_diff']).values.tolist()
    # troas
    merged_data['troas'] = combined_df['Total_RoAS_x'].values.tolist()
    merged_data['troas_change'] = combined_df['Total_RoAS_diff'].values.tolist() 
    # roas
    merged_data['roas'] = combined_df['Direct_RoAS_x'].values.tolist()
    merged_data['roas_change'] = combined_df['Direct_RoAS_diff'].values.tolist()
    # KEYWORD CLASS
    merged_data['keyword_class'] = combined_df['Keyword_Class'].values.tolist()
    # TARGET TYPE
    merged_data['type'] = combined_df['Campaign_Objective_Type'].values.tolist()

    merged_data['campaign_name'] = combined_df['Campaign_Name'].values.tolist()

    # merged_data['market_place'] = combined_df['platform_name'].values.tolist()

    merged_data['campaign_tags'] = combined_df['Campaign_Tags'].values.tolist()

    
    # print(merged_data.columns)
    
    new_list = []
    for data in range(len(merged_data['campaign_id'])):
        new_list.append({ i:merged_data[i][data] for i in merged_data.keys()})

    # print('==============', new_list)
    # print(len(new_list))
    cursor.close()
    connection.close()
    
    # df = pd.DataFrame(new_list)

    # Save to Excel

    # df.to_excel('abcd.xlsx', index=False, header=False)
    
    return new_list
blinkit_godrej_keywords()









