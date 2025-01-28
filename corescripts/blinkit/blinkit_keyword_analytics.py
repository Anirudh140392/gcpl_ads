import cloudscraper
import pandas as pd
import numpy as np
from datetime import datetime,timedelta
# import datetime
import pymysql

def blnkt_kw_anlys(start_date='2025-01-26', end_date='2025-01-26'):
    print(start_date, end_date)

    start_date = datetime.strptime(start_date , "%Y-%m-%d")
    end_date = datetime.strptime(end_date , "%Y-%m-%d")

    previous_end_date = start_date - timedelta(days=1)
    previous_start_date = previous_end_date - timedelta(days=(end_date - start_date).days)
    
    start_date = start_date.strftime("%Y-%m-%d")
    end_date = end_date.strftime("%Y-%m-%d")

    previous_end_date = previous_end_date.strftime("%Y-%m-%d")
    previous_start_date = previous_start_date.strftime("%Y-%m-%d")

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

    def report(date1, date2):
        sql = f"SELECT * FROM blinkit_keywords_data_gcpl WHERE Date BETWEEN '{date1}' AND '{date2}' AND Targeting_Type = 'Keyword'"
        cursor.execute(sql)
        data = cursor.fetchall()
        columns = [i[0] for i in cursor.description]
        df = pd.DataFrame(data, columns = columns)
        df['Estimated_Budget_Consumed'] = df['Estimated_Budget_Consumed'].astype(float)
        df['Direct_Sales'] = df['Direct_Sales'].astype(float)
        df['Indirect_Sales'] = df['Indirect_Sales'].astype(float)
        df['Total_Sales'] = df['Direct_Sales'] + df['Indirect_Sales']
        df['Orders'] = df['Direct_Quantities_Sold'] + df['Indirect_Quantities_Sold']
        df['Direct_RoAS'] = df['Direct_RoAS'].astype(float)
        df['Total_RoAS'] = df['Total_RoAS'].astype(float)
        df['CPATC'] = df['Estimated_Budget_Consumed'] / (df['Direct_ATC'] + df['Indirect_ATC'])
        df['CR'] = ((df['Direct_ATC'] + df['Indirect_ATC']) / df['Impressions']) * 100
        df['CPA'] = df['Estimated_Budget_Consumed'] / df['New_Users_Acquired']
        df['ACoS'] = (df['Estimated_Budget_Consumed'] / df['Total_Sales']) * 100
        # print(df.dtypes)
        return df

    # df1 = report("2025-01-25", "2025-01-25")
    # df2 = report("2025-01-25", "2025-01-25")

    df1 = report(previous_start_date, previous_end_date)
    df2 = report(start_date, end_date)

    # print(df1)
    # print(df2)
    # exit()

    df1 = df1.groupby('Targeting_Value').agg({
        'Campaign_Name' : lambda x: ', '.join(x),
        'Match_Type' : 'first',
        'Impressions' : 'sum',
        'Estimated_Budget_Consumed': 'sum',
        'Direct_Sales': 'sum',
        'Total_Sales': 'sum',
        'Orders': 'sum',
        'Direct_ATC': 'sum',
        'Direct_RoAS': 'mean',
        'Total_RoAS': 'mean',
        'CPATC': 'mean',
        'CPM': 'mean',
        'CR': 'mean',
        'CPA' : 'mean',
        'ACoS': 'mean'
    })

    df1.reset_index(inplace = True)
    df1['Campaigns_Count'] = df1['Campaign_Name'].apply(lambda x: len(x.split(', ')))
    df1['Is_Exact'] = df1['Match_Type'].map({'EXACT': True, 'SMART': False})
    df1.replace(np.inf, 0, inplace = True)
    df1.replace(np.nan, 0, inplace = True)
    df1 = df1.round(2)

    df2 = df2.groupby('Targeting_Value').agg({
        'Campaign_Name' : lambda x: ', '.join(x),
        'Match_Type' : 'first',
        'Impressions' : 'sum',
        'Estimated_Budget_Consumed': 'sum',
        'Direct_Sales': 'sum',
        'Total_Sales': 'sum',
        'Orders': 'sum',
        'Direct_ATC': 'sum',
        'Direct_RoAS': 'mean',
        'Total_RoAS': 'mean',
        'CPATC': 'mean',
        'CPM': 'mean',
        'CR': 'mean',
        'CPA' : 'mean',
        'ACoS': 'mean'
    })

    df2.reset_index(inplace = True)
    df2['Campaigns_Count'] = df2['Campaign_Name'].apply(lambda x: len(x.split(', ')))
    df2['Is_Exact'] = df2['Match_Type'].map({'EXACT': True, 'SMART': False})
    df2.replace(np.inf, 0, inplace = True)
    df2.replace(np.nan, 0, inplace = True)
    df2 = df2.round(2)

    columns_to_include = ['Targeting_Value', 'Impressions', 'Estimated_Budget_Consumed', 'Direct_Sales', 'Total_Sales', 'Orders', 
                        'Direct_ATC', 'Direct_RoAS', 'Total_RoAS', 'CPATC', 'CPM', 'CR', 'CPA', 'ACoS']

    merged_df = pd.merge(df2, df1[columns_to_include], on = 'Targeting_Value', how = 'left')
    merged_df.replace(np.nan, 0, inplace = True)

    columns_to_subtract = ['Impressions', 'Estimated_Budget_Consumed', 'Direct_Sales', 'Total_Sales', 'Orders', 'Direct_ATC', 
                        'Direct_RoAS', 'Total_RoAS', 'CPATC', 'CPM', 'CR', 'CPA', 'ACoS']

    for column in columns_to_subtract:
        try:
            merged_df[f'{column}_diff'] = ((merged_df[f'{column}_x'] - merged_df[f'{column}_y']) / merged_df[f'{column}_y']) * 100
        except ZeroDivisionError:
            merged_df[f'{column}_diff'] = 0

    merged_df.replace(np.inf, 0, inplace = True)
    merged_df.replace(np.nan, 0, inplace = True)
    merged_df = merged_df.round(2)

    # DB_HOST = "tr-wp-database.cdq264akgdm2.us-east-1.rds.amazonaws.com"
    # DB_USER = "Python"
    # DB_PASSWORD = "Trailytics@789"
    # DB_DATABASE = "ey_ads_auto"
    # DB_PORT = 3306

    # connection = pymysql.connect(host = DB_HOST,
    #                             user = DB_USER,
    #                             password = DB_PASSWORD,
    #                             db = DB_DATABASE,
    #                             port = DB_PORT,
    #                             connect_timeout = 1000,
    #                             autocommit = True)

    # cursor = connection.cursor()

    def report(date1, date2):
        sql = f"""SELECT
        t1.keyword AS Targeting_Value,
        t1.MIN_POSITION AS 'Rank',
        t2.Average_Rank
        FROM
            (
                SELECT
                    keyword,
                    MIN(POSITION) AS MIN_POSITION
                FROM
                    grofers_crawl_kw
                WHERE
                    is_keyword_rb_product = 1
                    AND DATE(created_on) BETWEEN '{date1}' AND '{date2}'
                GROUP BY
                    keyword
            ) AS t1
        JOIN
            (
                SELECT
                    keyword,
                    SUM(MIN_POSITION) / COUNT(*) AS Average_Rank
                FROM
                    (
                        SELECT
                            keyword,
                            MIN(POSITION) AS MIN_POSITION
                        FROM
                            grofers_crawl_kw
                        WHERE
                            is_keyword_rb_product = 1
                            AND DATE(created_on) BETWEEN '{date1}' AND '{date2}'
                        GROUP BY
                            keyword, DATE(created_on)
                    ) AS daily_min_positions
                GROUP BY
                    keyword
            ) AS t2 ON t1.keyword = t2.keyword;"""
        
        cursor.execute(sql)
        data = cursor.fetchall()
        columns = [i[0] for i in cursor.description]
        df = pd.DataFrame(data, columns = columns)
        return df

    df1_kw = report("2025-01-26", "2025-01-26")
    df2_kw = report("2025-01-26", "2025-01-26")

    # df1_kw = report(previous_end_date, previous_start_date)
    # df2_kw = report(end_date, start_date)

    # print(df1_kw)
    # print(df2_kw)

    merged_df_kw = pd.merge(df2_kw, df1_kw, on = 'Targeting_Value', how = 'left')
    merged_df_kw.replace(np.nan, 0, inplace = True)

    merged_df_kw['Rank_diff'] = merged_df_kw['Rank_x'] - merged_df_kw['Rank_y']
    merged_df_kw['Average_Rank_diff'] = merged_df_kw['Average_Rank_x'] - merged_df_kw['Average_Rank_y']

    combined_df = pd.merge(merged_df, merged_df_kw, on = 'Targeting_Value', how = 'left')
    combined_df.replace(np.nan, 0, inplace = True)

    combined_df['Rank_x'] = combined_df['Rank_x'].astype(int)
    combined_df['Rank_y'] = combined_df['Rank_y'].astype(int)
    combined_df['Average_Rank_x'] = combined_df['Average_Rank_x'].astype(int)
    combined_df['Average_Rank_y'] = combined_df['Average_Rank_y'].astype(int)
    combined_df['Rank_diff'] = combined_df['Rank_diff'].astype(int)
    combined_df['Average_Rank_diff'] = combined_df['Average_Rank_diff'].astype(int)
    combined_df['Program_Type'] = 'Performance'
    combined_df['SP_IMPR_Rank'] = 0
    combined_df['IMPR_Percent_Share'] = 0
    combined_df['Overall_SOV'] = 0
    combined_df['Organic_SOV'] = 0
    combined_df['Top_Product'] = ''
    combined_df['Total_IMPR'] = 0
    combined_df['SOS'] = 0

    combined_df['Keyword_Class'] = np.where(combined_df['Targeting_Value'].str.contains('Godrej|Godrej Aer|Godrej Cartini|Godrej Ezee|Godrej Genteel|Godrej Professional|Godrej Protekt|Godrej Yummiez', case = False), 'Branded', 'Generic')

    # print(combined_df.columns)
    # print(combined_df)

    combined_df=combined_df[combined_df['Rank_x'] != 0]

    # print(combined_df)

    merged_data = {}
    merged_data['search_term'] = combined_df['Targeting_Value'].values.tolist()
    merged_data['campaign_count'] = combined_df['Campaigns_Count'].values.tolist()
    merged_data['is_exact'] = combined_df['Is_Exact'].values.tolist()
    merged_data['impressions'] = combined_df['Impressions_x'].values.tolist()
    merged_data['impressions_change'] = combined_df['Impressions_diff'].values.tolist()
    merged_data['spends'] = combined_df['Estimated_Budget_Consumed_x'].values.tolist()
    merged_data['spends_change'] = combined_df['Estimated_Budget_Consumed_diff'].values.tolist()
    merged_data['sales'] = combined_df['Direct_Sales_x'].values.tolist()
    merged_data['sales_change'] = combined_df['Direct_Sales_diff'].values.tolist()
    merged_data['cpatc'] = combined_df['CPATC_x'].values.tolist()
    merged_data['cpatc_change'] = combined_df['CPATC_diff'].values.tolist()
    merged_data['cpm'] = combined_df['CPM_x'].values.tolist()
    merged_data['cpm_change'] = combined_df['CPM_diff'].values.tolist()
    merged_data['total_ad_sales'] = (combined_df['Total_Sales_x']).values.tolist() 
    merged_data['total_ad_sales_change'] = (combined_df['Total_Sales_diff']).values.tolist()
    merged_data['troas'] = combined_df['Total_RoAS_x'].values.tolist()
    merged_data['troas_change'] = combined_df['Total_RoAS_diff'].values.tolist() 
    merged_data['roas'] = combined_df['Direct_RoAS_x'].values.tolist()
    merged_data['roas_change'] = combined_df['Direct_RoAS_diff'].values.tolist()
    merged_data['program_type'] = combined_df['Program_Type'].values.tolist()
    merged_data['acos'] = combined_df['ACoS_x'].values.tolist()
    merged_data['acos_change'] = combined_df['ACoS_diff'].values.tolist()
    merged_data['orders'] = combined_df['Orders_x'].values.tolist()
    merged_data['orders_change'] = combined_df['Orders_diff'].values.tolist()

    merged_data['overall_rank'] = combined_df['Rank_x'].values.tolist()
    # merged_data['SOS'] = combined_df['SOS_x'].values.tolist()
    merged_data['campaign_name'] = combined_df['Campaign_Name'].values.tolist()
    merged_data['keyword_class'] = combined_df['Keyword_Class'].values.tolist()

    new_list = []
    for data in range(len(merged_data['campaign_name'])):
        new_dict = {}
        for i in merged_data.keys():
            try:
                new_dict[i] = float(merged_data[i][data])
            except:
                if type(merged_data[i][data]) == list:
                    new_dict[i] = merged_data[i][data][0]
                else:
                    new_dict[i] = merged_data[i][data]
        new_list.append(new_dict)

    # print(new_list)
    return new_list

# blnkt_kw_anlys()