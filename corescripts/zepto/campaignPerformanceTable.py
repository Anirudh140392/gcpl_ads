import json
import mysql.connector
import pandas as pd
from decimal import Decimal
from datetime import datetime, timedelta
import time
import numpy as np


def cmp_perfm_table(start_date, end_date):
    connection = mysql.connector.connect(
        host="tr-wp-database.cdq264akgdm2.us-east-1.rds.amazonaws.com",
        port="3306",
        user="Python",
        password="Trailytics@7899",
        database="ey_ads_auto"
    )

    if connection.is_connected():
        print("Connected to MySQL database")
        
        cursor = connection.cursor(dictionary=True)  

        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.strptime(end_date, '%Y-%m-%d')        
        
        previous_end_date = start_date - timedelta(days=1)
        previous_start_date = previous_end_date - timedelta(days=(end_date - start_date).days)
        
        previous_start_date = previous_start_date.date()
        previous_end_date = previous_end_date.date()
        
        
    def get_data(start_date,end_date):
        query = f"""
            SELECT
                *
            FROM
                campaign_data_gcpl
            WHERE
                DATE(created_on) BETWEEN '{start_date}' AND '{end_date}'
            """

        cursor.execute(query)
        current_rows = cursor.fetchall()
        df = pd.DataFrame(current_rows)
        return df

    current_df = get_data(start_date.date(),end_date.date())
    previous_df = get_data(previous_start_date,previous_end_date)

        
    metrics =  [
            'campaign_atc', 'campaign_clicks', 'campaign_cpc',
            'campaign_cpm', 'campaign_impressions','campaign_ctr' ,'campaign_other_skus',
            'campaign_revenue', 'campaign_roas', 'campaign_same_skus', 'campaign_spend'
        ]
    for metric in metrics:
        current_df[f'{metric}_previous'] = previous_df[metric]

        current_df[metric] = current_df[metric].apply(lambda x: float(x) if isinstance(x, Decimal) else x)
        current_df[f'{metric}_previous'] = current_df[f'{metric}_previous'].apply(lambda x: float(x) if isinstance(x, Decimal) else x)
        def calculate_change(row, metric):
            current_value = row[metric]
            previous_value = row[f'{metric}_previous']
            if pd.notnull(current_value) and pd.notnull(previous_value) and previous_value != 0:
                return ((current_value - previous_value) / previous_value) * 100
            else:
                return 0.0
            
        current_df[f'{metric}_change'] = current_df.apply(calculate_change, axis=1, metric=metric)        
        
    current_df.reset_index(inplace=True)
    ordered_columns = ["created_on"]
    for metric in metrics:
        ordered_columns.append(metric)
        ordered_columns.append(f'{metric}_change')
    current_df = current_df[ordered_columns]        

        
    data = {}
    data["created_on"] = current_df["created_on"]

    data["atc"] = current_df["campaign_atc"]
    data["atc_change"] = current_df["campaign_atc_change"]

    data["clicks"] = current_df["campaign_clicks"]
    data["clicks_change"] = current_df["campaign_clicks_change"]

    data["cpc"] = current_df["campaign_cpc"]
    data["cpc_change"] = current_df["campaign_cpc_change"]

    data["cpm"] = current_df["campaign_cpm"]
    data["cpm_change"] = current_df["campaign_cpm_change"]

    data["ctr"] = current_df["campaign_ctr"]
    data["ctr_change"] = current_df["campaign_ctr_change"]

    data["impression"] = current_df["campaign_impressions"]
    data["impression_change"] = current_df["campaign_impressions_change"]

    data["other_skus"] = current_df["campaign_other_skus"]
    data["other_skus_change"] = current_df["campaign_other_skus_change"]

    data["revenue"] = current_df["campaign_revenue"]
    data["revenue_change"] = current_df["campaign_revenue_change"]

    data["roas"] = current_df["campaign_roas"]
    data["roas_change"] = current_df["campaign_roas_change"]

    data["same_skus"] = current_df["campaign_same_skus"]
    data["same_skus_change"] = current_df["campaign_same_skus_change"]

    data["spend"] = current_df["campaign_spend"]
    data["spend_change"] = current_df["campaign_spend_change"]

        
    packet = []
    for i in range(len(data['created_on'])):
        packet.append({ k:data[k][i] for k in data.keys() })

    return packet

    
cmp_perfm_table(start_date='2025-01-26', end_date='2025-01-27')