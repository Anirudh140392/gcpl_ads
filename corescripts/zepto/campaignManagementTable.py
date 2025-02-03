import json
import mysql.connector
import pandas as pd
from decimal import Decimal
from datetime import datetime, timedelta
import time
import numpy as np


def cmp_mngt_table(start_date, end_date):
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
            SELECT *
            FROM 
                zepto_campaign_data_gcpl
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
            "spend", "impressions", "ecpm", "roi", "clicks", "cpc", 
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
    ordered_columns = ["campaign_name", "campaign_type", "campaign_sub_type", "created_on", 
                       "bid_targeting_type", "status", "start_date", "end_date",
                       "lifetime_budget", "base_bid", "daily_budget", "campaign_id"
                       ]
    for metric in metrics:
        ordered_columns.append(metric)
        ordered_columns.append(f'{metric}_change')
    current_df = current_df[ordered_columns]

        
    data = {}
    data["campaign_name"] = current_df["campaign_name"]
    data["campaign_type"] = current_df["campaign_type"]
    data["campaign_sub_type"] = current_df["campaign_sub_type"]
    data["created_on"] = current_df["created_on"]
    data["bid_targeting_type"] = current_df["bid_targeting_type"]
    data["status"] = current_df["status"]
    data["start_date"] = current_df["start_date"]
    data["end_date"] = current_df["end_date"]
    data["lifetime_budget"] = current_df["lifetime_budget"]
    data["base_bid"] = current_df["base_bid"]
    data["campaign_id"] = current_df["campaign_id"]
    data["daily_budget"] = current_df["daily_budget"]

    data["spend"] = current_df["spend"]
    data["spend_change"] = current_df["spend_change"]

    data["impression"] = current_df["impressions"]
    data["impression_change"] = current_df["impressions_change"]

    data["ecpm"] = current_df["ecpm"]
    data["ecpm_change"] = current_df["ecpm_change"]

    data["roi"] = current_df["roi"]
    data["roi_change"] = current_df["roi_change"]

    data["clicks"] = current_df["clicks"]
    data["clicks_change"] = current_df["clicks_change"]

    data["cpc"] = current_df["cpc"]
    data["cpc_change"] = current_df["cpc_change"]
    
    packet = []
    for i in range(len(data['created_on'])):
        packet.append({ k:data[k][i] for k in data.keys() })

    return packet

    
cmp_mngt_table(start_date='2025-01-26', end_date='2025-01-27')