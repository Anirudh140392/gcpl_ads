import json
import mysql.connector
import pandas as pd
from decimal import Decimal
from datetime import datetime, timedelta
import time
import numpy as np


def keyword_table(start_date, end_date):
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
                keyword_data_gcpl
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
            "keyword_atc", "keyword_clicks", "keyword_cpc", "keyword_ctr", "keyword_cpm", 
            "keyword_impressions", "keyword_other_skus", "keyword_revenue", "keyword_roas", 
            "keyword_same_skus", "keyword_spend", "keyword_orders"
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
    ordered_columns = ["keyword_match_type", "keyword_name", "created_on"]
    for metric in metrics:
        ordered_columns.append(metric)
        ordered_columns.append(f'{metric}_change')
    current_df = current_df[ordered_columns]        

        
    data = {}
    data["match_type"] = current_df["keyword_match_type"]
    data["keyword_name"] = current_df["keyword_name"]
    data["created_on"] = current_df["created_on"]

    data["atc"] = current_df["keyword_atc"]
    data["atc_change"] = current_df["keyword_atc_change"]

    data["clicks"] = current_df["keyword_clicks"]
    data["clicks_change"] = current_df["keyword_clicks_change"]

    data["cpc"] = current_df["keyword_cpc"]
    data["cpc_change"] = current_df["keyword_cpc_change"]

    data["cpm"] = current_df["keyword_cpm"]
    data["cpm_change"] = current_df["keyword_cpm_change"]

    data["ctr"] = current_df["keyword_ctr"]
    data["ctr_change"] = current_df["keyword_ctr_change"]

    data["impression"] = current_df["keyword_impressions"]
    data["impression_change"] = current_df["keyword_impressions_change"]

    data["order"] = current_df["keyword_orders"]
    data["orders_change"] = current_df["keyword_orders_change"]

    data["other_skus"] = current_df["keyword_other_skus"]
    data["other_skus_change"] = current_df["keyword_other_skus_change"]

    data["revenue"] = current_df["keyword_revenue"]
    data["revenue_change"] = current_df["keyword_revenue_change"]

    data["roas"] = current_df["keyword_roas"]
    data["roas_change"] = current_df["keyword_roas_change"]

    data["same_skus"] = current_df["keyword_same_skus"]
    data["same_skus_change"] = current_df["keyword_same_skus_change"]

    data["spend"] = current_df["keyword_spend"]
    data["spend_change"] = current_df["keyword_spend_change"]

        
    packet = []
    for i in range(len(data['created_on'])):
        packet.append({ k:data[k][i] for k in data.keys() })

    
    return packet

    
keyword_table(start_date='2025-01-26', end_date='2025-01-27')