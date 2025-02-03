import json
import mysql.connector
import pandas as pd
from decimal import Decimal
from datetime import datetime, timedelta
import time
import numpy as np


def funnel_data(start_date, end_date):
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
        
    def get_data(start_date,end_date):
        query = f"""
            SELECT 
                SUM(category_spend) AS spends,
                SUM(category_revenue) AS sales,
                SUM(category_cpc) AS cpc,
                SUM(category_roas) AS roas,
                SUM(category_orders) AS orders,
                SUM(category_clicks) AS clicks,
                SUM(category_impressions) AS impressions,
                SUM(category_orders)/SUM(category_clicks)*100 AS cvr
            FROM 
                category_data_gcpl
            WHERE
                DATE(created_on) BETWEEN '{start_date}' AND '{end_date}'
            """

        cursor.execute(query)
        current_rows = cursor.fetchall()
        df = pd.DataFrame(current_rows)
        return df

    current_df = get_data(start_date.date(),end_date.date())

    metrics = ['spends', 'sales', 'cpc', 'roas', 'orders', 'clicks', 'impressions', 'cvr']

    for metric in metrics:
        current_df[metric] = current_df[metric].apply(lambda x: float(x) if isinstance(x, Decimal) else x)

    data = {}
    data["spends"] = current_df["spends"]
    data["sales"] = current_df["sales"]
    data["cpc"] = current_df["cpc"]
    data["roas"] = current_df["roas"]
    data["orders"] = current_df["orders"]
    data["clicks"] = current_df["clicks"]
    data["impressions"] = current_df["impressions"]
    data["cvr"] = current_df["cvr"]

    packet = []
    for i in range(len(data['spends'])):
        packet.append({ k:data[k][i] for k in data.keys() })

    return packet
    
funnel_data(start_date='2025-01-26', end_date='2025-01-27')