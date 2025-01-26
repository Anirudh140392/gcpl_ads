from django.core.cache import cache
from django.shortcuts import render, redirect
from django.views import View
import operator
import os
import json
import datetime as DT
import subprocess
from django.http import JsonResponse
from rest_framework_simplejwt.tokens import RefreshToken
from rest_framework.response import Response
from rest_framework.decorators import api_view
from rest_framework import status
from django.views.decorators.csrf import csrf_exempt

#import locale

import requests
import os

#    Authenticator     #

from django.contrib import messages
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from .models import *


#Blinkit
from corescripts.blinkit.blinkit_campaign_data import *
from corescripts.blinkit.blinkit_category_table import *
from corescripts.blinkit.blinkit_keyword_analytics import *
from corescripts.blinkit.blinkit_keyword_data import *
from corescripts.blinkit.blinkit_negative_keyword_data import *
from corescripts.blinkit.blinkit_overview_data import *
from corescripts.blinkit.blinkit_product_analytics_data import *
from corescripts.blinkit.blinkit_totals_data import *

row_counts = {'campagins_c': None,
			  'portfolio_c': None,
			  'adgroups_c': None,
			  'keyword_c': None,
			  'product_c': None,
			  'keyword_analysis_c':None,
			  'product_analysis_c':None,
			  'negative_keyword_c':None,
			  'recommendations_c':None,
			  'history_c':None,
}


def logout_view(request):
	logout(request)
	return redirect('login_page')

def login_page(request):
    # Check if the HTTP request method is POST (form submission)
    if request.user != 'AnonymousUser':
    	print(request.user)

    	    
    if request.method == "POST":
        username = request.POST.get('username')
        password = request.POST.get('password')
         
        if not User.objects.filter(username=username).exists():
            messages.error(request, 'Invalid Username')
            return redirect('/login/')

        user = authenticate(username=username, password=password)
         
        if user is None:
            messages.error(request, "Invalid Password")
            return redirect('/login/')
        else:
            login(request, user)
            return redirect('/blinkit/')

    return render(request, 'login.html')

def register_page(request):
    # Check if the HTTP request method is POST (form submission)
    if request.method == 'POST':
        first_name = request.POST.get('first_name')
        last_name = request.POST.get('last_name')
        username = request.POST.get('username')
        password = request.POST.get('password')
         
        # Check if a user with the provided username already exists
        user = User.objects.filter(username=username)
         
        if user.exists():
            # Display an information message if the username is taken
            messages.info(request, "Username already taken!")
            return redirect('/register/')
         
        # Create a new User object with the provided information
        user = User.objects.create_user(
            first_name=first_name,
            last_name=last_name,
            username=username
        )
         
        # Set the user's password and save the user object
        user.set_password(password)
        user.save()
         
        # Display an information message indicating successful account creation
        messages.info(request, "Account created Successfully!")
        return redirect('/register/')
     
    # Render the registration page template (GET request)
    return render(request, 'register.html')

def enter(request):
	return redirect('login_page')


@login_required(login_url="/")
def blnkt_home(request):
    start_date, end_date = str(DT.date.today() - DT.timedelta(days=7)), str(DT.date.today() - DT.timedelta(days=1))
    
    request.session["platform"] = "Blinkit"
    request.session["wallet_balance"] = "N/A"
    
    global row_counts
    for key in row_counts.keys():
        row_counts[key] = '?'
    
    if request.POST:
        print("form submitted")
        inpu_date = request.POST.get('dates', None)
        start_date, end_date = tuple(inpu_date.split('/'))
        print(start_date, end_date)

    blnktData = {'date': ['2024-05-01', '2024-05-02', '2024-05-03', '2024-05-04', '2024-05-05', '2024-05-06', '2024-05-07'], 
            'ads_pend': ['149395.8700', '212355.3500', '184596.5800', '171908.3500', '159957.8100', '152096.4000', '120481.5300'], 
            'impressions': ['576953', '914770', '905676', '754497', '663202', '718199', '651973'], 
            'clicks': ['13983', '21807', '19992', '18028', '17182', '16393', '14606'], 
            'ctr': ['0.0242', '0.0238', '0.0221', '0.0239', '0.0259', '0.0228', '0.0224'], 
            'cdcu': ['1779', '3969', '2993', '2477', '2331', '2182', '1513'], 
            'cicu': ['590', '1433', '889', '826', '676', '739', '508'], 
            'cvr': ['0.1694', '0.2477', '0.1942', '0.1832', '0.1750', '0.1782', '0.1384'], 
            'cdcr': ['582549.0000', '1186230.0000', '851869.0000', '696546.0000', '657842.0000', '613704.0000', '436903.0000'], 
            'cicr': ['181154.0000', '429206.0000', '262953.0000', '235166.0000', '190096.0000', '209814.0000', '143184.0000'], 
            'roi': ['1.2126', '2.0212', '1.4245', '1.3680', '1.1884', '1.3795', '1.1884']}
    
    start_date='2025-01-21'
    end_date='2025-01-22'

    blnktData = blnkt_ov_data(start_date, end_date)

    # totals=
    blnktData['totals'] = blnkt_overview_funnel_data(start_date, end_date)
    
    platf = request.session['platform']
    bal = request.session['wallet_balance']
    # totals =  Totals_generator() 
    # blnktData['totals'] = totals
    o_s = DT.datetime.strptime(start_date, '%Y-%m-%d')
    o_e = DT.datetime.strptime(end_date, '%Y-%m-%d')
    o_d = o_e - o_s
    
    cat_table = blnkt_cat_data(start_date, end_date) # working
	# cat_table = catagorial_data()
    fk_rule = [{}]
    return render(request, "Home_new.html", { 'username':request.user, 'cat_data':cat_table, 'pf_op':platf, 'balance':bal, 'data':blnktData, 'fk_rule_data': fk_rule,  'DATE':(start_date, end_date), 'O_date':(o_s, o_e, o_d.days+1) })



@login_required(login_url="/")
def Campagins(request):

	filter_list = { 'string_filter' : ['Campaign', 'Campaign Type', 'Campaign Tags', 'Market Place', 'Campaign ID'],
                    'metrics_filter': ['Impressions', 'Clicks', 'Spends', 'Sales', 'CTR', 'Total Ad Sales', 'TROAS', 'Orders', 'ROAS'],
                  }
	start_date, end_date = str(DT.date.today() - DT.timedelta(days=7)), str(DT.date.today() - DT.timedelta(days=1))

	platf = request.session['platform']
	global row_counts

	if platf == 'Blinkit' :
		# bal = request.session["wallet_balance"]
		bal = request.session["wallet_balance"]
		filter_list = { 'string_filter' : ['Campaign', 'Campaign Type', 'Campaign Tags', 'Market Place', 'Campaign ID'],
										'metrics_filter': ['Impressions', 'Spends', 'Sales', 'Total Ad Sales', 'TROAS', 'Direct ATC', 'Orders', 'CPM', 'ROAS'],
					}
		
		start_date='2025-01-22'
		end_date='2025-01-22'

		if request.POST:
			inpu_date = request.POST.get('dates', None)
			if inpu_date == None:
				end_date = DT.date.today() - DT.timedelta(days=1)
				start_date = DT.date.today() - DT.timedelta(days=7)
				start_date_str = start_date.strftime("%Y-%m-%d")
				end_date_str = end_date.strftime("%Y-%m-%d")
				inpu_date = f"{start_date_str}/{end_date_str}"

			start_date, end_date = tuple(inpu_date.split('/'))	

		# try:
		blnktdata = blinkit_campaign(start_date, end_date)
		# except Exception as e:
		# 	pass
		row_counts['campagins_c'] = len(blnktdata)

		o_s = DT.datetime.strptime(start_date, '%Y-%m-%d')
		o_e = DT.datetime.strptime(end_date, '%Y-%m-%d')
		o_d = o_e - o_s

		return render(request, "blinkit/blnkt_campaign.html", {'campaign':blnktdata, 'filters':filter_list, 'username':request.user, 'nums':row_counts, 'balance': bal, 'pf_op':platf, 'data':[], 'DATE':(start_date, end_date), 'O_date':(o_s, o_e, o_d.days+1) })

		

@login_required(login_url="/")
def keywords(request):
	# if 'Dates' not in request.session:
	# 	request.session['Dates'] = (str(DT.date.today() - DT.timedelta(days=7)), str(DT.date.today() - DT.timedelta(days=1)))
	start_date, end_date = str(DT.date.today() - DT.timedelta(days=7)), str(DT.date.today() - DT.timedelta(days=1))
	start_date='2024-11-15'
	end_date='2024-11-21'

	filter_list = { 'string_filter' : ['Target', 'Ad Group', 'Campaign', 'Campaign Type', 'Campaign Tags', 'Market Place', 'Campaign ID'],
                    'metrics_filter': ['Spends', 'Sales', 'CTR', 'Total Ad Sales', 'TROAS', 'ROAS'],
                  }

	platf = request.session['platform']
	bal = request.session["wallet_balance"]
	q1 , q2 = "", ""
	pre_filt = None
	global row_counts

	
	if platf == 'Blinkit':
		start_date, end_date = str(DT.date.today() - DT.timedelta(days=7)), str(DT.date.today() - DT.timedelta(days=1))

		filter_list = { 'string_filter' : ['Target', 'Campaign', 'Campaign Type'],
                    'metrics_filter': ['Spends', 'Sales', 'Total Ad Sales', 'TROAS', 'ROAS'],
                  }
		keyword = [
							{'Placement_Type':'body lotion', 'campaign_id': '8VN5DXLKJN8A', 'campaign_name': 'Body Lotion (summer) NCR PPB', 'Absolute_Cost':1339, 'keyword_type':'Keyword',  'spends': 16793.9, 'spends_change': 19.2, 'sales': 75149, 'sales_change': 5.1, 'total_ad_sales': 120032, 'total_ad_sales_change': 12.6, 'troas': 7.15, 'troas_change': 5.5, 'cpm': 2025.8, 'cpm_change': 20.7, 'roas': 2.9, 'roas_change': -9.9, 'keyword_class': 'Branded', 'type':'performance', 'campaign_tags': 'body lotion', 'market_place': 'Blinkit'}, 
							{'Placement_Type':'roll on', 'campaign_id': '8VN5DXLKJN8A', 'campaign_name': 'Roll on Women NCR PPB', 'Absolute_Cost':2031, 'keyword_type':'Keyword',  'spends': 16296.4, 'spends_change': -9.8, 'sales':84705, 'sales_change': 2.5, 'total_ad_sales': 89445, 'total_ad_sales_change': 1.9, 'troas': 5.49, 'troas_change': -6.01, 'cpm':1277.76, 'cpm_change': 5, 'roas': 8.1, 'roas_change': 12.8, 'keyword_class': 'Generic', 'type':'performance', 'campaign_tags': 'roll on', 'market_place': 'Blinkit'}, 
							{'Placement_Type':'Face wash & Scrub', 'campaign_id': '8VN5DXLKJN8A', 'campaign_name': 'Men Face Wash NCR PPBCR PPB', 'Absolute_Cost':245, 'keyword_type':'Category',  'spends': 15374, 'spends_change': 27.9, 'sales': 47854, 'sales_change': 84.4, 'total_ad_sales': 73320, 'total_ad_sales_change': 11.3, 'troas': 4.77, 'troas_change': 23, 'cpm': 267.35, 'cpm_change': -9.8, 'roas': 4.5, 'roas_change': 30.9, 'keyword_class': 'Branded', 'type':'performance', 'campaign_tags': 'Face wash', 'market_place': 'Blinkit'},
							{'Placement_Type':'body wash', 'campaign_id': '8VN5DXLKJN8A', 'campaign_name': 'Body wash Women NCR PPB', 'Absolute_Cost':500.00, 'keyword_type':'Keyword',  'spends': 13273.3, 'spends_change':-0.2, 'sales': 25028, 'sales_change': -16.4, 'total_ad_sales': 28633, 'total_ad_sales_change': 46.5, 'troas': 2.16, 'troas_change': 54, 'cpm': 1519.37, 'cpm_change': 49, 'roas': 4.4, 'roas_change': 15.2, 'keyword_class': 'Branded', 'type':'performance', 'campaign_tags': 'body wash', 'market_place': 'Blinkit'}, 
							{'Placement_Type':'roll on', 'campaign_id': '8VN5DXLKJN8A', 'campaign_name': 'Roll on Women Others PPB', 'Absolute_Cost':500.00, 'keyword_type':'Category',  'spends': 7480.6, 'spends_change': 10.2, 'sales': 8905, 'sales_change': -3.6, 'total_ad_sales': 10094, 'total_ad_sales_change': -7.6, 'troas': 1.5, 'troas_change': 17, 'cpm': 1549.04, 'cpm_change': 20.1, 'roas': 3.5, 'roas_change': 8.5, 'keyword_class': 'Generic', 'type':'performance', 'campaign_tags': 'roll on', 'market_place': 'Blinkit'}, 
							{'Placement_Type':'Face wash & Scrub', 'campaign_id': '8VN5DXLKJN8A', 'campaign_name': 'Men Face Wash Metro 1 PPB', 'Absolute_Cost':2693, 'keyword_type':'Keyword',  'spends': 7459.3, 'spends_change': 4.8, 'sales': 9698, 'sales_change': -1.2, 'total_ad_sales': 11205, 'total_ad_sales_change': 13.5, 'troas': 4.6, 'troas_change': 6.9, 'cpm': 2672.71, 'cpm_change': -0.1, 'roas': 6.6, 'roas_change': -17.8, 'keyword_class': 'Generic', 'type':'performance', 'campaign_tags': 'Face wash', 'market_place': 'Blinkit'}, 
							{'Placement_Type':'men deodorant', 'campaign_id': '8VN5DXLKJN8A', 'campaign_name': 'Deo Men NCR PPB', 'Absolute_Cost':200, 'keyword_type':'Keyword',  'spends': 6506.6, 'spends_change': -1.6, 'sales': 27828, 'sales_change': 60.9, 'total_ad_sales': 11057, 'total_ad_sales_change': 4, 'troas': 6.36, 'troas_change': -1.1, 'cpm': 864.9, 'cpm_change': 4.5, 'roas': 1.3, 'roas_change': 18.81, 'keyword_class': 'Generic', 'type':'performance', 'campaign_tags': 'deo', 'market_place': 'Blinkit'}, 
							{'Placement_Type':'women deodorant', 'campaign_id': '8VN5DXLKJN8A', 'campaign_name': 'Deo Women NCR PPB', 'Absolute_Cost':1474, 'keyword_type':'Category',  'spends': 4831.8, 'spends_change': 19.6, 'sales': 19641, 'sales_change':22, 'total_ad_sales': 30724, 'total_ad_sales_change': 18.4, 'troas': 5.72, 'troas_change': 11.3, 'cpm': 630.02, 'cpm_change': 5, 'roas': 1.91, 'roas_change': -27.2, 'keyword_class': 'Branded', 'type':'performance', 'campaign_tags': 'deo', 'market_place': 'Blinkit'}, 
							{'Placement_Type':'Nivea', 'campaign_id': '8VN5DXLKJN8A', 'campaign_name': 'Branded Nivea Creme women Metro 2 PPB', 'Absolute_Cost':1386, 'keyword_type':'Keyword',  'spends': 4173.8, 'spends_change': -2.2, 'sales': 17686, 'sales_change':29, 'total_ad_sales': 21513, 'total_ad_sales_change': 13.2, 'troas': 5.81, 'troas_change': 4.2, 'cpm': 317.54, 'cpm_change': 1.3, 'roas': 5, 'roas_change': -11.1, 'keyword_class': 'Branded', 'type':'performance', 'campaign_tags': 'Moisturiser', 'market_place': 'None'}]
		
		if request.POST:
			inpu_date = request.POST.get('dates', None)
			if inpu_date == None:
				end_date = DT.date.today() - DT.timedelta(days=1)
				start_date = DT.date.today() - DT.timedelta(days=7)
				start_date_str = start_date.strftime("%Y-%m-%d")
				end_date_str = end_date.strftime("%Y-%m-%d")
				inpu_date = f"{start_date_str}/{end_date_str}"
			
				# placement_type = request.POST.get('para2', None)
				# bid_value_tabuj = request.POST.get('para3', None)
				# bid_campaign_id = request.POST.get('para4', None)
				# bid_target_type = request.POST.get('para1', None)
				# print( "Tanuj vales are ",placement_type,bid_value_tabuj,bid_campaign_id,bid_target_type )
			start_date, end_date = tuple(inpu_date.split('/'))	

		start_date='2025-01-22'
		end_date='2025-01-24'
		keyword = blinkit_godrej_keywords(start_date, end_date)
	

		row_counts['keyword_c'] = len(keyword)
		o_s = DT.datetime.strptime(start_date, '%Y-%m-%d')
		o_e = DT.datetime.strptime(end_date, '%Y-%m-%d')
		o_d = o_e - o_s

		return render(request, "blinkit/blinkt_keyword.html", {'keyword': keyword, 'username':request.user, 'nums':row_counts, 'filters':filter_list, 'pf_op':platf, 'balance': bal, 'DATE':(start_date, end_date), 'O_date':(o_s, o_e, o_d.days+1) })



@login_required(login_url="/")
def Rule(request):
	filter_list = { 'string_filter' : ['portfolio' ],
                    'metrics_filter': ['Impressions','Clicks', 'Spends', 'orders', 'Sales', 'CTR', 'ACoS'],
                  }
	platf = request.session['platform']
	bal = request.session["wallet_balance"]
	
	if platf =='Blinkit':
			# blnkt_rule = blnkt_rules()
			# limit_type = [rule.get('limit_type', '') for rule in blnkt_rule]
		blnkt_rule=[]
		limit_type=[]
		return render(request, "rules.html", { 'balance': bal, 'filters':filter_list,  'pf_op':platf, 'fk_rule_data': blnkt_rule, 'username':request.user,'limit_type':limit_type, })


@login_required(login_url="/")
def keywordAnalytics(request):
	filter_list = { 'string_filter' : ['Campaign', 'Campaign Type'],
                    'metrics_filter': ['Impressions', 'Clicks', 'Spends', 'Sales', 'CTR', 'Total Ad Sales', 'TROAS'],
                  }
	start_date, end_date = str(DT.date.today() - DT.timedelta(days=7)), str(DT.date.today() - DT.timedelta(days=1))
	platf = request.session['platform']
	global row_counts
	
	if platf == 'Blinkit':
		bal = request.session["wallet_balance"]
		filter_list = { 'string_filter' : ['Seach Term' ],
										'metrics_filter': ['Is Exact','Impressions', 'Spends', 'CPATC', 'Sales', 'CPM', 'Total AD Sales', 'TROAS', 'Program Type', 'ROAS'],
									}
		
		kw_anlytics_data = [{'search_term': 'nivea', 'campaign_count': 50.0, 'is_exact': 1.0, 'impressions': 206402.0, 'impressions_change': 26.94, 'spends': 123903.1, 'spends_change': 27.0, 'sales': 331499.0, 'sales_change': 21.97, 'cpatc': 5.23, 'cpatc_change': -7.76, 'cpm': 587.5, 'cpm_change': 1.47, 'total_ad_sales': 915267.0, 'total_ad_sales_change': 22.82, 'troas': 6.82, 'troas_change': 7.91, 'roas': 2.5, 'roas_change': 0.81, 'program_type': 'Performance', 'acos': 16.5, 'acos_change': -13.57, 'orders': 19971.0, 'orders_change': 23.84, }, 
		{'search_term': 'body wash', 'campaign_count': 42.0, 'is_exact': 1.0, 'impressions': 19311.0, 'impressions_change': 15.96, 'spends': 9402.5, 'spends_change': 16.9, 'sales': 36663.0, 'sales_change': 8.85, 'cpatc': 8.3, 'cpatc_change': -3.4, 'cpm': 478.88, 'cpm_change': 1.35, 'total_ad_sales': 124243.0, 'total_ad_sales_change': 12.66, 'troas': 12.41, 'troas_change': 2.73, 'roas': 4.13, 'roas_change': -15.37, 'program_type': 'Performance', 'acos': 0.0, 'acos_change': 0.0, 'orders': 2729.0, 'orders_change': 12.49, }, 
		{'search_term': 'roll on', 'campaign_count': 22.0, 'is_exact': 1.0, 'impressions': 18492.0, 'impressions_change': 38.18, 'spends': 9053.5, 'spends_change': 39.18, 'sales': 25391.0, 'sales_change': 26.42, 'cpatc': 3.97, 'cpatc_change': 0.25, 'cpm': 491.64, 'cpm_change': 1.89, 'total_ad_sales': 90393.0, 'total_ad_sales_change': 29.46, 'troas': 10.12, 'troas_change': -1.17, 'roas': 2.86, 'roas_change': -0.69, 'program_type': 'Performance', 'acos': 11.16, 'acos_change': -2.19, 'orders': 1817.0, 'orders_change': 30.72, }, 
		{'search_term': 'body lotion', 'campaign_count': 17.0, 'is_exact': 1.0, 'impressions': 10658.0, 'impressions_change': 12.38, 'spends': 7509.8, 'spends_change': 13.14, 'sales': 18345.0, 'sales_change': -2.08, 'cpatc': 9.58, 'cpatc_change': 27.06, 'cpm': 680.18, 'cpm_change': 1.77, 'total_ad_sales': 42558.0, 'total_ad_sales_change': 5.08, 'troas': 4.95, 'troas_change': -21.8, 'roas': 1.83, 'roas_change': -25.31, 'program_type': 'Performance', 'acos': 23.45, 'acos_change': 38.02, 'orders': 700.0, 'orders_change': 5.74, }, 
		{'search_term': 'men deodorant', 'campaign_count': 42.0, 'is_exact': 1.0, 'impressions': 15548.0, 'impressions_change': 22.08, 'spends': 6760.6, 'spends_change': 22.16, 'sales': 26597.0, 'sales_change': 7.43, 'cpatc': 4.12, 'cpatc_change': 0.0, 'cpm': 422.21, 'cpm_change': 1.13, 'total_ad_sales': 87503.0, 'total_ad_sales_change': 6.78, 'troas': 11.59, 'troas_change': -11.59, 'roas': 4.01, 'roas_change': -21.83, 'program_type': 'Performance', 'acos': 0.0, 'acos_change': 0.0, 'orders': 1952.0, 'orders_change': 7.19, }, 
		{'search_term': 'women deodorant', 'campaign_count': 22.0, 'is_exact': 1.0, 'impressions': 11107.0, 'impressions_change': 10.57, 'spends': 6082.8, 'spends_change': 10.78, 'sales': 89500.0, 'sales_change': 11.1, 'cpatc': 2.44, 'cpatc_change': -29.48, 'cpm': 546.14, 'cpm_change': 2.08, 'total_ad_sales': 114493.0, 'total_ad_sales_change': 14.96, 'troas': 18.43, 'troas_change': 23.03, 'roas': 13.99, 'roas_change': 24.47, 'program_type': 'Performance', 'acos': 6.43, 'acos_change': -26.93, 'orders': 2108.0, 'orders_change': 14.57, }, 
		{'search_term': 'Others', 'campaign_count': 5.0, 'is_exact': 1.0, 'impressions': 9426.0, 'impressions_change': -20.46, 'spends': 2827.8, 'spends_change': -20.46, 'sales': 14670.0, 'sales_change': -30.64, 'cpatc': 3.7, 'cpatc_change': 23.75, 'cpm': 300.0, 'cpm_change': 0.0, 'total_ad_sales': 18725.0, 'total_ad_sales_change': -31.66, 'troas': 6.61, 'troas_change': -13.71, 'roas': 5.13, 'roas_change': -13.05, 'program_type': 'Performance', 'acos': 15.24, 'acos_change': 13.82, 'orders': 419.0, 'orders_change': -31.98, }, 
		{'search_term': 'body', 'campaign_count': 5.0, 'is_exact': 1.0, 'impressions': 3902.0, 'impressions_change': -21.96, 'spends': 1916.2, 'spends_change': -21.95, 'sales': 29680.0, 'sales_change': -17.65, 'cpatc': 1.53, 'cpatc_change': 0.66, 'cpm': 491.0, 'cpm_change': 0.0, 'total_ad_sales': 31991.0, 'total_ad_sales_change': -16.1, 'troas': 16.34, 'troas_change': 7.36, 'roas': 15.21, 'roas_change': 5.48, 'program_type': 'Performance', 'acos': 6.2, 'acos_change': -6.91, 'orders': 795.0, 'orders_change': -16.32, }, 
		{'search_term': 'cream', 'campaign_count': 17.0, 'is_exact': 1.0, 'impressions': 3967.0, 'impressions_change': 54.42, 'spends': 1802.1, 'spends_change': 55.73, 'sales': 27788.0, 'sales_change': 58.73, 'cpatc': 1.69, 'cpatc_change': -6.11, 'cpm': 439.35, 'cpm_change': -0.15, 'total_ad_sales': 35139.0, 'total_ad_sales_change': 58.12, 'troas': 18.33, 'troas_change': 2.8, 'roas': 12.46, 'roas_change': 1.71, 'program_type': 'Performance', 'acos': 5.95, 'acos_change': -6.15, 'orders': 853.0, 'orders_change': 57.67, }, 
		{'search_term': 'nivea body lotion', 'campaign_count': 31.0, 'is_exact': 1.0, 'impressions': 2497.0, 'impressions_change': 26.75, 'spends': 1406.7, 'spends_change': 29.55, 'sales': 3710.0, 'sales_change': -6.67, 'cpatc': 0.30, 'cpatc_change': 0.0, 'cpm': 540.87, 'cpm_change': 2.7, 'total_ad_sales': 4985.0, 'total_ad_sales_change': -19.75, 'troas': 2.48, 'troas_change': -54.58, 'roas': 1.5, 'roas_change': -50.0, 'program_type': 'Performance', 'acos': 0.0, 'acos_change': 0.0, 'orders': 111.0, 'orders_change': -18.98, }, 
		{'search_term': 'nivea body wash', 'campaign_count': 5.0, 'is_exact': 1.0, 'impressions': 3555.0, 'impressions_change': -19.11, 'spends': 1066.5, 'spends_change': -19.11, 'sales': 27040.0, 'sales_change': -19.81, 'cpatc': 0.98, 'cpatc_change': 6.52, 'cpm': 300.0, 'cpm_change': 1.0, 'total_ad_sales': 28809.0, 'total_ad_sales_change': -20.71, 'troas': 26.49, 'troas_change': 0.49, 'roas': 24.81, 'roas_change': 1.39, 'program_type': 'Performance', 'acos': 3.82, 'acos_change': -3.54, 'orders': 717.0, 'orders_change': -20.51, }, 
		{'search_term': 'men perfume', 'campaign_count': 5.0, 'is_exact': 1.0, 'impressions': 2459.0, 'impressions_change': -17.51, 'spends': 983.6, 'spends_change': -17.51, 'sales': 24200.0, 'sales_change': -5.91, 'cpatc': 1.06, 'cpatc_change': -2.75, 'cpm': 400.0, 'cpm_change': 40.3, 'total_ad_sales': 26219.0, 'total_ad_sales_change': -5.54, 'troas': 27.2, 'troas_change': 17.49, 'roas': 24.98, 'roas_change': 15.76, 'program_type': 'Performance', 'acos': 3.81, 'acos_change': -14.19, 'orders': 651.0, 'orders_change': -5.52, }, 
		{'search_term': 'nivea cream', 'campaign_count': 5.0, 'is_exact': 1.0, 'impressions': 2830.0, 'impressions_change': 673.22, 'spends': 849.0, 'spends_change': 673.22, 'sales': 6320.0, 'sales_change': 887.5, 'cpatc': 2.44, 'cpatc_change': 0.0, 'cpm': 300.0, 'cpm_change': 0.0, 'total_ad_sales': 8708.0, 'total_ad_sales_change': 838.36, 'troas': 11.05, 'troas_change': 13.8, 'roas': 7.89, 'roas_change': 7.93, 'program_type': 'Performance', 'acos': 9.26, 'acos_change': 0.0, 'orders': 211.0, 'orders_change': 859.09, }, 
		{'search_term': 'lip gloss', 'campaign_count': 1.0, 'is_exact': 1.0, 'impressions': 2117.0, 'impressions_change': -10.0, 'spends': 794.0, 'spends_change': 31.0, 'sales': 315.0, 'sales_change': 11.0, 'cpatc': 22.69, 'cpatc_change': 0.0, 'cpm': 375.0, 'cpm_change': 0.0, 'total_ad_sales': 1048.0, 'total_ad_sales_change': 0.0, 'troas': 1.32, 'troas_change': 0.0, 'roas': 0.4, 'roas_change': 0.0, 'program_type': 'Performance', 'acos': 75.76, 'acos_change': 0.0, 'orders': 24.0, 'orders_change': 0.0, }]
		
		start_date='2025-01-25'
		end_date='2025-01-25'
		kw_anlytics_data=blnkt_kw_anlys(start_date,end_date)
		o_s = DT.datetime.strptime(start_date, '%Y-%m-%d')
		o_e = DT.datetime.strptime(end_date, '%Y-%m-%d')
		o_d = o_e - o_s

		row_counts['keyword_analysis_c'] = len(kw_anlytics_data)

		return render(request, 'blinkit/blnkt_keyword_analytics.html', {'keyword_anlys': kw_anlytics_data, 'balance': bal, 'filters':filter_list, 'pf_op':platf, 'username':request.user, 'DATE':(start_date, end_date), 'O_date':(o_s, o_e, o_d.days+1) , 'nums':row_counts})



@login_required(login_url="/")
def productAnalytics(request):
	start_date, end_date = str(DT.date.today() - DT.timedelta(days=7)), str(DT.date.today() - DT.timedelta(days=1))
	# if 'Dates' not in request.session:
	# 	request.session['Dates'] = (str(DT.date.today() - DT.timedelta(days=7)), str(DT.date.today() - DT.timedelta(days=1)))
	bal = request.session["wallet_balance"]
	platf = request.session['platform']
	filter_list = { 'string_filter' : ['portfolio' ],
										'metrics_filter': ['Impressions','Clicks', 'Spends', 'orders', 'Sales', 'CTR', 'ACoS'],
									}
	
	if platf == 'Binkit':
		filter_list = { 'string_filter' : ['portfolio' ],
										'metrics_filter': ['Impressions','Clicks', 'Spends', 'orders', 'Sales', 'CTR', 'ACoS'],
									}
		start_date, end_date = str(DT.date.today() - DT.timedelta(days=7)), str(DT.date.today() - DT.timedelta(days=1))
		
		prdAnalytics=[]
		o_s = DT.datetime.strptime(start_date, '%Y-%m-%d')
		o_e = DT.datetime.strptime(end_date, '%Y-%m-%d')
		o_d = o_e - o_s
		
		return render(request, 'blinkit/blinkit_product_analytics.html', { 'productanalysis':prdAnalytics, 'username':request.user, 'balance': bal, 'pf_op':platf, 'filters':filter_list, 'DATE':(start_date, end_date), 'data':[], 'O_date':(o_s, o_e, o_d.days+1) })
		# prdAnalytics = [
		# 	{'product_name': 'NIVEA Body Lotion  Aloe Hydration, with Aloe Vera for Instant Hydration in Summer, for Men & Women, 400 ml', 'ad_group_name': 'NV AUTO', 'campaign_name': 'HM_BL_NV_SP_Auto___Summer_Grow', 'asin': 'B079KGC4NZ', 'total_ad_sales': 495225.0, 'total_ad_sales_change': 6.595821620295234, 'troas': 1.9, 'troas_change': 20.918032786885245, 'spends': 56446.23087601364, 'spends_change': 165.98517976336672, 'sales': 78512.24073791504, 'sales_change': 85.7099432620371, 'cpm': 295.0, 'cpm_change': 87.89808917197452, 'roas': 42.577137059606855, 'roas_change': 53.583331602281234, 'ctr': 0.3724, 'ctr_change': 13.43283582089553, 'reseller_name_crawl': 'RK World Infocom Pvt Ltd', 'osa': 100.0, 'price_sp': 299.0, }, 
		# 	{'product_name': 'NIVEA Body Lotion for Dry Skin  Shea Smooth, with Shea Butter, For Men & Women, 400 ml', 'ad_group_name': 'NV AUTO', 'campaign_name': 'HM_BL_NV_SP_Auto___Summer_Grow', 'asin': 'B00BKY9I62', 'total_ad_sales': 399574.0, 'total_ad_sales_change': 81.72614689121646, 'troas': 8.1, 'troas_change': 76.62337662337663, 'spends': 24315.15971619636, 'spends_change': 88.9187040549532, 'sales': 48261.76985168457, 'sales_change': 57.24062226637685, 'cpm': 159.0, 'cpm_change': 63.91752577319587, 'roas': 59.144325798633055, 'roas_change': -10.974059452124866, 'ctr': 0.2042, 'ctr_change': -2.808186577820093, 'reseller_name_crawl': 'RK World Infocom Pvt Ltd', 'osa': 100.0, 'price_sp': 390.0, }, 
		# 	{'product_name': 'NIVEA Body Lotion Natural Glow  Cell Repair, SPF 15 & 50x Vitamin C 400 ml', 'ad_group_name': 'exact match', 'campaign_name': 'HM_BL_NV_SP_Brand_Core_KW_Summer_Maintain', 'asin': 'B019XY4FOU', 'total_ad_sales': 663012.0, 'total_ad_sales_change': 67.63978578905582, 'troas': 2.9, 'troas_change': 113.05483028720627, 'spends': 109903.35971911252, 'spends_change': 226.21013891228085, 'sales': 141031.1792602539, 'sales_change': 80.72502851302085, 'cpm': 531.0, 'cpm_change': 101.13636363636364, 'roas': 57.61077584160699, 'roas_change': -15.174404509207124, 'ctr': 0.3692, 'ctr_change': 27.091222030981065, 'reseller_name_crawl': 'RK World Infocom Pvt Ltd', 'osa': 100.0, 'price_sp': 330.0,}, 
		# 	{'product_name': 'Nivea Aloe Protection SPF 15  Summer Body Lotions for Men and Women for All Skin Type 400 ml', 'ad_group_name': 'exact match', 'campaign_name': 'HM_BL_NV_SP_Brand_Core_KW_Summer_Maintain', 'asin': 'B085LZH7VZ', 'total_ad_sales': 425741.0, 'total_ad_sales_change': 109.1435617299719, 'troas': 4.5, 'troas_change': 78.714859437751, 'spends': 15365.409837782383, 'spends_change': 138.04478099606305, 'sales': 21362.539924621582, 'sales_change': 89.88299855844178, 'cpm': 76.0, 'cpm_change': 105.40540540540539, 'roas': 48.00570769577884, 'roas_change': 6.249138519217995, 'ctr': 0.1045, 'ctr_change': -14.554374488961578, 'reseller_name_crawl': 'unknown', 'osa': 0.0, 'price_sp': 0.0, }, 
		# 	{'product_name': 'NIVEA Body Lotion  Aloe Hydration, with Aloe Vera for Instant Hydration in Summer, for Men & Women 600 ml', 'ad_group_name': 'exact match', 'campaign_name': 'HM_BL_NV_SP_Brand_Core_KW_Summer_Maintain', 'asin': 'B082T9ZFDK', 'total_ad_sales': 293096.0, 'total_ad_sales_change': 67.50735819402772, 'troas': 1.4, 'troas_change': 145.14480408858603, 'spends': 58964.74060387164, 'spends_change': 258.56510519442423, 'sales': 160002.18942260742, 'sales_change': 159.47296448205063, 'cpm': 522.0, 'cpm_change': 169.0721649484536, 'roas': 111.1898467148071, 'roas_change': 5.844774253623161, 'ctr': 0.491, 'ctr_change': 46.348733233979125, 'reseller_name_crawl': 'unknown', 'osa': 0.0, 'price_sp': 0.0, }, 
		# 	{'product_name': 'NIVEA Body Lotion For Men & Women  Express Hydration, for Fast Absorption, 400 ml', 'ad_group_name': 'exact match', 'campaign_name': 'HM_BL_NV_SP_Brand_Core_KW_Summer_Maintain', 'asin': 'B006LX9WCM', 'total_ad_sales': 180972.0, 'total_ad_sales_change': 92.84549726671142, 'troas': 2.9, 'troas_change': 89.6103896103896, 'spends': 9217.190061647445, 'spends_change': 110.61460482291652, 'sales': 14530.280059814453, 'sales_change': 38.40958810961342, 'cpm': 438.0, 'cpm_change': 50.0, 'roas': 49.76123308155635, 'roas_change': -27.003162435340865, 'ctr': 0.1614, 'ctr_change': -1.6453382084095112, 'reseller_name_crawl': 'WZONE', 'osa': 100.0, 'price_sp': 499.0, }, 
		# 	{'product_name': 'Nivea Fresh Pure Shower Gel', 'ad_group_name': 'exact match', 'campaign_name': 'HM_BL_NV_SP_Brand_Core_KW_Summer_Maintain', 'asin': 'B0D1VMTQRZ', 'total_ad_sales': 56918.0, 'total_ad_sales_change': 133.88395792241946, 'troas': 4.1, 'troas_change': -14.583333333333334, 'spends': 1367.639986038208, 'spends_change': 34.03110446557384, 'sales': 837.2799987792969, 'sales_change': -71.88004825736238, 'cpm': 209.0, 'cpm_change': -71.42857142857143, 'roas': 20.4214633848609, 'roas_change': -67.07908088666815, 'ctr': 0.072, 'ctr_change': -63.48884381338742, 'reseller_name_crawl': 'unknown', 'osa': 0.0, 'price_sp': 0.0, }, 
		# 	{'product_name': 'Nivea Powerfruit Fresh Shower Gel', 'ad_group_name': 'SP_Auto', 'campaign_name': 'HM_SS_NV_SP_Auto', 'asin': 'B00E96MSGK', 'total_ad_sales': 27131.0, 'total_ad_sales_change': -26.13596144945686, 'troas': 1.6, 'troas_change': 5.128205128205128, 'spends': 1737.1500036120415, 'spends_change': -11.323297749700462, 'sales': 6744.889991760254, 'sales_change': -61.034787081556296, 'cpm': 271.0, 'cpm_change': -3.571428571428571, 'roas': 41.12737799853814, 'roas_change': -62.93552917513892, 'ctr': 0.6045, 'ctr_change': 42.33576642335767, 'reseller_name_crawl': 'RK World Infocom Pvt Ltd', 'osa': 100.0, 'price_sp': 324.0, }, 
		# 	{'product_name': 'Nivea SPF 50 PA++ Sunscreen', 'ad_group_name': 'SP_Auto', 'campaign_name': 'HM_SS_NV_SP_Auto', 'asin': 'B00E96MU5E', 'total_ad_sales': 73164.0, 'total_ad_sales_change': -46.19028006589786, 'troas': 11.7, 'troas_change': -16.120218579234972, 'spends': 3488.0199933052063, 'spends_change': -32.51910500618673, 'sales': 19952.12017059326, 'sales_change': 3.6504462683094117, 'cpm': 551.0, 'cpm_change': 34.146341463414636, 'roas': 64.99061944818652, 'roas_change': 23.570238873619683, 'ctr': 0.4196, 'ctr_change': 55.86924219910846, 'reseller_name_crawl': 'RK World Infocom Pvt Ltd', 'osa': 100.0, 'price_sp': 494.0, }, 
		# 	{'product_name': 'Nivea Cherry Shine Tinted Lip Balm', 'ad_group_name': 'BRAND| PT |', 'campaign_name': 'HM_NV_SS_SP_Brand__PT', 'asin': 'B00E96MT64', 'total_ad_sales': 22765.0, 'total_ad_sales_change': -45.84275008921137, 'troas': 11.9, 'troas_change': -35.40983606557377, 'spends': 2786.369980365038, 'spends_change': -44.73703031592933, 'sales': 5283.569915771484, 'sales_change': -61.76410369099471, 'cpm': 321.0, 'cpm_change': -57.14285714285714, 'roas': 26.820151856708044, 'roas_change': -40.80229251651464, 'ctr': 0.8654, 'ctr_change': 19.26681367144431, 'reseller_name_crawl': 'RK World Infocom Pvt Ltd', 'osa': 100.0, 'price_sp': 230.0, }, 
		# 	{'product_name': 'Nivea Essential Care Lip Balm', 'ad_group_name': 'BRAND| PT |', 'campaign_name': 'HM_NV_SS_SP_Brand__PT', 'asin': 'B00E96MV30', 'total_ad_sales': 74436.0, 'total_ad_sales_change': -14.914727264414065, 'troas': 6, 'troas_change': -11.611030478955007, 'spends': 7107.669913768768, 'spends_change': -31.248876260748993, 'sales': 28910.04981994629, 'sales_change': -8.070394892736404, 'cpm': 830.0, 'cpm_change': -21.69811320754717, 'roas': 47.47134617396763, 'roas_change': 4.005743709202971, 'ctr': 0.8182, 'ctr_change': 3.885220924327079, 'reseller_name_crawl': 'RK World Infocom Pvt Ltd', 'osa': 100.0, 'price_sp': 346.5, }, 
		# 	{'product_name': 'NIVEA Body Lotion for Very Dry Skin  Cocoa Nourish, with Coconut Oil & Cocoa Butter, For Men & Women, 400 ml', 'ad_group_name': 'EXACT | SS KW', 'campaign_name': 'HM_BL_SP_NV_ SUN BRAND KWs', 'asin': 'B00NW7NTTW', 'total_ad_sales': 176304.0, 'total_ad_sales_change': 0.4077727407339909, 'troas': 9.9, 'troas_change': 31.69107856191744, 'spends': 12078.149589538574, 'spends_change': 280.33629690779713, 'sales': 34097.29913330078, 'sales_change': 0.0, 'cpm': 126.0, 'cpm_change': 0.0, 'roas': 34.4765410852384, 'roas_change': 0.0, 'ctr': 0.561, 'ctr_change': 31.16670563479075, 'reseller_name_crawl': 'RK World Infocom Pvt Ltd', 'osa': 100.0, 'price_sp': 302.0, }, 
		# 	{'product_name': 'NIVEA Body Lotion for Very Dry Skin  Nourishing Body Milk with 2x Almond Oil 48H Moisturization, For Men & Women, 600 ml', 'ad_group_name': 'EXACT', 'campaign_name': 'HM_BL_NV_SP_Brand__KW_Winter_Maintain', 'asin': 'B07VKM2HR5', 'total_ad_sales': 12.17, 'total_ad_sales_change': 13.16, 'troas': 13, 'troas_change': -5.5, 'spends': 4809.13, 'spends_change': -13.7, 'sales': 25437.79, 'sales_change': -1.6, 'cpm': 379.27, 'cpm_change': -0.4, 'roas': 3.0, 'roas_change': 1.4, 'ctr': 7, 'ctr_change': 2.5, 'reseller_name_crawl': 'RK World Infocom Pvt Ltd', 'osa': 100.0, 'price_sp': 349.0, }, 
		# 	{'product_name': 'NIVEA Body Lotion for Very Dry Skin  Nourishing Body Milk with 2x Almond Oil for 48H Moisturization, For Men & Women, 400 ml', 'ad_group_name': 'PHRASE', 'campaign_name': 'HM_BL_NV_SP_Brand__KW_Winter_Maintain', 'asin': 'B00IJ72QWQ', 'total_ad_sales': 40612.96, 'total_ad_sales_change': 40.1, 'troas': 19, 'troas_change': 0.0, 'spends': 5795.2, 'spends_change': 5.05, 'sales': 29969.7, 'sales_change': -9.3, 'cpm':642.87, 'cpm_change': -3.0, 'roas': 5.9, 'roas_change': 9.1, 'ctr': 7.4, 'ctr_change': 0.6, 'reseller_name_crawl': 'RK World Infocom Pvt Ltd', 'osa': 100.0, 'price_sp': 299.0, }, 
		# 	{'product_name': 'Nivea Body Lotion for Dry Skin  Shea Smooth, with Shea Butter, For Men & Women, 600 ml, Transparent', 'ad_group_name': 'EXACT', 'campaign_name': 'HM_BL_NV_SP_Brand_Core_KW_Winter_Grow', 'asin': 'B099NNW9LL', 'total_ad_sales': 41600.48, 'total_ad_sales_change': -5.8, 'troas': 10.4, 'troas_change': 0.0, 'spends': 5324.7, 'spends_change': 0.0, 'sales': 29969.7, 'sales_change': 13.6, 'cpm': 525.46, 'cpm_change': 27.9, 'roas': 14.8, 'roas_change': 8.8, 'ctr': 3.7, 'ctr_change': 0.5, 'reseller_name_crawl': 'RK World Infocom Pvt Ltd', 'osa': 100.0, 'price_sp': 500.0, }, 
		# 	{'product_name': 'Nivea Smooth Milk Shea Butter Moisturizing Lotion', 'ad_group_name': 'Exact match', 'campaign_name': 'HM_NV_FS_SP_Auto__', 'asin': 'B0CXTKRKLH', 'total_ad_sales': 9070.0, 'total_ad_sales_change': -33.91139609443311, 'troas': 98.0, 'troas_change': 24.050632911392405, 'spends': 4718.929972648621, 'spends_change': 269.7496504968617, 'sales': 3388.1299743652344, 'sales_change': 70.96484421239563, 'cpm': 686.26, 'cpm_change': -20.0, 'roas': 34.57275484046158, 'roas_change': 37.818598905910775, 'ctr': 1.0805, 'ctr_change': 87.71716469770674, 'reseller_name_crawl': 'unknown', 'osa': 0.0, 'price_sp': 0.0, }, 
		# 	]

			
		
	

@login_required(login_url="/")
def negativeKeyword(request):
	# if 'Dates' not in request.session:
	# 	request.session['Dates'] = (str(DT.date.today() - DT.timedelta(days=7)), str(DT.date.today() - DT.timedelta(days=1)))

	start_date, end_date = str(DT.date.today() - DT.timedelta(days=7)), str(DT.date.today() - DT.timedelta(days=1))
 
	filter_list = { 'string_filter' : ['Search Term', 'Ad Group', 'Campaign' ],
                    'metrics_filter': ['Impressions','Clicks', 'Spends', 'Sales', 'CTR', 'ROAS', 'Orders'],
                  }
	bal = request.session["wallet_balance"]
	platf = request.session['platform']
	global row_counts
	
	if platf == 'Blinkit':
		filter_list = { 'string_filter' : ['Search Term', 'Campaign'],
                    'metrics_filter': ['Impressions','Spends', 'Sales', 'ACOS', 'Direct ATC'],
                  }
		start_date, end_date = str(DT.date.today() - DT.timedelta(days=7)), str(DT.date.today() - DT.timedelta(days=1))
		bal = request.session["wallet_balance"]

		ngt_kwrd_data= [
			{'search_term': 'men', 'type': 'Performance', 'campaign_name': 'Roll On Men Metro 1 PPB', 'impressions': 32702.0, 'impressions_change': -1.3454808736575359, 'direct_atc': 2062.0, 'direct_atc_change': -6.865401987353207, 'spends': 22842.4, 'spends_change': -0.4176439302124808, 'sales': 88895.0, 'sales_change': -7.425149700598803, 'acos': 25.39, 'acos_change': -60.82394692177133}, 
			{'search_term': 'cream', 'type': 'Performance', 'campaign_name': 'Body Lotion (summer) Metro 2 PPB', 'impressions': 25993.0, 'impressions_change': 5.847619823268315, 'direct_atc': 1043.0, 'direct_atc_change': 12.513484358144552, 'spends': 18333.2, 'spends_change': 8.029816446186032, 'sales': 44930.0, 'sales_change': 8.881618805282928, 'acos': 41.26, 'acos_change': 0.9789525208027376}, 
			{'search_term': 'deo', 'type': 'Performance', 'campaign_name': 'Roll On Men Metro 1 PPB', 'impressions': 25862.0, 'impressions_change': 41.13730626500764, 'direct_atc': 604.0, 'direct_atc_change': 49.504950495049506, 'spends': 16395.1, 'spends_change': 44.32179294196352, 'sales': 23160.0, 'sales_change': 42.69870609981516, 'acos': 72.43, 'acos_change': 1.2157629960872058}, 
			{'search_term': 'men perfume', 'type': 'Performance', 'campaign_name': 'Roll On Men Others PPB', 'impressions': 22650.0, 'impressions_change': 57.751776013372336, 'direct_atc': 1286.0, 'direct_atc_change': 51.294117647058826, 'spends': 13540.7, 'spends_change': 62.578793809358004, 'sales': 34726.0, 'sales_change': 32.37525254450501, 'acos': 41.13, 'acos_change': 26.748844375963017}, 
			{'search_term': 'spray', 'type': 'Performance', 'campaign_name': 'Deo Women Metro 2 PPB', 'impressions': 21608.0, 'impressions_change': 43.10881515332141, 'direct_atc': 2077.0, 'direct_atc_change': 46.267605633802816, 'spends': 13417.8, 'spends_change': 43.35103257443829, 'sales': 55456.0, 'sales_change': 56.518303180830344, 'acos': 24.49, 'acos_change': -7.445200302343169}, 
			{'search_term': 'hamper', 'type': 'Performance', 'campaign_name': 'Roll On Men Metro 1 PPB', 'impressions': 22898.0, 'impressions_change': 100.5781359495445, 'direct_atc': 1342.0, 'direct_atc_change': 106.46153846153845, 'spends': 12314.4, 'spends_change': 103.49671150477575, 'sales': 34387.0, 'sales_change': 94.55162659123056, 'acos': 36.48, 'acos_change': -6.46153846153847}, 
			{'search_term': 'body cream', 'type': 'Performance', 'campaign_name': 'Body Lotion (winter) Metro 2 PPB', 'impressions': 17245.0, 'impressions_change': 2.654919935710459, 'direct_atc': 364.0, 'direct_atc_change': 0.8310249307479225, 'spends': 9055.8, 'spends_change': 2.659502108556642, 'sales': 12760.0, 'sales_change': -9.213802917111348, 'acos': 72.74, 'acos_change': 19.973610423882555}, 
			{'search_term': 'women perfume', 'type': 'Performance', 'campaign_name': 'Deo Women Metro 2 PPB', 'impressions': 17777.0, 'impressions_change': 30.1105174559028, 'direct_atc': 1194.0, 'direct_atc_change': 39.976553341148886, 'spends': 8888.5, 'spends_change': 30.1105174559028, 'sales': 28455.0, 'sales_change': 34.13311963797492, 'acos': 31.91, 'acos_change': -11.6800442845281}, 
			{'search_term': 'rol', 'type': 'Performance', 'campaign_name': 'Roll on Women NCR PPB', 'impressions': 17891.0, 'impressions_change': 17.441249835893398, 'direct_atc': 194.0, 'direct_atc_change': 19.018404907975462, 'spends': 7934.1, 'spends_change': 14.930324188081245, 'sales': 5175.0, 'sales_change': 41.819676623732526, 'acos': 150.1, 'acos_change': -59.1008174386921}, 
			{'search_term': 'bathing', 'type': 'Performance', 'campaign_name': 'Body Wash Men NCR PPB', 'impressions': 7557.0, 'impressions_change': 15.409285277947465, 'direct_atc': 329.0, 'direct_atc_change': 1.5432098765432098, 'spends': 5252.8, 'spends_change': 15.413178652253201, 'sales': 14530.0, 'sales_change': 10.494296577946768, 'acos': 37.62, 'acos_change': -29.5901179112858}, 
			{'search_term': 'moisturizer', 'type': 'Performance', 'campaign_name': 'Body Lotion (summer) Others PPB', 'impressions': 6987.0, 'impressions_change': 3.865021554927903, 'direct_atc': 1045.0, 'direct_atc_change': 2.1505376344086025, 'spends': 3633.1, 'spends_change': 3.880025161548574, 'sales': 41880.0, 'sales_change': 6.932209881271543, 'acos': 8.98, 'acos_change': -6.165099268547543}, 
			{'search_term': 'face moisturizer', 'type': 'Performance', 'campaign_name': 'Nivea Creme women Metro 2 PPB', 'impressions': 9426.0, 'impressions_change': -20.455696202531644, 'direct_atc': 661.0, 'direct_atc_change': -32.27459016393443, 'spends': 2827.8, 'spends_change': -20.45569620253164, 'sales': 14670.0, 'sales_change': -30.638297872340424, 'acos': 19.94, 'acos_change': 13.812785388127862}, 
			{'search_term': 'men perfume', 'type': 'Performance', 'campaign_name': 'Deo men Others PPB', 'impressions': 5763.0, 'impressions_change': 40.21897810218978, 'direct_atc': 308.0, 'direct_atc_change': 26.229508196721312, 'spends': 2662.6, 'spends_change': 43.768898488120946, 'sales': 9295.0, 'sales_change': 10.865935114503817, 'acos': 35.82, 'acos_change': 48.13895781637718},
			{'search_term': 'body roll on', 'type': 'Performance', 'campaign_name': 'Roll on Women NCR PPB', 'impressions': 4049.0, 'impressions_change': -4.54974068835455, 'direct_atc': 118.0, 'direct_atc_change': -28.04878048780488, 'spends': 2323.6, 'spends_change': -3.919947072444599, 'sales': 5050.0, 'sales_change': -23.13546423135464, 'acos': 56.05, 'acos_change': 9.729835552075174}, 
		]

		if request.POST:
			inpu_date = request.POST.get('dates', None)
			if inpu_date == None:
				end_date = DT.date.today() - DT.timedelta(days=1)
				start_date = DT.date.today() - DT.timedelta(days=7)
				start_date_str = start_date.strftime("%Y-%m-%d")
				end_date_str = end_date.strftime("%Y-%m-%d")
				inpu_date = f"{start_date_str}/{end_date_str}"
				
			start_date, end_date = tuple(inpu_date.split('/'))	

		start_date='2025-01-22'
		end_date='2025-01-22'
		ngt_kwrd_data = blnkt_neg_kw(start_date, end_date)
		row_counts['negative_keyword_c'] = len(ngt_kwrd_data)

		o_s = DT.datetime.strptime(start_date, '%Y-%m-%d')
		o_e = DT.datetime.strptime(end_date, '%Y-%m-%d')
		o_d = o_e - o_s

		return render(request, 'blinkit/blinkit_neg_keyword.html', {'balance': bal, 'filters':filter_list, 'pf_op':platf, 'negative_keyword':ngt_kwrd_data, 'username':request.user, 'DATE':(start_date, end_date),'O_date':(o_s, o_e, o_d.days+1) , 'nums':row_counts})



@login_required(login_url="/")
def recommendation(request):
	filter_list = { 'string_filter' : ['Campaign', 'Campaign Type'],
                    'metrics_filter': ['Impressions', 'Clicks', 'Spends', 'Sales', 'CTR', 'Total Ad Sales', 'TROAS'],
                  }
	start_date, end_date = str(DT.date.today() - DT.timedelta(days=7)), str(DT.date.today() - DT.timedelta(days=1))
	bal = request.session["wallet_balance"]
	platf = request.session['platform']
	if platf == 'Blinkit' :
		# recom= [
		# 	{'user_name': 'nivea', 'date': '2024-05-15', 'time': '0 days 12:30:29', 'campaign_name': 'HM_SM_PCAi_Roll On_M_Auto__High CVR_6 Mar 24', 'placement_type': 'SEARCH_PAGE_TOP_SLOT', 'keyword': 'keyword', 'suggestion': 'Decrease the Bid it is Out Of Range', 'type': 'PCA'}, 
		# 	{'user_name': 'nivea', 'date': '2024-05-15', 'time': '0 days 12:30:30', 'campaign_name': 'HM_SM_PLA_Roll On_M_Auto__High CVR_5 Apr 24', 'placement_type': 'All', 'keyword': 'keyword', 'suggestion': 'Multiple ads groups found Kindly refactor this campaign', 'type': 'PLA'}, 
		# 	{'user_name': 'nivea', 'date': '2024-05-15', 'time': '0 days 12:30:33', 'campaign_name': 'HM_SM_DEO_M_SEARCH_30 APR', 'placement_type': 'BROWSE_PAGE_TOP_SLOT', 'keyword': 'keyword', 'suggestion': 'Increase the Bid it is Out Of Range', 'type': 'PLA'}, 
		# 	{'user_name': 'nivea', 'date': '2024-05-15', 'time': '0 days 12:30:33', 'campaign_name': 'HM_SM_DEO_M_SEARCH_30 APR', 'placement_type': 'BROWSE_PAGE', 'keyword': 'keyword', 'suggestion': 'Increase the Bid it is Out Of Range', 'type': 'PLA'}, 
		# 	{'user_name': 'nivea', 'date': '2024-05-15', 'time': '0 days 12:30:34', 'campaign_name': 'HM_SM_DEO_M_SEARCH_30 APR', 'placement_type': 'PRODUCT_PAGE', 'keyword': 'keyword', 'suggestion': 'Increase the Bid it is Out Of Range', 'type': 'PLA'}, 
		# 	{'user_name': 'nivea', 'date': '2024-05-15', 'time': '0 days 12:30:34', 'campaign_name': 'HM_SM_DEO_M_SEARCH_30 APR', 'placement_type': 'HOME_PAGE', 'keyword': 'keyword', 'suggestion': 'Increase the Bid it is Out Of Range', 'type': 'PLA'}, 
		# 	{'user_name': 'nivea', 'date': '2024-05-15', 'time': '0 days 12:30:39', 'campaign_name': 'HM_SM_PCA_BW_M_NewCreative_Brand', 'placement_type': 'SEARCH_PAGE_TOP_SLOT', 'keyword': 'keyword', 'suggestion': 'Decrease the Bid it is Out Of Range', 'type': 'PCA'},
		# 	  {'user_name': 'nivea', 'date': '2024-05-15', 'time': '0 days 12:30:39', 'campaign_name': 'HM_SM_PLA_Deo_M_Comp_PI kwd', 'placement_type': 'BROWSE_PAGE', 'keyword': 'keyword', 'suggestion': 'Increase the Bid it is Out Of Range', 'type': 'PLA'}, 
		# 		{'user_name': 'nivea', 'date': '2024-05-15', 'time': '0 days 12:30:40', 'campaign_name': 'HM_SM_PLA_Deo_M_Comp_PI kwd', 'placement_type': 'PRODUCT_PAGE', 'keyword': 'keyword', 'suggestion': 'Increase the Bid it is Out Of Range', 'type': 'PLA'}, 
		# 		{'user_name': 'nivea', 'date': '2024-05-15', 'time': '0 days 12:30:40', 'campaign_name': 'HM_SM_PLA_Deo_M_Comp_PI kwd', 'placement_type': 'HOME_PAGE', 'keyword': 'keyword', 'suggestion': 'Increase the Bid it is Out Of Range', 'type': 'PLA'}, 
		# 		{'user_name': 'nivea', 'date': '2024-05-15', 'time': '0 days 12:30:41', 'campaign_name': 'HM_SM_PLA_BL_Geo_Top 3', 'placement_type': 'SEARCH_PAGE_TOP_SLOT', 'keyword': 'keyword', 'suggestion': 'Decrease the Bid it is Out Of Range', 'type': 'PLA'}, 
		# 		{'user_name': 'nivea', 'date': '2024-05-15', 'time': '0 days 12:30:42', 'campaign_name': 'HM_SM_PLA_BL_Geo_Top 3', 'placement_type': 'SEARCH_PAGE', 'keyword': 'keyword', 'suggestion': 'Decrease the Bid it is Out Of Range', 'type': 'PLA'}, 
		# 		{'user_name': 'nivea', 'date': '2024-05-15', 'time': '0 days 12:30:42', 'campaign_name': 'HM_SM_PLA_BL_Geo_Top 3', 'placement_type': 'BROWSE_PAGE_TOP_SLOT', 'keyword': 'keyword', 'suggestion': 'Decrease the Bid it is Out Of Range', 'type': 'PLA'}, 
		# 		{'user_name': 'nivea', 'date': '2024-05-15', 'time': '0 days 12:30:44', 'campaign_name': 'HM_SM_CREME_SEARCH_30 APR', 'placement_type': 'All', 'keyword': 'keyword', 'suggestion': 'Multiple ads groups found Kindly refactor this campaign', 'type': 'PLA'}, 
		# 		{'user_name': 'nivea', 'date': '2024-05-15', 'time': '0 days 12:30:45', 'campaign_name': 'HM_SM_CREME_SEARCH_30 APR', 'placement_type': 'BROWSE_PAGE_TOP_SLOT', 'keyword': 'keyword', 'suggestion': 'Increase the Bid it is Out Of Range', 'type': 'PLA'}, 
		# 		{'user_name': 'nivea', 'date': '2024-05-15', 'time': '0 days 12:30:46', 'campaign_name': 'HM_SM_CREME_SEARCH_30 APR', 'placement_type': 'BROWSE_PAGE', 'keyword': 'keyword', 'suggestion': 'Increase the Bid it is Out Of Range', 'type': 'PLA'}, ]
		recom=[]
		return render(request, 'blinkit/blinkit_recommend.html', {'balance': bal, 'recdata':recom,  'pf_op':platf, 'filters':filter_list,  'username':request.user, 'DATE':(start_date, end_date) })



@login_required(login_url="/")
def History(request):
	filter_list = { 'string_filter' : ['portfolio' ],
                    'metrics_filter': ['Impressions','Clicks', 'Spends', 'orders', 'Sales', 'CTR', 'ACoS'],
                  }
	bal = request.session["wallet_balance"]
	platf = request.session['platform']
	
	if platf == 'Blinkit':
		# history = [
		# 	{'date': '2024-08-03', 'time': '02:32', 'revert': 'Not Possible', 'module': 'Keywords', 'type': 'bid change_rule', 'property': 'bid', 'from': 21.11, 'to': 20.48, 'source': 'System', 'campaign_name': 'HM_SM_PLA_FW_M_Brand_KT_Broad', 'placement_type': 'BROWSE_PAGE_TOP_SLOT', 'platform': 'GROCERY', 'nature': 'Auto', 'source_name': 'System', 'camp_type': 'PLA', 't_camp': 61.0, 'u_camp': 60.0}, 
		# 	{'date': '2024-08-03', 'time': '02:32', 'revert': 'Not Possible', 'module': 'Keywords', 'type': 'bid change_rule', 'property': 'bid', 'from': 11.68, 'to': 11.33, 'source': 'System', 'campaign_name': 'HM_SM_PLA_FW_M_Brand_KT_Broad', 'placement_type': 'BROWSE_PAGE', 'platform': 'GROCERY', 'nature': 'Auto', 'source_name': 'System', 'camp_type': 'PLA', 't_camp': 61.0, 'u_camp': 60.0}, 
		# 	{'date': '2024-08-03', 'time': '02:32', 'revert': 'Not Possible', 'module': 'Keywords', 'type': 'bid change_rule', 'property': 'bid', 'from': 25.0, 'to': 25.75, 'source': 'System', 'campaign_name': 'HM_SM_BL_L SALENCY FSN_1 JUL24', 'placement_type': 'SEARCH_PAGE_TOP_SLOT', 'platform': 'GROCERY', 'nature': 'Auto', 'source_name': 'System', 'camp_type': 'PLA', 't_camp': 61.0, 'u_camp': 60.0}, 
		# 	{'date': '2024-08-03', 'time': '02:32', 'revert': 'Not Possible', 'module': 'Keywords', 'type': 'bid change_rule', 'property': 'bid', 'from': 22.0, 'to': 22.66, 'source': 'System', 'campaign_name': 'HM_SM_BL_L SALENCY FSN_1 JUL24', 'placement_type': 'SEARCH_PAGE', 'platform': 'GROCERY', 'nature': 'Auto', 'source_name': 'System', 'camp_type': 'PLA', 't_camp': 61.0, 'u_camp': 60.0}, 
		# 	{'date': '2024-08-03', 'time': '02:32', 'revert': 'Not Possible', 'module': 'Keywords', 'type': 'bid change_rule', 'property': 'bid', 'from': 6.42, 'to': 6.61, 'source': 'System', 'campaign_name': 'HM_SM_BL_L SALENCY FSN_1 JUL24', 'placement_type': 'BROWSE_PAGE_TOP_SLOT', 'platform': 'GROCERY', 'nature': 'Auto', 'source_name': 'System', 'camp_type': 'PLA', 't_camp': 61.0, 'u_camp': 60.0}, 
		# 	{'date': '2024-08-03', 'time': '02:32', 'revert': 'Not Possible', 'module': 'Keywords', 'type': 'bid change_rule', 'property': 'bid', 'from': 6.42, 'to': 6.61, 'source': 'System', 'campaign_name': 'HM_SM_BL_L SALENCY FSN_1 JUL24', 'placement_type': 'BROWSE_PAGE', 'platform': 'GROCERY', 'nature': 'Auto', 'source_name': 'System', 'camp_type': 'PLA', 't_camp': 61.0, 'u_camp': 60.0}, 
		# 	{'date': '2024-08-03', 'time': '02:32', 'revert': 'Not Possible', 'module': 'Keywords', 'type': 'bid change_rule', 'property': 'bid', 'from': 6.42, 'to': 6.61, 'source': 'System', 'campaign_name': 'HM_SM_BL_L SALENCY FSN_1 JUL24', 'placement_type': 'PRODUCT_PAGE', 'platform': 'GROCERY', 'nature': 'Auto', 'source_name': 'System', 'camp_type': 'PLA', 't_camp': 61.0, 'u_camp': 60.0}, 
		# 	{'date': '2024-08-03', 'time': '02:32', 'revert': 'Not Possible', 'module': 'Keywords', 'type': 'bid change_rule', 'property': 'bid', 'from': 6.42, 'to': 6.61, 'source': 'System', 'campaign_name': 'HM_SM_BL_L SALENCY FSN_1 JUL24', 'placement_type': 'HOME_PAGE', 'platform': 'GROCERY', 'nature': 'Auto', 'source_name': 'System', 'camp_type': 'PLA', 't_camp': 61.0, 'u_camp': 60.0}, 
		# 	{'date': '2024-08-03', 'time': '02:32', 'revert': 'Not Possible', 'module': 'Keywords', 'type': 'bid change_rule', 'property': 'bid', 'from': 27.7, 'to': 28.53, 'source': 'System', 'campaign_name': 'HM_SM_BW_W_SEARCH_30 APR', 'placement_type': 'SEARCH_PAGE_TOP_SLOT', 'platform': 'GROCERY', 'nature': 'Auto', 'source_name': 'System', 'camp_type': 'PLA', 't_camp': 61.0, 'u_camp': 60.0}, 
		# 	{'date': '2024-08-03', 'time': '02:32', 'revert': 'Not Possible', 'module': 'Keywords', 'type': 'bid change_rule', 'property': 'bid', 'from': 24.74, 'to': 25.48, 'source': 'System', 'campaign_name': 'HM_SM_BW_W_SEARCH_30 APR', 'placement_type': 'SEARCH_PAGE', 'platform': 'GROCERY', 'nature': 'Auto', 'source_name': 'System', 'camp_type': 'PLA', 't_camp': 61.0, 'u_camp': 60.0}, 
		# 	{'date': '2024-08-03', 'time': '02:32', 'revert': 'Not Possible', 'module': 'Keywords', 'type': 'bid change_rule', 'property': 'bid', 'from': 25.48, 'to': 26.24, 'source': 'System', 'campaign_name': 'HM_SM_BW_W_SEARCH_30 APR', 'placement_type': 'BROWSE_PAGE_TOP_SLOT', 'platform': 'GROCERY', 'nature': 'Auto', 'source_name': 'System', 'camp_type': 'PLA', 't_camp': 61.0, 'u_camp': 60.0}, 
		# 	{'date': '2024-08-03', 'time': '02:32', 'revert': 'Not Possible', 'module': 'Keywords', 'type': 'bid change_rule', 'property': 'bid', 'from': 25.48, 'to': 26.24, 'source': 'System', 'campaign_name': 'HM_SM_BW_W_SEARCH_30 APR', 'placement_type': 'BROWSE_PAGE', 'platform': 'GROCERY', 'nature': 'Auto', 'source_name': 'System', 'camp_type': 'PLA', 't_camp': 61.0, 'u_camp': 60.0}, 
		# 	{'date': '2024-08-03', 'time': '02:32', 'revert': 'Not Possible', 'module': 'Keywords', 'type': 'bid change_rule', 'property': 'bid', 'from': 24.87, 'to': 25.62, 'source': 'System', 'campaign_name': 'HM_SM_BW_W_SEARCH_30 APR', 'placement_type': 'PRODUCT_PAGE', 'platform': 'GROCERY', 'nature': 'Auto', 'source_name': 'System', 'camp_type': 'PLA', 't_camp': 61.0, 'u_camp': 60.0}, 
		# 	{'date': '2024-08-03', 'time': '02:32', 'revert': 'Not Possible', 'module': 'Keywords', 'type': 'bid change_rule', 'property': 'bid', 'from': 18.58, 'to': 18.02, 'source': 'System', 'campaign_name': 'HM_SM_PLA_Deo_M_Generic_14 Mar 24_Combo', 'placement_type': 'SEARCH_PAGE_TOP_SLOT', 'platform': 'GROCERY', 'nature': 'Auto', 'source_name': 'System', 'camp_type': 'PLA', 't_camp': 61.0, 'u_camp': 60.0},
		# 	  {'date': '2024-08-03', 'time': '02:32', 'revert': 'Not Possible', 'module': 'Keywords', 'type': 'bid change_rule', 'property': 'bid', 'from': 24.87, 'to': 25.62, 'source': 'System', 'campaign_name': 'HM_SM_BW_W_SEARCH_30 APR', 'placement_type': 'HOME_PAGE', 'platform': 'GROCERY', 'nature': 'Auto', 'source_name': 'System', 'camp_type': 'PLA', 't_camp': 61.0, 'u_camp': 60.0}, 
		# 		{'date': '2024-08-03', 'time': '02:32', 'revert': 'Not Possible', 'module': 'Keywords', 'type': 'bid change_rule', 'property': 'bid', 'from': 18.58, 'to': 18.02, 'source': 'System', 'campaign_name': 'HM_SM_PLA_Deo_M_Generic_14 Mar 24_Combo', 'placement_type': 'SEARCH_PAGE', 'platform': 'GROCERY', 'nature': 'Auto', 'source_name': 'System', 'camp_type': 'PLA', 't_camp': 61.0, 'u_camp': 60.0}, 
		# 		 ]
		history=[]
		return render(request, 'blinkit/blinkit_history.html',  {'balance': bal, 'pf_op':platf, 'username':request.user, 'filters':filter_list, 'hst_data': history } )


@login_required(login_url="/")
def page_404(request):
	return render(request, 'page_404.html', {})



@login_required(login_url="/")
def insights(request):
	return render(request, 'gotoinsights.html', {})