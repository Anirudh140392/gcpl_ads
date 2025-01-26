"""
URL configuration for gcpl project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from Dash.views import * 

urlpatterns = [
    path('logout/', logout_view, name='logout'),
    path('login/', login_page, name='login_page'),    # Login page
    # path('login2/',login_view,name='login_view'),
    path('register/', register_page, name='register'),  # Registration page
    path("", enter, name='start'),
    path('admin/', admin.site.urls),


    path('blinkit/', blnkt_home, name='blnkthome'), 


    path('Campagins/', Campagins, name='camp'),
    # path('product/', Product, name='product'),
    path('Rules/', Rule, name='rules'), 
    path('keywrd/', keywords, name='key'), 
    path('keywrd_analytics/', keywordAnalytics, name='keywrd_analytics'),  
    path('product_analytics/', productAnalytics, name='product_analytics'),  
    path('negative_keyword/', negativeKeyword, name='negative_keyword'),  
    path('History/', History, name='historia'),   
    path('insights/', insights, name='insight'),
    # path('recommend/', recommendation, name='recomm'),          
    path('failed/', page_404, name='404')

]