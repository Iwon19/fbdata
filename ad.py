# -*- coding: utf-8 -*-
"""
Created on Fri Aug 12 11:38:54 2022

@author: 20040563d
"""
import pandas as pd
import sys
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebookads.adobjects.adcreative import AdCreative
from facebook_business.adobjects.ad import Ad
import datetime
import os 
from pandas.io import gbq 
from google.cloud import bigquery as bq

credential_path =''
os.environ['GOOGLE_APPLICATION_CREDENTIALS']=credential_path
my_app_id = ''
my_app_secret = ''
my_access_token = ''
FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)

my_account = AdAccount('')
fields = [
    'object_story_spec',
    # 'name'

]

ads = my_account.get_ad_creatives(fields=fields)
print(ads)
lists=list(ads)
filtered=[]
for indexs in range(len(lists)):
    if len(lists[indexs])>1:
        filtered.append(lists[indexs]._data)
        
        
ids=[]
for indexs in range(len(filtered)):
        ids.append(lists[indexs].get('id'))
        
links=[]
ids=[]
for indexs in range(len(filtered)):
    z=filtered[indexs]
    q=z['object_story_spec']._data
    if q.get('video_data', 'no') != 'no' :
        e=q['video_data']._data
        a=e['call_to_action']._data
        b=a['value']._data 
        link=b['link']
        links.append(link)
        ids.append(filtered[indexs].get('id'))
    if q.get('link_data', 'no') != 'no' :
        e=q['link_data']._data
        link=e['link']
        links.append(link)
        ids.append(filtered[indexs].get('id'))
    
adcreative=pd.DataFrame(list(zip(ids, links)), columns =['creative_id', 'link']) 
#----------------------------------------------------------------------------------------##--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
##------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# ##----------------------------------------------------------------------------------------##--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def get_before_n_date(n):
    day = datetime.datetime.now() - datetime.timedelta(days=n)
    before_n_day = datetime.datetime(day.year, day.month, day.day).strftime('%Y-%m-%d')
    return before_n_day
##Set time range here##
range1= get_before_n_date(100)
range2= get_before_n_date(1)
my_account = AdAccount('act_1164736643718482')
params = {
    'time_range':{'since':range1,'until':range2}
}
adsfields=[

    'campaign_id',
    
    'adset_id',
    'creative'
    
    ]
idindex=my_account.get_ads(fields=adsfields,params=params)
print(idindex)

idindex = list(idindex)

campaign_id=[]

adset_id=[]
creative=[]
adid=[]
for indexs in range(len(idindex)):
    campaign_id.append(idindex[indexs]._data.get('campaign_id'))
    adset_id.append(idindex[indexs]._data.get('adset_id'))
    creative.append(idindex[indexs]._data.get('creative').get('id'))
    adid.append(idindex[indexs].get('id'))
    
adsid=pd.DataFrame(list(zip(adid,campaign_id, adset_id,creative)), columns =['id','campaign_id', 'adset_id','creative_id']) 



test= adsid.merge(adcreative, how='inner',on='creative_id')

#----------------------------------------------------------------------------------------##--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
##------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# ##----------------------------------------------------------------------------------------##--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------



adsetsfields=[

    'name'
    
]
adsets = my_account.get_ad_sets(fields=adsetsfields,params=params)
adset= list(adsets)

adsetid=[]

adsetname=[]
for indexs in range(len(adset)):
    adsetname.append(adset[indexs]._data.get('name'))
    adsetid.append(adset[indexs]._data.get('id'))
    
    
adsetdata=pd.DataFrame(list(zip(adsetid, adsetname)), columns =['adset_id', 'adset_name']) 
test1= test.merge(adsetdata, how='left',on='adset_id')








#----------------------------------------------------------------------------------------##--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
##------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# ##----------------------------------------------------------------------------------------##--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------







range3= get_before_n_date(1000)
my_account = AdAccount('act_1164736643718482')
params = {
    'time_range':{'since':range1,'until':range2}
}
adcamfields=[

    'name'
    
]


params = {
    'level': 'campaign', # <== level mechanism
    'time_range':{'since':range3,'until':range2}
}

camfields = [
    'campaign_id',
    'campaign_name',
    'spend',
    'clicks'
    
]

insights = AdAccount('act_1164736643718482').get_insights(
    params = params,
    fields = camfields,
)
campaign=list(insights)


campaign_id=[]
campaign_name=[]
spend=[]
click=[]
for indexs in range(len(campaign)):
        campaign_id.append(campaign[indexs]._data.get('campaign_id'))
        campaign_name.append(campaign[indexs]._data.get('campaign_name'))
        spend.append(campaign[indexs]._data.get('spend'))
        click.append(campaign[indexs]._data.get('clicks'))
campaigndata=pd.DataFrame(list(zip(campaign_id,campaign_name,spend,click)), columns =['campaign_id', 'campaign_name','spend','click']) 


table=test1.merge(campaigndata, how='left',on='campaign_id')
table.to_csv(r'C:/Users/user/table.csv',index=False)







table.to_gbq(destination_table='data.datatable',
        project_id='ch-experience-portal', 
        if_exists='replace')
