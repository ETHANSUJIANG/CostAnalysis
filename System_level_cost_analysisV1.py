# coding: utf-8
"""
Created on Mon Jul  3 14:15:13 2023

@author: zhusj
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import ConnectionPatch
import warnings
import snowflake.connector
import sys
import os
import re
warnings.filterwarnings('ignore')

PN='P1000194588'
PN_string = PN,PN
my_query = f"""
select
ltrim(a.aufnr,'0') as production_order,
a.WERKS as plant,
b.GLTRI as actual_finish_date,
ltrim(b.plnbez,'0') as PN0,
h.description as description,
ltrim(c.matnr,'0') as PN1,
c.wewrt as accounting_cost,
ltrim(d.kstar,'0') as cost_element,
d.wrttp as cost_type,
d.WTG001,d.WTG002,d.WTG003,d.WTG004,d.WTG005,d.WTG006,d.WTG007,
d.WTG008,d.WTG009,d.WTG010,d.WTG011,d.WTG012,d.WTG013,d.WTG014,
d.WTG015,d.WTG016
from rpl_sap_attunity.aufk as a
inner join rpl_sap_attunity.afko as b on b.aufnr = a.aufnr
inner join core.material as h on h.material = b.plnbez
inner join rpl_sap_attunity.afpo as c on c.aufnr = a.aufnr
inner join rpl_sap_attunity.coss as d on d.objnr = a.objnr
where b.terkz ='1'
and c.elikz ='X'
and d.wrttp ='04'
--and a.WERKS = '2101'
and PN0 in {PN_string}
order by a.aufnr
"""    # internal cost query 

my_query1 = f"""
select 
ltrim(a.aufnr,'0') as production_order,
a.WERKS as plant,
b.GLTRI as actual_finish_date,
ltrim(b.plnbez,'0') as PN0,
ltrim(c.matnr,'0') as PN1,
c.wewrt as accounting_cost,
ltrim(d.kstar,'0') as cost_element,
d.wrttp as cost_type,
d.WTG001,d.WTG002,d.WTG003,d.WTG004,d.WTG005,d.WTG006,d.WTG007,d.WTG008,
d.WTG009,d.WTG010,d.WTG011,d.WTG012,d.WTG013,d.WTG014,d.WTG015,d.WTG016
from rpl_sap_attunity.aufk as a
inner join rpl_sap_attunity.afko as b on b.aufnr = a.aufnr
inner join rpl_sap_attunity.afpo as c on c.aufnr = a.aufnr
inner join rpl_sap_attunity.cosp as d on d.objnr = a.objnr
where b.terkz ='1'
and c.elikz ='X'
and d.wrttp ='04'
and PN0 in {PN_string}
--and a.WERKS = '2101'
order by a.aufnr
"""  # query statement

my_query2 = f"""
select 
ltrim(a.aufnr,'0') as production_order,
a.WERKS as plant,
b.GLTRI as actual_finish_date,
ltrim(b.plnbez,'0') as PN0,
d.matnr as component_pn,
f.description as description,
case when d.waers ='USD'
    then d.ENWRT 
    else round(d.enwrt * g.exchange_rate,2)
end as cot_total_cost_usd,
d.waers as orin_currency,
d.enmng as qty_consumption,
d.saknr as cost_element,
d.erfmg as qty_per_unit,
d.ERFME as UOM,
d.baugr as toplevel_pn,
case when d.waers ='USD'
    then d.GPREIS 
    else round(d.GPREIS * g.exchange_rate,2)
end as unit_cost_usd
from rpl_sap_attunity.aufk as a
inner join rpl_sap_attunity.afko as b on b.aufnr = a.aufnr
inner join rpl_sap_attunity.afpo as c on c.aufnr = a.aufnr
inner join (select matnr,ENWRT,waers,aufnr,enmng,saknr,erfmg,erfme,baugr,gpreis,sbter,concat(waers::text,substring(sbter::text,1,8)) as 
                                                                                 uni_key from rpl_sap_attunity.resb ) as d on d.aufnr = a.aufnr
inner join core.material as f on f.material = d.matnr
left join (select from_currency, to_currency, exchange_rate,rate_date,concat(from_currency::text,substring(to_char(rate_date,'yyyymmdd')::text,1,8)) as 
                                                                              uni_key from SAP_REPORTING.CURRENCY_CONVERSION where to_currency='USD') as g on g.uni_key = d.uni_key
where b.terkz ='1'
and c.elikz ='X'
and PN0 in {PN_string}
order by a.aufnr
""" # query the purchased individual part cost 


# query data from snowfake
with snowflake.connector.connect( 
    user='ethan.zhu@technipfmc.com', # Required. Replace with your email 
    authenticator="externalbrowser", # Required. 
    account='technipfmc-data', # Required. 
    database="idsprod", # Optional 
    schema="rpl_sap.ekko", # Optional. Replace with the schema you will be working on 
    role="reporting", # Optional. Replace with the role you will be working with 
    warehouse="reporting_wh", # Optional. Replace with the warehouse you will be working with 
    client_store_temporary_credential=True, # Only if installing secure-local-storage to avoid reopening tabs
    ) as conn: 
    cursor = conn.cursor()
    cursor.execute(my_query)
    cursor1 = conn.cursor()
    cursor1.execute(my_query1)
    cursor2 = conn.cursor()
    cursor2.execute(my_query2)
    # res = cursor.fetchall() # To return a list of tuples 
    df_query = cursor.fetch_pandas_all() # To return a dataframe
    df_query1 = cursor1.fetch_pandas_all()
    df_query2 = cursor2.fetch_pandas_all()
print(df_query.head(2),df_query.shape)
df_query.to_excel('data/order.xlsx')
df_query1.to_excel(f'data/outcost{PN}.xlsx')
df_query2.to_excel(f'data/indicost{PN}.xlsx')
pro_order = df_query['PRODUCTION_ORDER'].unique()
# prod_cols = df_query.columns

tpname =df_query['PN0'][0] +' '+ df_query['DESCRIPTION'][0][:35]
print(tpname)

class InternalCost:
    @staticmethod
    def iTcostSum(data):
        order = data['PRODUCTION_ORDER'].unique()
        df_new = data.iloc[:,0:8]
        df_new.index = df_new['PRODUCTION_ORDER']
        df_new['WTG_SUM']=0.0
        wtg_col=['WTG'+'00'+str(i) for i in range(1,10)]+\
            ['WTG'+'0'+str(i) for i in range(10,17)]
        for i in order:
            df = data[data['PRODUCTION_ORDER']==i][wtg_col]
            df_new.loc[i,'WTG_SUM']=df.sum(axis=1).tolist()
            if len(df_new[df_new['PRODUCTION_ORDER']==i])>1:
                total = df_new[df_new['PRODUCTION_ORDER']==i]['WTG_SUM'].sum()
                idx =list(df_new[df_new['PRODUCTION_ORDER']==i].index)
                df_new.drop(index=idx[1:])
                df_new.loc[i,'WTG_SUM']=total
        return df_new

df_order_inter = InternalCost.iTcostSum(df_query)        
df_order_inter.to_excel(f'data/{PN}sum_order.xlsx',index=False)
df_order_inter.info

class ComponentCost:
    @staticmethod
    def ComponentLevel(data):
        # order = data['PRODUCTION_ORDER'].unique()
        data.sort_values('ACTUAL_FINISH_DATE')
        data1 = data[['PRODUCTION_ORDER','ACTUAL_FINISH_DATE','PN0','COMPONENT_PN',\
                      'DESCRIPTION','COT_TOTAL_COST_USD','UNIT_COST_USD']]
        df = data1.groupby('PRODUCTION_ORDER')
        df_dict=dict()
        k =0
        for i,j in df:
            df_dict[k]=j
            k+=1
        return df_dict
df_dict = ComponentCost.ComponentLevel(df_query2)
print(df_dict[1])
# re.split("\s|','",'ACTUATOR SPRING HOUSING F/ AH700 (SUBSEA 2.0) 2 1/16-15K, W/ LINEAR OVERRIDE')

class Plot:
    @staticmethod
    def topLevelCost(data):
        x_y=data[['ACTUAL_FINISH_DATE','ACCOUNTING_COST']].\
        drop_duplicates('ACCOUNTING_COST',keep='last')
        x_y.sort_values('ACTUAL_FINISH_DATE')
        x= x_y['ACTUAL_FINISH_DATE']
        y= x_y['ACCOUNTING_COST']
        yper = [round((y.iloc[i+1]-y.iloc[i])/y.iloc[i]*100,0) for i in range(len(y)-1)]
        yper.insert(0,0)
        fig, ax = plt.subplots(figsize=(6,6))
        ax2 = ax.twinx()
        ax.bar(x, y,width =0.5,label ='Per unit cost')
        ax2.plot(x,yper,'*--r',label='cost rolling change by %')
        ax2.set_ylabel('%')
        ax.set_ylabel('usd')
        ax.tick_params(axis='x', rotation=-20)
        ax.set_ylim(y.min()*0.9,y.max()*1.1)
        ax.set_title(f'{PN} Per pcs cost')
        ax.set_xlabel('Prodduction date')
        ax.legend(loc = 'upper left')
        ax2.legend(loc ='best')
        plt.show()
    @staticmethod
    def costBreakdown(data):
      fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(9, 5))
      fig.subplots_adjust(wspace=0)

      # pie chart parameters
      overall_ratios = [.27, .56, .17]
      labels = ['Approve', 'Disapprove', 'Undecided']
      explode = [0.1, 0, 0]
      # rotate so that first wedge is split by the x-axis
      angle = -180 * overall_ratios[0]
      wedges, *_ = ax1.pie(overall_ratios, autopct='%1.1f%%', startangle=angle,
                          labels=labels, explode=explode)

      # bar chart parameters
      age_ratios = [.33, .54, .07, .06]
      age_labels = ['Under 35', '35-49', '50-65', 'Over 65']
      bottom = 1
      width = .2

      # Adding from the top matches the legend.
      for j, (height, label) in enumerate(reversed([*zip(age_ratios, age_labels)])):
          bottom -= height
          bc = ax2.bar(0, height, width, bottom=bottom, color='C0', label=label,
                      alpha=0.1 + 0.25 * j)
          ax2.bar_label(bc, labels=[f"{height:.0%}"], label_type='center')

      ax2.set_title('Age of approvers')
      ax2.legend()
      ax2.axis('off')
      ax2.set_xlim(- 2.5 * width, 2.5 * width)

      # use ConnectionPatch to draw lines between the two plots
      theta1, theta2 = wedges[0].theta1, wedges[0].theta2
      center, r = wedges[0].center, wedges[0].r
      bar_height = sum(age_ratios)

      # draw top connecting line
      x = r * np.cos(np.pi / 180 * theta2) + center[0]
      y = r * np.sin(np.pi / 180 * theta2) + center[1]
      con = ConnectionPatch(xyA=(-width / 2, bar_height), coordsA=ax2.transData,
                            xyB=(x, y), coordsB=ax1.transData)
      con.set_color([0, 0, 0])
      con.set_linewidth(4)
      ax2.add_artist(con)
      # draw bottom connecting line
      x = r * np.cos(np.pi / 180 * theta1) + center[0]
      y = r * np.sin(np.pi / 180 * theta1) + center[1]
      con = ConnectionPatch(xyA=(-width / 2, 0), coordsA=ax2.transData,
                            xyB=(x, y), coordsB=ax1.transData)
      con.set_color([0, 0, 0])
      ax2.add_artist(con)
      con.set_linewidth(4)

      plt.show()





