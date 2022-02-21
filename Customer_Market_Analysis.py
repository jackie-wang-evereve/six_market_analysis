### READ ME ###
# in terminal type: streamlit run Customer_Market_Analysis.py


# In[177]:

import pandas as pd
from tqdm import tqdm
import json
import sqlalchemy as sa
import numpy as np
import pymssql
import seaborn as sns
import matplotlib as plt
import folium
from folium.plugins import HeatMapWithTime,FastMarkerCluster
from folium import plugins
from tqdm import tqdm
from geopy import distance
import haversine 
from haversine import inverse_haversine, Direction, Unit
import branca.colormap as cm
import streamlit as st
import mitosheet
import plotly
import chart_studio.plotly as py
import plotly.figure_factory as ff
import plotly.express as px
from plotly.offline import iplot
import plotly.graph_objects as go

# In[181]:

st.title('Monthly Customers Acquired')

# In[114]:
@st.cache(allow_output_mutation=True)

def get_six_market_analysis():
    engine = sa.create_engine('mssql+pymssql://data_team_read_only:RYQus6dmCdybLZsUcGde@katniss.evereve.com:1433/edw')
    conn = engine.connect()
    market_name =['Southpark_NC_Distance','Broadway_CA_Distance','University_CA_Distance','Hyde_Park_FL_Distance','Woodlands_TX_Distance','Highland_TX_Distance']
    query = '''
            select 
    	      orig_customer_id, orig_date_closed, orig_store, Channel, address_id, Latitude,Longitude,   Market_Region, Distance_from_market
    
        from(
            select
               rlp.orig_customer_id
             , rlp.orig_store
             , rlp.orig_date_closed    
    
             , Row_Number()Over(Partition by rlp.orig_customer_id order by rlp.orig_date_closed asc) as rownumber
             ,case when rlp.orig_store in ('99') then 'ECOMM'
                when rlp.orig_store in ('50', '200') then 'TRENDSEND'
                else
                'STORES'
                end as channel
    		 ,case 
    		     when (Woodlands_TX_Distance <=100 and Highland_TX_Distance <=100) and (Woodlands_TX_Distance < Highland_TX_Distance) then 'Woodlands Market Street TX Market'
    			 when (Highland_TX_Distance  <=100 and Woodlands_TX_Distance <=100) and (Highland_TX_Distance < Woodlands_TX_Distance)  then 'Highland Village TX Market'
    		     when Broadway_CA_Distance <=100 then 'Broadway Plaza CA Market'
    			 when University_CA_Distance <=100 then 'University Town Center San Diego CA Market'
    		     when Hyde_Park_FL_Distance <=100 then 'Hyde Park Tampa FL Market'
    			 when Woodlands_TX_Distance <=100 then 	'Woodlands Market Street TX Market'
    			 when Highland_TX_Distance <=100 then 'Highland Village TX Market'
    			 when Southpark_NC_Distance <=100 then 'Southpark NC Market'
    		 end as Market_Region
    		 ,case 
    		     when (Woodlands_TX_Distance <=100 and Highland_TX_Distance <=100) and (Woodlands_TX_Distance < Highland_TX_Distance) then Woodlands_TX_Distance
    			 when (Highland_TX_Distance  <=100 and Woodlands_TX_Distance <=100) and (Highland_TX_Distance < Woodlands_TX_Distance)  then Highland_TX_Distance
    		     when Broadway_CA_Distance <=100 then Broadway_CA_Distance
    			 when University_CA_Distance <=100 then University_CA_Distance
    		     when Hyde_Park_FL_Distance <=100 then Hyde_Park_FL_Distance
    			 when Woodlands_TX_Distance <=100 then 	Woodlands_TX_Distance
    			 when Highland_TX_Distance <=100 then Highland_TX_Distance
    			 when Southpark_NC_Distance <=100 then Southpark_NC_Distance
    		 end as Distance_from_market
    		 ,Latitude
    		 ,Longitude
    		 ,ADDRESS_ID
    
            from edw.dbo.receipt_lines_processed rlp
            left join edw.dbo.dates dte on cast(rlp.orig_date_closed as date) = dte.Date
    		left join edw.dbo.Customer_Market_Distances cmd on rlp.orig_customer_id = cmd.CUSTOMER_ID
    		
    
            where (rlp.orig_customer_id is not null) 
    	 
    
            group by
              rlp.orig_date_closed
             ,rlp.orig_customer_id
             ,rlp.orig_store
             ,rlp.EXTENDED
             ,rlp.orig_date_closed 
    		 ,case 
    		 	 when (Woodlands_TX_Distance <=100 and Highland_TX_Distance <=100) and (Woodlands_TX_Distance < Highland_TX_Distance) then 'Woodlands Market Street TX Market'
    			 when (Highland_TX_Distance  <=100 and Woodlands_TX_Distance <=100) and (Highland_TX_Distance < Woodlands_TX_Distance)  then 'Highland Village TX Market'
    		     when Broadway_CA_Distance <=100 then 'Broadway Plaza CA Market'
    			 when University_CA_Distance <=100 then 'University Town Center San Diego CA Market'
    		     when Hyde_Park_FL_Distance <=100 then 'Hyde Park Tampa FL Market'
    			 when Woodlands_TX_Distance <=100 then 	'Woodlands Market Street TX Market'
    			 when Highland_TX_Distance <=100 then 'Highland Village TX Market'
    			 when Southpark_NC_Distance <=100 then 'Southpark NC Market'
    		 end 
    		 ,case 
    		     when (Woodlands_TX_Distance <=100 and Highland_TX_Distance <=100) and (Woodlands_TX_Distance < Highland_TX_Distance) then Woodlands_TX_Distance
    			 when (Highland_TX_Distance  <=100 and Woodlands_TX_Distance <=100) and (Highland_TX_Distance < Woodlands_TX_Distance)  then Highland_TX_Distance
    		     when Broadway_CA_Distance <=100 then Broadway_CA_Distance
    			 when University_CA_Distance <=100 then University_CA_Distance
    		     when Hyde_Park_FL_Distance <=100 then Hyde_Park_FL_Distance
    			 when Woodlands_TX_Distance <=100 then 	Woodlands_TX_Distance
    			 when Highland_TX_Distance <=100 then Highland_TX_Distance
    			 when Southpark_NC_Distance <=100 then Southpark_NC_Distance
    		 end
    		 ,Latitude
    		 ,Longitude
    		 ,ADDRESS_ID
          )cohort
    
    	  WHERE (orig_date_closed>='2018-11-01') AND (orig_date_closed<='2021-12-31')
    	  and Market_Region is not null
    	  and cohort.rownumber=1
    
    '''

    sm_df = pd.DataFrame([])
    sm_df = pd.read_sql(query, conn)
    sm_df['Market_Region'].value_counts()
    sm_df['year-month'] = sm_df['orig_date_closed'].dt.to_period('M')
    conn.close()

    return sm_df

# In[116]:
@st.cache(allow_output_mutation=True)

def get_all_cust():
    engine = sa.create_engine('mssql+pymssql://data_team_read_only:RYQus6dmCdybLZsUcGde@katniss.evereve.com:1433/edw')
    conn = engine.connect()
    query = '''WITH dc AS (
    select rlp.orig_date_closed
    	   ,rlp.orig_customer_id
    	  ,Row_Number()Over(Partition by rlp.orig_customer_id order by rlp.orig_date_closed asc) as rownumber
    	  from edw.dbo.receipt_lines_processed rlp
    left join celerant.dbo.TB_CUST_ADDRESS ca on rlp.orig_customer_id = ca.CUSTOMER_ID
    left join celerant.dbo.TB_ADDRESS ad on ca.ADDRESS_ID = ad.ADDRESS_ID
    where (rlp.orig_customer_id is not null) 
    group by rlp.orig_date_closed
    	   , rlp.orig_customer_id)
    
    select dc.orig_date_closed
    	   ,dc.orig_customer_id
    	   ,cmd.CUSTOMER_ID
          ,cmd.ADDRESS1
          ,cmd.ADDRESS2
          ,cmd.CITY
          ,cmd.STATE
          ,cmd.ZIP
          ,cmd.Latitude
          ,cmd.Longitude
          ,cmd.Placekey_DLU
    	  , iif(first_location_visited = 100, 'TRENDSEND', iif(first_location_visited = 99, 'ECOMM', 'STORES')) channel
    FROM dc 
    RIGHT JOIN  [edw].[dbo].[Customer_Market_Distances] cmd ON cmd.CUSTOMER_ID = dc.orig_customer_id
    LEFT JOIN edw.dbo.customer_tenure ct ON cmd.CUSTOMER_ID = ct.customer_id
    WHERE dc.orig_date_closed is not null
    and dc.rownumber =1
    '''
    all_cust = pd.DataFrame([])
    all_cust = pd.read_sql(query, conn)
    all_cust['year-month'] = all_cust['orig_date_closed'].dt.to_period('M')
    all_cust['coordinate'] = list(zip(all_cust['Latitude'],all_cust['Longitude']))
    all_cust['STATE'] = all_cust['STATE'].fillna('')
    conn.close()

    return all_cust

# In[127]:
@st.cache(allow_output_mutation=True)
def get_stores():
        engine = sa.create_engine('mssql+pymssql://data_team_read_only:RYQus6dmCdybLZsUcGde@katniss.evereve.com:1433/edw')
        conn = engine.connect()
        query = '''
                SELECT *
          FROM [edw].[dbo].[store_data] 
          WHERE [open_status]='TRUE' AND (store_id !='99')
          AND (store_id !='100') AND (store_id !='2000')
        
        '''
        stores = pd.DataFrame([])
        stores= pd.read_sql(query, conn)
        stores = stores.dropna(subset=['Latitude'])
        stores['coordinates'] =  stores[['Latitude', 'Longitude']].apply(list, axis=1)
        conn.close()

        return stores
    

@st.cache
def num_of_cust_store(radius, store_id,sd,ed):
    store_coord = stores.loc[stores['store_id']==store_id,'coordinates'].values.item()

    west = inverse_haversine(store_coord, radius, Direction.WEST, unit=Unit.MILES)
    north = inverse_haversine(store_coord, radius, Direction.NORTH, unit=Unit.MILES)
    south = inverse_haversine(store_coord, radius, Direction.SOUTH, unit=Unit.MILES)
    east = inverse_haversine(store_coord, radius, Direction.EAST, unit=Unit.MILES)

    # nw = inverse_haversine(store_coord, radius, Direction.NORTHWEST, unit=Unit.MILES)
    # ne = inverse_haversine(store_coord, radius, Direction.NORTHEAST, unit=Unit.MILES)
    # sw = inverse_haversine(store_coord, radius, Direction.SOUTHWEST, unit=Unit.MILES)
    # se = inverse_haversine(store_coord, radius, Direction.SOUTHEAST, unit=Unit.MILES)
    temp_data = all_cust.loc[((all_cust['Latitude']<=(north[0])) & (all_cust['Latitude']>=(south[0])))
                         & ((all_cust['Longitude']<=(east[1])) & (all_cust['Longitude']>=(west[1]))) &
                         ((all_cust['orig_date_closed']>=str(sd))&(all_cust['orig_date_closed']<=str(ed))),
                         ['Latitude','Longitude','channel','orig_date_closed','year-month','CUSTOMER_ID']]

    # temp_data = all_cust.loc[((all_cust['Latitude']<=(north[0])) & (all_cust['Latitude']>=(south[0])))
    #                         & ((all_cust['Latitude']<=nw[0])&(all_cust['Longitude']>=nw[1]))
    #                         & ((all_cust['Latitude']<=ne[0])&(all_cust['Longitude']<=ne[1])) 
    #                         & ((all_cust['Latitude']>=sw[0]) & (all_cust['Longitude']>=sw[1]))
    #                         & ((all_cust['Latitude']>=se[0]) & (all_cust['Longitude']<=se[1]))
    #                         & ((all_cust['Longitude']<=(east[1])) & (all_cust['Longitude']>=(west[1]))),['Latitude','Longitude','channel','orig_date_closed','year-month','CUSTOMER_ID']]
    # print('Store '+str(store_id)+' has '+ str(temp_data.shape[0])+' customers are within the '+str(radius)+' miles radius.')
    return temp_data,store_coord
    
#%%
# data_load_state = st.text('Loading data...')
stores = get_stores()
# data_load_state.text(" get_stores Done! (using st.cache)")

# data_load_state = st.text('Loading data...')
all_cust = get_all_cust()
# data_load_state.text(" get_all_cust Done! (using st.cache)")

# data_load_state = st.text('Loading data...')
sm_df = get_six_market_analysis()
# data_load_state.text(" sm_df Done! (using st.cache)")


#%%

radius = st.slider('Distance from stores/market in miles: ', 1,100,value=20)
store_id  = st.selectbox('Select a Store ID: ',stores.store_id)
market_name  = st.selectbox('Select a market to compare against: ',sm_df['Market_Region'].unique())

start_time,end_time = st.slider(
     "Select the range of dates: ",min(all_cust['orig_date_closed'].dt.date),
     max(all_cust['orig_date_closed'].dt.date),
     value =( min(all_cust['orig_date_closed'].dt.date), max(all_cust['orig_date_closed'].dt.date))
     ,format="MM/YYYY")

# All Customer Analysis
# In[172]:

temp_store,temp_coord = num_of_cust_store(radius, str(store_id),start_time,end_time) 
graph_data = temp_store.groupby(['year-month','channel']).size().reset_index().rename(columns={0:'num of customers'})
graph_data['year-month'] = graph_data['year-month'].astype(str)    
graph_data['type']=('Store '+str(store_id))

# 6 Market Analysis
# In[172]:

sm_data = sm_df.loc[(sm_df['Market_Region']==market_name) & (sm_df['Distance_from_market']<=radius)].groupby(['year-month','Channel']).size().reset_index().rename(columns={0:'Num of Customers'})
sm_data['year-month'] = sm_data['year-month'].astype(str)   
sm_data = sm_data.rename(columns={'Channel':'channel'}) 
sm_data['type']=market_name

#%%
compare_data = graph_data.merge(sm_data,on=['year-month','type'],how='outer')
print(compare_data['num of customers'].isnull().sum())
compare_data['num of customers'] = compare_data['num of customers'].fillna(compare_data['Num of Customers']).astype(int)
print(compare_data['num of customers'].isnull().sum())
compare_data= compare_data[['year-month', 'num of customers', 'type']]

###############

# Main Comparison
#%%
fig = px.bar(compare_data, x="year-month", y='num of customers',color='type',title =(market_name +' COMPARE AGAINST '+'Store '+(str(store_id))))

st.plotly_chart(fig)

# By channel
# In[167]:
st.subheader('Store '+str(store_id)+' Performance (by channels)')

fig = px.bar(graph_data, x="year-month", y='num of customers', color='channel',title=('Store '+str(store_id)+' | '+str(radius)
                      +' mi | '+str(min(graph_data['year-month']))+'-'+str(max(graph_data['year-month']))
                       # + ' | Open Date:'+str(stores.loc[stores['store_id']==store_id,'opening_date'].values.item())
                       +' | '+str(','.join(stores.loc[stores['store_id']==store_id,['city','state']].values.tolist().pop()))
                       +' | Customers: '+str(graph_data['num of customers'].sum())))
st.plotly_chart(fig)

fig = px.line(graph_data, x="year-month", y='num of customers', color='channel')
st.plotly_chart(fig)

st.subheader(market_name+' Performance  (by channels)')

fig = px.bar(sm_data, x="year-month", y='Num of Customers', color='channel',title=(market_name+' | '+str(radius)
                      +' mi | '+str(min(sm_data['year-month']))+'-'+str(max(sm_data['year-month']))
                       +' | Customers: '+str(sm_data['Num of Customers'].sum())))

st.plotly_chart(fig)
fig = px.line(sm_data, x="year-month", y='Num of Customers', color='channel')
st.plotly_chart(fig)



###############
# Store Tabular Data
#%%
st.subheader('Store '+str(store_id)+' Tabular Data')

table_data = temp_store.groupby(['year-month']).size().reset_index().rename(columns={0:'num of customers'})
table_data['year-month'] = table_data['year-month'].astype(str)    

table = ff.create_table(table_data.loc[(pd.to_datetime(table_data['year-month'])>=pd.to_datetime(start_time)) &
                                       (pd.to_datetime(table_data['year-month'])<=pd.to_datetime(end_time))])

st.plotly_chart(table)

st.subheader(market_name+' Tabular Data')

table_data = sm_data.groupby(['year-month']).agg('sum').reset_index()
table_data['year-month'] = table_data['year-month'].astype(str)    

st.plotly_chart(table)

#%%

