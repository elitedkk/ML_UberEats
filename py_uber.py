import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)

# Input data files are available in the "../input/" directory.
# For example, running this (by clicking run or pressing Shift+Enter) will list all files under the input directory

import os
for dirname, _, filenames in os.walk('/kaggle/input'):
    for filename in filenames:
        print(os.path.join(dirname, filename))

import dateutil.parser
import calendar
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px
#%matplotlib inline

plt.rcParams['figure.figsize'] = (40,10)
plt.style.use('ggplot')

df1 = pd.read_csv('~/workbox/ml_uber/uber-raw-data-jul14.csv')
df2 = pd.read_csv('~/workbox/ml_uber/uber-raw-data-may14.csv')
df3 = pd.read_csv('~/workbox/ml_uber/uber-raw-data-apr14.csv')
df4 = pd.read_csv('~/workbox/ml_uber/uber-raw-data-jun14.csv')
df5 = pd.read_csv('~/workbox/ml_uber/uber-raw-data-sep14.csv')
df6 = pd.read_csv('~/workbox/ml_uber/uber-raw-data-aug14.csv')
#df_location = df1

df_location = pd.concat([df1,df2,df3,df3,df4,df5,df6])

df_location.columns = ['Datetime', 'Lat', 'Lon', 'Base']
df_location.shape

df_location.head()

px.histogram(df_location, x='Base',color = 'Base').show()

px.scatter(df_location,x = 'Lon',y = 'Lat',color = 'Base',height = 800) #.show()

df_location['Datetime'] = pd.to_datetime(df_location['Datetime'], format="%m/%d/%Y %H:%M:%S")
df_location['DayOfWeekNum'] = df_location['Datetime'].dt.dayofweek
#df_location['DayOfWeek'] = df_location['Datetime'].dt.weekday_name
df_location['MonthDayNum'] = df_location['Datetime'].dt.day
df_location['Month'] = df_location['Datetime'].dt.month
df_location['Year'] = df_location['Datetime'].dt.year
df_location['HourOfDay'] = df_location['Datetime'].dt.hour

px.histogram(df_location, x='HourOfDay',color = 'Base', barmode = 'group').show()
px.histogram(df_location, x='DayOfWeekNum',color = 'Base', barmode = 'group').show()
px.histogram(df_location, x='MonthDayNum',color = 'Base', barmode = 'group').show()
px.histogram(df_location, x='Month',color = 'Base', barmode = 'group').show()
df_location.head()
del df_location

df_pickup_loc = pd.read_csv('~/workbox/ml_uber/uber-raw-data-janjune-15.csv')

df_pickup_loc['Pickup_date'] = pd.to_datetime(df_pickup_loc['Pickup_date'], format="%Y-%m-%d %H:%M:%S")
df_pickup_loc['DayOfWeekNum'] = df_pickup_loc['Pickup_date'].dt.dayofweek
#df_pickup_loc['DayOfWeek'] = df_pickup_loc['Pickup_date'].dt.weekday_name
df_pickup_loc['MonthDayNum'] = df_pickup_loc['Pickup_date'].dt.day
df_pickup_loc['Month'] = df_pickup_loc['Pickup_date'].dt.month
df_pickup_loc['Year'] = df_pickup_loc['Pickup_date'].dt.year
df_pickup_loc['HourOfDay'] = df_pickup_loc['Pickup_date'].dt.hour

import collections
counter = collections.Counter(df_pickup_loc['locationID'].values)

def get_most_freq(locs):   
    #print('Done')
    return counter[locs]

df_pickup_loc['location_freq'] = df_pickup_loc['locationID'].apply(lambda x : get_most_freq(x))

df_pickup_loc_order = df_pickup_loc.sort_values(by=['location_freq'], ascending=False).reset_index(drop=True)

df_pickup_loc_order.head(10)

top_locs = list(set(df_pickup_loc_order['location_freq'].values))
top_locs = sorted(top_locs,reverse=True)
#top_locs[:20]

df_pickup_loc_order =  df_pickup_loc_order.loc[df_pickup_loc_order['location_freq'].isin(top_locs[:20])]

df_pickup_loc_order['locationID'] = df_pickup_loc_order['locationID'].apply(lambda x : str(x))

#sns.countplot(x="locationID", data=df_pickup_loc_order,hue = 'DayOfWeek')

sns.countplot(x="locationID", data=df_pickup_loc_order,hue = 'Month')

df_pickup_loc_order.head()

del df_pickup_loc_order
del df_pickup_loc

df_cars_trips = pd.read_csv('~/workbox/ml_uber/Uber-Jan-Feb-FOIL.csv')

df_cars_trips['date'] = pd.to_datetime(df_cars_trips['date'], format="%m/%d/%Y")
df_cars_trips['DayOfWeekNum'] = df_cars_trips['date'].dt.dayofweek
df_cars_trips['DayOfWeek'] = df_cars_trips['date'].dt.weekday_name
df_cars_trips['MonthDayNum'] = df_cars_trips['date'].dt.day
df_cars_trips['Month'] = df_cars_trips['date'].dt.month
df_cars_trips['Year'] = df_cars_trips['date'].dt.year
df_cars_trips['Month_name'] = df_cars_trips['Month'].apply(lambda x: calendar.month_abbr[x])

df_cars_trips['Month'] = df_cars_trips['Month'].apply(lambda x : str(x))

df_cars_trips.head()

px.bar(df_cars_trips, x='dispatching_base_number',y='active_vehicles',color = 'DayOfWeek', barmode = 'group')

px.bar(df_cars_trips, x='dispatching_base_number',y='active_vehicles',color = 'Month_name', barmode = 'group')

px.bar(df_cars_trips, x='dispatching_base_number',y='trips',color = 'DayOfWeek', barmode = 'group')

px.bar(df_cars_trips, x='dispatching_base_number',y='trips',color = 'DayOfWeek', barmode = 'group')

px.bar(df_cars_trips, x='dispatching_base_number',y='trips',color = 'Month_name', barmode = 'group')

px.bar(df_cars_trips, x='MonthDayNum',y='trips',color = 'dispatching_base_number', barmode = 'group')

