import os
import requests
import pandas as pd
import datetime
import schedule
import time 

from config import upload_to_csv, upload_to_sql, count_files_os

time_period = 3600

milliseconds_timestamp = datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000 
seconds_timestamp = milliseconds_timestamp / 1000.0
seconds_1_hour_before = seconds_timestamp - time_period
dt_object = datetime.datetime.fromtimestamp(seconds_timestamp, datetime.timezone.utc)
dt_object2 = datetime.datetime.fromtimestamp(seconds_1_hour_before, datetime.timezone.utc)

end_time = dt_object.isoformat()
start_time = dt_object2.isoformat()
end_time = end_time.replace('+00:00', 'Z')
start_time = start_time.replace('+00:00', 'Z')
print(start_time)
print(end_time)

url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
params ={
    "format" : 'geojson',
    "starttime" : start_time,
    'endtime' : end_time
}

response = requests.get(url, params=params).json()
df = pd.json_normalize(response["features"])
df = pd.DataFrame(df)

df['properties.time'] = pd.to_datetime(df['properties.time'], unit = 'ms', utc = True)
df = df[['properties.time','properties.place', 'geometry.coordinates', 'properties.ids']]
new_cols_uneven = df['geometry.coordinates'].apply(pd.Series)
new_cols_uneven.columns = ['latitude', 'longitude', 'depth']
df = pd.concat([df, new_cols_uneven], axis=1)
df = df.drop(columns=['geometry.coordinates'])
df = df.rename(columns={"properties.time":"date", 'properties.place':"place", 'geometry.coordinates':"coordinates", "properties.ids":"id"})
folder_name = "data"
if os.path.isdir(folder_name) != True:
    os.mkdir(folder_name)

table_name = count_files_os(folder_name)

upload_to_csv(df, table_name, False, folder_name, len(df))

upload_to_sql(table_name,"earthquake_data", 1, folder_name)

print(df)
