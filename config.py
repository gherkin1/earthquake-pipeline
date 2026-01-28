import os
import math
import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
from dotenv import load_dotenv

def count_files_recursive(directory_path):
    count = 0
    for root, dirs, files in os.walk(directory_path):
        count += len(files)
    return count

def count_files_os(directory_path): 
    count = 0 
    for entry in os.scandir(directory_path): 
        if entry.is_file(): 
            count += 1 
    return count

def retrieve_from_api(url, params):
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    responses = openmeteo.weather_api(url, params=params)
    return responses

def convert_to_df(responses):
    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]

    # Process minutely_15 data. The order of variables needs to be the same as requested.
    minutely_15 = response.Minutely15()
    minutely_15_temperature_2m = minutely_15.Variables(0).ValuesAsNumpy()
    minutely_15_wind_speed_10m = minutely_15.Variables(1).ValuesAsNumpy()

    minutely_15_data = {"date": pd.date_range(
        start = pd.to_datetime(minutely_15.Time(), unit = "s", utc = True),
        end =  pd.to_datetime(minutely_15.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = minutely_15.Interval()),
        inclusive = "left"
    )}

    minutely_15_data["temperature_2m"] = minutely_15_temperature_2m
    minutely_15_data["wind_speed_10m"] = minutely_15_wind_speed_10m

    minutely_15_dataframe = pd.DataFrame(data = minutely_15_data)

    minutely_15_dataframe['date'] = pd.to_datetime(minutely_15_dataframe['date'], utc = True)

    #Clean the df
    minutely_15_dataframe = minutely_15_dataframe.drop_duplicates()
    minutely_15_dataframe = minutely_15_dataframe.sort_values(by='date')
    minutely_15_dataframe = minutely_15_dataframe.set_index('date')

    return minutely_15_dataframe

def get_params_history(start_date, end_date, latitude, longitude):
    params = {
        "latitude": latitude,
        "longitude": longitude, #Place of data retrieval
        "start_date": start_date,
        "end_date": end_date,
        "minutely_15": ["temperature_2m", "wind_speed_10m"],
    }
    return params

def get_params_forecast(latitude, longitude, forecast_days):
    params = {
        "latitude":latitude,
        "longitude":longitude,
        "minutely_15":"temperature_2m,wind_speed_10m",
        "timezone":"UTC",
        "forecast_days":forecast_days,
    }
    return params

def upload_to_csv(final_minutely_15_dataframe, table_name, index_bool, path, chunks):
    amnt_chunks = math.ceil(len(final_minutely_15_dataframe)/chunks)
    start = 0
    length = len(final_minutely_15_dataframe)

    for i in range(amnt_chunks):
        start = i * chunks
        end = min(start + chunks, len(final_minutely_15_dataframe))
        temp = final_minutely_15_dataframe.iloc[start:end]

        path = path + f"/table_{table_name}.csv"
        temp.to_csv(path, index = index_bool)

        table_name += 1

def upload_to_sql(name_file, name_upload, count, path):
    #Connecting the mySQL engine
    from sqlalchemy import create_engine
    from urllib.parse import quote_plus
    user = os.getenv("DB_USER")
    pwd  = quote_plus(os.getenv("DB_PASSWORD"))
    host = os.getenv("DB_HOST")
    db   = os.getenv("DB_NAME")
    engine = create_engine(f"mysql+pymysql://{user}:{pwd}@{host}/{db}")

    check = True
    try:
        df2 = pd.read_sql_table(name_upload , engine)

    except:
        df2 = pd.DataFrame()
        check = False

    for i in range(count):
        temp = pd.read_csv(path + f"/table_{name_file}.csv")
        temp['date'] = pd.to_datetime(temp['date'], format='ISO8601')

        if check:
            df2['date'] = pd.to_datetime(df2['date'], format='ISO8601')
            temp = temp[~temp["id"].isin(df2["id"])]

        temp.to_sql(
            name=name_upload,
            con = engine,
            if_exists='append',
            index=False
        )
        name_file += 1
