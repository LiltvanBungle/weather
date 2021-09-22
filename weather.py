import json
import sqlite3
import sys
from sqlite3 import Error

import pandas as pd
import requests


class pipeline:
    base_url = "https://www.metaweather.com/api/location/"
    param1 = sys.argv[1]
    woeid = 0
    db = "weatherdb.db"

def main():    
    pl = pipeline()

    #connect to the sqlite db
    def create_connection(db_file):

        conn = None
        try:
            conn = sqlite3.connect(db_file)
            return conn
        except Error as e:
            print(e)

        return conn


    #fetch and transforn data from api into pandas dataframe
    def process_data():
        #build api url
        url = pl.base_url + pl.woeid + "/"
        #parse JSON - response = dict
        response = json.loads(requests.get(url).text)

        if response == []:
            print("No data loaded, check location name and retry")
            print("URL used: " + url)

        else: 
            return transform(response)

    #fetch and transforn data from api into pandas dataframe
    def process_single(date):
        #build api url
        splitDate = date.split("-")
        url = pl.base_url + pl.woeid + "/" + splitDate[0] + "/" + splitDate[1] + "/" + splitDate[2] + "/"
        #parse JSON - response = dict
        response = json.loads(requests.get(url).text)

        if response == []:
            print("No data loaded, check location name and retry")
            print("URL used: " + url)

        else: 
            return transform(response)

        
    def transform(response):
        #create dataframe, add columns for title/lat/lon
        df = pd.DataFrame(response['consolidated_weather'])
        
        df['title'] = response['title']
        
        parent = response['parent']
        df['parent_title'] = parent['title']
        
        #split latt/long as api provides comma separated
        coords = response["latt_long"]
        coords = coords.split(",")
        df['lat'] = coords[0]
        df['lon'] = coords[1]

        df['sun_rise'] = response['sun_rise']
        df['sun_set'] = response['sun_set']

        #drop unwanted columns
        df = df.drop(['id', 'weather_state_abbr', 'wind_direction_compass', 
                    'the_temp', 'wind_speed', 'wind_direction', 'air_pressure', 
                    'visibility', 'predictability'], axis=1)
        
        return(df)
    

    #get woeid for location from api using parameter
    def get_woeid(loc):
        url = "https://www.metaweather.com/api/location/search/?query=" + loc
        response = json.loads(requests.get(url).text)
        mylist = response[0]
        pl.woeid = str(mylist['woeid'])

    #pass to db
    def update_db(data, conn):
        data6 = data.drop(columns=['created'])
        data.to_sql('history', con=conn, if_exists='append', index=False)
        data6.to_sql('six_day', con=conn, if_exists='replace', index=False)    

    #pass to db single date
    def update_db_single(data, conn):
        data.to_sql('history', con=conn, if_exists='append', index=False)
 
        
    #help    
    if pl.param1 == "h" :
        print("type weather.py [location] to load location \n type weather.py [location] [yyyy-mm-dd] to add date to history")
    elif len(sys.argv) > 2 :
        #do single date add
        get_woeid(pl.param1)
        data = process_single(sys.argv[2])
        conn = create_connection(pl.db)
        update_db_single(data, conn)
    else :
        #call functions
        get_woeid(pl.param1)
        data = process_data()
        conn = create_connection(pl.db)
        update_db(data, conn)

if __name__ == '__main__':
    main()