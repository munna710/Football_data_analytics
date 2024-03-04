
import logging
import pandas as pd
from geopy import Nominatim
import json
from bs4 import BeautifulSoup
from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings
from io import StringIO
import requests
from datetime import datetime
NO_IMAGE = 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png'


def get_wikipedia_page(url):
    

    print("Getting wikipedia page...", url)

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # check if the request is successful

        return response.text
    except requests.RequestException as e:
        print(f"An error occured: {e}")


def get_wikipedia_data(html):
   


    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find_all("table", {"class": "wikitable sortable sticky-header"})

    if not table:
        print("No tables found on the Wikipedia page.")
        return []

    table_rows = table[0].find_all('tr')  # Assuming you want the first table

    return table_rows

def clean_text(text):
    text = str(text).strip()
    text = text.replace('&nbsp', '')
    if text.find(' ♦'):
        text = text.split(' ♦')[0]
    if text.find('[') != -1:
        text = text.split('[')[0]
    if text.find(' (formerly)') != -1:
        text = text.split(' (formerly)')[0]

    return text.replace('\n', '')


def extract_wikipedia_data(**kwargs):
    url = kwargs['url']
    html = get_wikipedia_page(url)
    rows = get_wikipedia_data(html)

    if rows is None:
        print("No data found. Exiting.")
        return "No data found."

    data = []

    for i in range(1, len(rows)):
        tds = rows[i].find_all('td')
        values = {
            'rank': i,
            'stadium': clean_text(tds[0].text),
            'capacity': clean_text(tds[1].text).replace(',', '').replace('.', ''),
            'region': clean_text(tds[2].text),
            'country': clean_text(tds[3].text),
            'city': clean_text(tds[4].text),
            'images': 'https://' + tds[5].find('img').get('src').split("//")[1] if tds[5].find('img') else "NO_IMAGE",
            'home_team': clean_text(tds[6].text),
        }
        data.append(values)

    json_rows = json.dumps(data)
    kwargs['ti'].xcom_push(key='rows', value=json_rows)
    data_df = pd.DataFrame(data)
    data_df.to_csv("data/output.csv", index=False)  # Corrected the file name

    return "OK"
# def get_lat_long(country, city):
#     try:
#         geolocator = Nominatim(user_agent='geoapiExercises')
#         location = geolocator.geocode(f'{city}, {country}', timeout=10)  # Set the timeout value as needed

#         if location:
#             return location.latitude, location.longitude
#     except Exception as e:
#         print(f"Error geocoding for {city}, {country}: {e}")

#     return None


def transform_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='extract_data_from_wikipedia')

    data = json.loads(data)

    stadiums_df = pd.DataFrame(data)
   #stadiums_df['location'] = stadiums_df.apply(lambda x: get_lat_long(x['country'], x['stadium']), axis=1)
    stadiums_df['images'] = stadiums_df['images'].apply(lambda x: x if x not in ['NO_IMAGE', '', None] else NO_IMAGE)
    stadiums_df['capacity'] = stadiums_df['capacity'].astype(int)

    # handle the duplicates
    # duplicates = stadiums_df[stadiums_df.duplicated(['location'])]
    # duplicates['location'] = duplicates.apply(lambda x: get_lat_long(x['country'], x['city']), axis=1)
    # stadiums_df.update(duplicates)

    # push to xcom
    kwargs['ti'].xcom_push(key='rows', value=stadiums_df.to_json())

    return "OK"

def write_wikipedia_data(**kwargs):
    try:
        data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')
        data = json.loads(data)
        data_df = pd.DataFrame(data)
        
        # Convert DataFrame to CSV
        csv_data = data_df.to_csv(index=False)
        
        # Azure storage account details
        account_name = 'dataengineeringmunna710'
        account_key = 'WthwgsbwvwOTy30YKRRNbKiJbXV1VjjFfHuBk41qNAfYtqw31DoX/Tq+ESRkQ7J9WBUKKVP9iBvT+AStR/Mchg=='
        
        # Create a DataLakeServiceClient
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", account_name), credential=account_key)
        
        # Create a file system client
        file_system_client = service_client.get_file_system_client(file_system="footballdataeng")
        
        # Create a directory client
        directory_client = file_system_client.get_directory_client("data")
        
        # Get current date and time
        now = datetime.now()
        formatted_date = now.strftime('%Y%m%d%H%M%S')
        
        # Create a file client
        file_client = directory_client.get_file_client(f"cleanedData_{formatted_date}.csv")
        
        # Upload data to file
        file_client.upload_data(csv_data, overwrite=True)
        
        return "OK"
    except Exception as e:
        print(e)