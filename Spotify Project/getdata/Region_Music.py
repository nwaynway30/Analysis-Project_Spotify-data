import base64
import json
import os
import requests
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

client_id1 = os.getenv("client_id")
client_secret1 = os.getenv("client_secret")
def get_token():
    authstring = client_id1 + ":" + client_secret1
    authbyte = authstring.encode("utf-8")
    auth_base64 = str(base64.b64encode(authbyte),"utf-8")
    url = "https://accounts.spotify.com/api/token"
    headers = {  
        "Authorization": f"Basic {auth_base64}",  
        "Content-Type": "application/x-www-form-urlencoded"  
    }  
    data = {  
        "grant_type": "client_credentials"  
    }
    response = requests.post(url, headers=headers, data=data)  
    return response.json().get('access_token')
token = get_token()
def get_header(token):
    return {"Authorization": "Bearer "+ token}

def search_for_track(token, track_name):
    url = "https://api.spotify.com/v1/search"
    headers = get_header(token)
    query = f"?q={track_name}&type=track&limit=1"
    query_url = url + query
    result = requests.get(query_url, headers = headers)
    json_result = json.loads(result.content)
    return json_result
data = search_for_track(token,"happy")

track_data = data.get("tracks", {}).get("items", [])
extracted_data = [{"id": track.get("id"), "available_markets": str(track.get("available_markets", []))} for track in track_data]

# Print extracted data
for track in extracted_data:
    print(f"Track ID: {track['id']}")
    print(f"Available Markets: {track['available_markets']}")
    print()
df = pd.DataFrame(extracted_data[0],index=[0])

import requests

def get_tracks_available_markets_batch(token, track_ids):
    """
    Fetch the available markets for a list of track IDs, even if the list exceeds 50 IDs.

    :param token: Spotify API access token
    :param track_ids: List of track IDs (any size)
    :return: A dictionary with track IDs as keys and their available markets as values
    """
    url = "https://api.spotify.com/v1/tracks"
    headers = {
        "Authorization": f"Bearer {token}"
    }
    # Split track IDs into batches of 50
    batch_size = 50
    batches = [track_ids[i:i + batch_size] for i in range(0, len(track_ids), batch_size)]
    available_markets = {}

    # Process each batch
    for batch in batches:
        track_ids_str = ",".join(batch)  # Join IDs in the batch into a string
        params = {"ids": track_ids_str}
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code != 200:
            print(f"Failed request for batch: {response.content}")
            continue
        
        # Parse response JSON
        tracks_data = response.json()
        for track in tracks_data.get("tracks", []):
            available_markets[track["id"]] = track.get("available_markets", [])
    
    return available_markets

org_tracks = pd.read_csv("/Users/thyneminhtetaungaung/Music Project All file /tracks.csv")
org_tracksid = org_tracks["track_id"].to_list()
market = get_tracks_available_markets_batch(token,org_tracksid)
list1 = {"track_id":[],"available_market": []} 
for track_id, market_list in market.items():
    list1["track_id"].append(track_id)
    list1["available_market"].append(",".join(market_list))

dataframe = pd.DataFrame(list1)
dataframe.to_csv("market2.csv")