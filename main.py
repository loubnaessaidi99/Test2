import os
import requests
import json
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
from google.cloud import storage
from flask import Flask, request

# Remplacez par votre clé API Lever
API_KEY = 'dTbjBylazOJpu7LFacOLRqnF/Bo+B1gn1YEa0cXeglrzsB2u'

# Remplacez par l'URL de base de l'API Lever
BASE_URL = 'https://api.lever.co/v1/'

# Remplacez par le nom de votre bucket GCP
BUCKET_NAME = 'test-bucket-lever'

# Initialisation de l'application Flask
app = Flask(__name__)

# Fonction pour uploader un fichier JSON dans Google Cloud Storage
def upload_json_to_gcs(json_string, gcs_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(json_string, content_type='application/json')
    print(f"File uploaded to {gcs_path} in bucket {BUCKET_NAME}.")

# Fonction pour récupérer tous les objets avec pagination depuis l'API Lever
def get_all(objects, filters=None, lastupdate=False):
    list_objects = []
    next_cursor = None
    auth = HTTPBasicAuth(API_KEY, '')
    today = datetime.now()
    date_30j = today - timedelta(days=30)

    URL = BASE_URL + objects
    if filters:
        URL += "?" + filters

    if lastupdate:
        last_update = int(date_30j.timestamp())
        last_update_str = str(last_update)
        URL += f'?updated_at_start={last_update_str}000'

    i = 0

    while True:
        i += 1
        

        params = {}
        if next_cursor:
            params['offset'] = next_cursor

        response = requests.get(URL, auth=auth, params=params)

        if response.status_code == 200:
            data = response.json()
            list_objects.extend(data.get('data', []))

            next_cursor = data.get('next')
            if not next_cursor:
                print("Reached end of data")
                if list_objects:
                    json_string = '\n'.join(json.dumps(obj) for obj in list_objects)
                    upload_json_to_gcs(json_string, f"{objects}.json")

                return list_objects

        else:
            print(f"Failed to retrieve objects: {response.status_code} - {response.text}")
            return None

# Fonction pour obtenir les offres pour les opportunités spécifiques
def get_offers_for_opportunities(list_oppy):
    all_offers = []
    auth = HTTPBasicAuth(API_KEY, '')

    for i, opportunity in enumerate(list_oppy, start=1):
        print(f"Processing opportunity {i}/{len(list_oppy)}")
        opportunity_id = opportunity['id']
        URL = BASE_URL + f'opportunities/{opportunity_id}/offers'
        response = requests.get(URL, auth=auth)

        if response.status_code == 200:
            data = response.json()
            all_offers.extend(data.get('data', []))
        else:
            print(f"Failed to retrieve offers for opportunity {opportunity_id}: {response.status_code} - {response.text}")

    if all_offers:
        json_string = '\n'.join(json.dumps(obj) for obj in all_offers)
        upload_json_to_gcs(json_string, 'offers.json')

# Fonction principale déclenchée par une requête HTTP
@app.route('/hello_http', methods=['POST'])
def hello_http(request=None):  # Accepte maintenant un argument request
    try:
        get_all("users", None, False)
        return 'Data processing complete.'

    except Exception as e:
        print(f"Error in hello_http: {e}")
        return 'Error processing data.', 500

# Point d'entrée pour exécuter localement ou déployer sur Google Cloud Functions
if __name__ == "__main__":
    app.run(debug=True)
