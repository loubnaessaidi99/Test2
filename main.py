import os
import requests
import json
from requests.auth import HTTPBasicAuth
import datetime
from datetime import datetime, timedelta
from google.cloud import storage
from flask import Flask, request

# Remplacez par votre clé API Lever
API_KEY = 'dTbjBylazOJpu7LFacOLRqnF/Bo+B1gn1YEa0cXeglrzsB2u'
BASE_URL = 'https://api.lever.co/v1/'

# Remplacer le nom du Bucket GCP
BUCKET_NAME = 'lever-loubna-test'

# Fonction pour uploader un fichier JSON directement dans Google Cloud Storage
def upload_json_to_gcs(json_string, gcs_path):
    # Instancie un client GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(gcs_path)

    # Upload le contenu JSON
    blob.upload_from_string(json_string, content_type='application/json')

    print(f"File uploaded to {gcs_path} in bucket {BUCKET_NAME}.")

# Fonction pour récupérer les users avec pagination
def get_all(objects, filters=None, lastupdate=False):
    list_objects = []
    next_cursor = None
    auth = HTTPBasicAuth(API_KEY, '')
    today = datetime.now()
    date_30j = today - timedelta(days=30)

    URL = BASE_URL + objects
    if filters:
        URL = URL + "?" + filters

    if lastupdate:
        last_update = int(date_30j.timestamp())
        last_update_str = str(last_update)
        URL = URL + '?updated_at_start=' + last_update_str + "000"
    i = 0

    while True:
        i = i + 1
        print(i)
        params = {}

        if next_cursor:
            params['offset'] = next_cursor

        response = requests.get(URL, auth=auth, params=params)

        if response.status_code == 200:
            data = response.json()
            list_objects.extend(data.get('data', []))  # Ajoute les users actuels à la liste

            # Vérifie s'il y a un curseur pour la page suivante
            next_cursor = data.get('next')
            if not next_cursor:
                print("fin avant data dir")
                if list_objects:
                    # Afficher les users de manière lisible
                    json_string = '\n'.join(json.dumps(obj) for obj in list_objects)

                    # Écrire directement dans GCS
                    upload_json_to_gcs(json_string, f"{objects}.json")

                return list_objects

        else:
            print(f"Failed to retrieve objects: {response.status_code} - {response.text}")
            break

def get_offers_for_opportunities(list_oppy):
    all_offers = []
    auth = HTTPBasicAuth(API_KEY, '')

    i = 0

    for opportunity in list_oppy:
        i = i + 1
        print(i)
        opportunity_id = opportunity['id']
        URL = BASE_URL + f'opportunities/{opportunity_id}/offers'
        response = requests.get(URL, auth=auth)

        if response.status_code == 200:
            data = response.json()
            all_offers.extend(data.get('data', []))

    # Écrire les offres dans un fichier JSON
    if all_offers:
        json_string = '\n'.join(json.dumps(obj) for obj in all_offers)

        # Écrire directement dans GCS
        upload_json_to_gcs(json_string, 'offers.json')

# Fonction principale déclenchée par un événement HTTP
def main(request):
    list_oppy = get_all("opportunities", None, True)
    get_offers_for_opportunities(list_oppy)
    return 'Data processing complete.'

# Pour le déploiement comme une Cloud Function
app = Flask(__name__)

@app.route('/', methods=['POST'])
def handle_request():
    return main(request)

if __name__ == "__main__":
    app.run(debug=True)
