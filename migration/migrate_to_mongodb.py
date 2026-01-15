import json
import os
from pymongo import MongoClient
import pandas as pd

def migrate_data(json_file_path, mongo_uri, db_name, collection_name):
    """
    Connecte à MongoDB, lit les données d'un fichier JSON et les insère dans une collection.
    """
    try:
        # Connexion à MongoDB
        print(f"Connexion à MongoDB à l'adresse : {mongo_uri}")
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]

        # Charger les données depuis le fichier JSON
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Vérifier que les données sont une liste
        if not isinstance(data, list):
            print("Erreur : Le fichier JSON ne contient pas une liste d'objets.")
            return

        # Vider la collection avant d'insérer de nouvelles données
        print(f"Nettoyage de la collection '{collection_name}'...")
        collection.delete_many({})

        # Insérer les données dans la collection
        print(f"Insertion de {len(data)} documents...")
        result = collection.insert_many(data)
        
        # Mesure de la qualité des données post-migration
        source_count = len(data)
        inserted_count = len(result.inserted_ids)
        
        print(f"Nombre de documents dans le fichier source : {source_count}")
        print(f"Nombre de documents insérés dans MongoDB : {inserted_count}")

        error_rate = (source_count - inserted_count) / source_count if source_count > 0 else 0
        print(f"Taux d'erreur de migration : {error_rate:.2%}")

        if source_count == inserted_count:
            print("Migration des données réussie.")
        else:
            print("Erreur : Le nombre de documents insérés ne correspond pas au nombre de documents source.")

    except FileNotFoundError:
        print(f"Erreur : Le fichier '{json_file_path}' n'a pas été trouvé.")
    except Exception as e:
        print(f"Une erreur est survenue : {e}")

if __name__ == "__main__":
    # Configuration depuis les variables d'environnement ou valeurs par défaut
    JSON_FILE_PATH = os.getenv('JSON_FILE_PATH', 'transformed_data/data_for_mongodb.json')
    MONGO_URI = os.getenv('MONGO_URI', "mongodb://localhost:27017/")
    DB_NAME = os.getenv('DB_NAME', "greenandcoop")
    COLLECTION_NAME = os.getenv('COLLECTION_NAME', "weather_stations")

    # Exécuter la migration
    migrate_data(JSON_FILE_PATH, MONGO_URI, DB_NAME, COLLECTION_NAME)
