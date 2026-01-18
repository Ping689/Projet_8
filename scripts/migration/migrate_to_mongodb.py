import json
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
import os
import logging

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("migration.log"), # Écrit les logs dans un fichier
        logging.StreamHandler() # Affiche les logs dans la console
    ]
)

# Configuration de la connexion MongoDB
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "greenandcoop"
COLLECTION_NAME = "weather_stations"

# Chemin vers le fichier JSON transformé
JSON_FILE_PATH = os.path.join('transformed_data', 'data_for_mongodb.json')

def migrate_to_mongodb():
    """
    Lit les données depuis un fichier JSON et les insère dans une collection MongoDB.
    La collection est vidée avant l'insertion pour éviter les doublons.
    """
    logging.info("Démarrage de la migration vers MongoDB")

    # Vérification de l'existence du fichier JSON
    if not os.path.exists(JSON_FILE_PATH):
        logging.error(f"Le fichier JSON '{JSON_FILE_PATH}' n'a pas été trouvé.")
        logging.error("Veuillez d'abord exécuter le script 'transformation_parquet.py'.")
        return

    # Lecture des données JSON
    try:
        with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logging.info(f"{len(data)} enregistrements chargés depuis le fichier JSON.")
    except json.JSONDecodeError:
        logging.error(f"Le fichier '{JSON_FILE_PATH}' contient un JSON invalide.", exc_info=True)
        return
    except Exception as e:
        logging.error(f"Erreur inattendue lors de la lecture du fichier JSON.", exc_info=True)
        return

    if not data:
        logging.warning("Le fichier JSON est vide. Aucune donnée à migrer.")
        return

    # Connexion à MongoDB
    client = None  # Initialisation pour le bloc finally
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        # Le ping suivant confirme que la connexion est bien établie
        client.admin.command('ping')
        logging.info("Connexion à MongoDB réussie.")
        
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]

        # Vider la collection pour éviter les doublons lors de ré-exécutions
        logging.info(f"Nettoyage de la collection '{COLLECTION_NAME}'...")
        collection.delete_many({})
        
        # Insertion des données
        logging.info("Insertion des données dans MongoDB...")
        result = collection.insert_many(data)
        logging.info(f"{len(result.inserted_ids)} documents insérés avec succès.")

    except ConnectionFailure:
        logging.error(f"Impossible de se connecter à MongoDB. Vérifiez que le service est bien en cours d'exécution sur {MONGO_URI}", exc_info=True)
    except OperationFailure as e:
        logging.error(f"Erreur d'opération MongoDB : {e.details}", exc_info=True)
    except Exception as e:
        logging.error(f"Une erreur inattendue est survenue avec MongoDB.", exc_info=True)
    finally:
        if client:
            client.close()
            logging.info("Connexion à MongoDB fermée.")

if __name__ == '__main__':
    migrate_to_mongodb()
