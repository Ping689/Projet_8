import time
import logging
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from datetime import datetime, timedelta

# --- Configuration du Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler() # Affiche les logs dans la console
    ]
)

# --- Configuration de la Connexion ---
MONGO_URI = "mongodb://13.60.208.12:27017/"
DB_NAME = "greenandcoop"
COLLECTION_NAME = "weather_stations"

def test_database_latency():
    """
    Se connecte à la base de données, trouve une date valide,
    puis exécute une requête de test et mesure le temps d'exécution.
    """
    logging.info(f"Tentative de connexion à la base de données sur : {MONGO_URI}")
    client = None
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        logging.info("Connexion à MongoDB réussie.")
        
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]

        # --- Étape 1: Trouver une date valide ---
        logging.info("Recherche d'un document avec un timestamp valide...")
        station_to_find = "La Madeleine"
        sample_document = collection.find_one({
            "station_name": station_to_find,
            "timestamp": {"$ne": None} 
        })

        if not sample_document or "timestamp" not in sample_document:
            logging.error(f"Impossible de trouver un document de test avec un timestamp valide pour la station '{station_to_find}'.")
            return

        timestamp_from_db = sample_document["timestamp"]
        valid_date_obj = None

        if isinstance(timestamp_from_db, datetime):
            logging.debug("Le timestamp est déjà un objet datetime, utilisation directe.")
            valid_date_obj = timestamp_from_db
        elif isinstance(timestamp_from_db, str):
            logging.debug("Le timestamp est une chaîne de caractères, conversion...")
            valid_date_obj = datetime.fromisoformat(timestamp_from_db)
        else:
            logging.error(f"Format de timestamp non supporté : {type(timestamp_from_db)}")
            return

        start_date = valid_date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = start_date + timedelta(days=1)
        
        logging.info(f"Date valide trouvée. Test sur la journée du {start_date.strftime('%Y-%m-%d')}")

        # --- Étape 2: Exécuter la requête de performance ---
        query = {
            "station_name": station_to_find,
            "timestamp": {
                "$gte": start_date if isinstance(timestamp_from_db, datetime) else start_date.isoformat(),
                "$lt": end_date if isinstance(timestamp_from_db, datetime) else end_date.isoformat()
            }
        }

        logging.info(f"Exécution de la requête de test pour la station '{station_to_find}'...")
        
        start_time = time.monotonic()
        documents = list(collection.find(query))
        end_time = time.monotonic()

        duration_ms = (end_time - start_time) * 1000
        
        logging.info("--- Résultats du Test ---")
        logging.info(f"Nombre de documents trouvés : {len(documents)}")
        logging.info(f"Temps d'exécution de la requête : {duration_ms:.2f} ms")
        logging.info("-------------------------")

    except ConnectionFailure as e:
        logging.error("Impossible de se connecter à MongoDB.", exc_info=True)
    except Exception as e:
        logging.error("Une erreur inattendue est survenue.", exc_info=True)
    finally:
        if client:
            client.close()
            logging.info("Connexion à MongoDB fermée.")

if __name__ == '__main__':
    test_database_latency()
