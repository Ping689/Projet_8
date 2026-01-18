import pandas as pd
import json
import os
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
from datetime import datetime
import shutil
import logging

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("transformation.log"),
        logging.StreamHandler()
    ]
)

# Configuration
S3_BUCKET_NAME = 'bucket-s3-stations'
S3_PREFIX_INFOCLIMAT = 'data_stations/infoclimat/'
S3_PREFIX_ICHTEGEM_WEATHER = 'data_stations/ichtegem_weather/'
S3_PREFIX_LA_MADELEINE_WEATHER = 'data_stations/la_madeleine_weather/'
LOCAL_DOWNLOAD_PATH = 'temp_data'
TRANSFORMED_OUTPUT_PATH = 'transformed_data'

# Métadonnées des stations fournies
STATION_METADATA = {
    "ILAMAD25": {
        "station_id": "ILAMAD25",
        "station_name": "La Madeleine",
        "latitude": 50.659,
        "longitude": 3.07,
        "elevation": 23,
        "city": "La Madeleine",
        "state": "-/-",
        "hardware": "other",
        "software": "EasyWeatherPro_V5.1.6"
    },
    "IICHTE19": {
        "station_id": "IICHTE19",
        "station_name": "WeerstationBS",
        "latitude": 51.092,
        "longitude": 2.999,
        "elevation": 15,
        "city": "Ichtegem",
        "state": "-/-",
        "hardware": "other",
        "software": "EasyWeatherV1.6.6"
    }
}

# Fonctions de Pipeline

def download_from_s3_securise(bucket_name, s3_prefix, local_dir, extensions_autorisees=None):
    """
    Télécharge les fichiers depuis S3 dans un sous-dossier local spécifique au préfixe.
    """
    source_name = s3_prefix.strip('/').split('/')[-1]
    source_local_dir = os.path.join(local_dir, source_name)
    
    logging.info(f"Téléchargement pour '{source_name}' depuis S3 : s3://{bucket_name}/{s3_prefix}")
    
    if not os.path.exists(source_local_dir):
        os.makedirs(source_local_dir)

    try:
        s3_client = boto3.client('s3')
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=s3_prefix)

        for page in pages:
            if "Contents" not in page: continue
            for obj in page["Contents"]:
                s3_key = obj['Key']
                if s3_key.endswith('/'): continue
                if extensions_autorisees and not any(s3_key.endswith(ext) for ext in extensions_autorisees):
                    continue

                local_file_path = os.path.join(source_local_dir, os.path.basename(s3_key))
                logging.info(f"Téléchargement de {s3_key} vers {local_file_path}...")
                s3_client.download_file(bucket_name, s3_key, local_file_path)
        
        logging.info(f"Téléchargement pour '{source_name}' terminé")
        return True

    except (NoCredentialsError, PartialCredentialsError):
        logging.error("Credentials AWS non configurés.", exc_info=True)
        return False
    except ClientError as e:
        logging.error(f"Erreur S3 inattendue : {e}", exc_info=True)
        return False
    except Exception as e:
        logging.error(f"Une erreur est survenue lors du téléchargement.", exc_info=True)
        return False

def transform_station_parquet(data_path, station_meta):
    """
    Transforme les fichiers Parquet d'une station, en aplatissant les colonnes objet
    et en utilisant la colonne 'timestamp' existante.
    """
    logging.info(f"Traitement des données Parquet pour la station : {station_meta['station_name']}")
    if not os.path.exists(data_path) or not os.listdir(data_path):
        logging.warning(f"Le dossier {data_path} est vide ou n'existe pas.")
        return pd.DataFrame()

    df = pd.read_parquet(data_path)

    # Aplatir les colonnes qui contiennent des dictionnaires
    for col in df.columns:
        if df[col].dtype == 'object':
            # Vérifie si le premier élément non nul est un dictionnaire contenant la clé 'string'
            first_valid = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            if isinstance(first_valid, dict) and 'string' in first_valid:
                logging.info(f"Aplatissement de la colonne '{col}'...")
                df[col] = df[col].apply(lambda x: x.get('string') if isinstance(x, dict) else x)

    df = df.assign(**station_meta)
    
    cols_to_drop = [col for col in df.columns if col.startswith('_airbyte')]
    df = df.drop(columns=cols_to_drop, errors='ignore')

    return df

def transform_infoclimat_parquet(data_path):
    """
    Transforme les fichiers Parquet de la source Infoclimat, qu'ils soient imbriqués ou non.
    """
    logging.info("Traitement des données Parquet pour Infoclimat")
    if not os.path.exists(data_path) or not os.listdir(data_path):
        logging.warning(f"Le dossier {data_path} est vide ou n'existe pas.")
        return pd.DataFrame()

    df = pd.read_parquet(data_path)

    try:
        source_df = df
        # Si les données sont imbriquées dans _airbyte_data, on les normalise
        if '_airbyte_data' in df.columns:
            logging.info("Données trouvées dans '_airbyte_data', normalisation...")
            source_df = pd.json_normalize(df['_airbyte_data'].apply(json.loads))

        # Si les données (maintenant dans source_df) sont encore imbriquées (format Infoclimat)
        if 'hourly' in source_df.columns:
            logging.info("Structure 'hourly' détectée, aplatissement des enregistrements...")
            exploded_records = []
            for _, row in source_df.iterrows():
                stations = {s['id']: s for s in row.get('stations', []) if isinstance(s, dict)}
                hourly_data = row.get('hourly', {})
                if not isinstance(hourly_data, dict): continue

                for station_id, measurements in hourly_data.items():
                    station_info = stations.get(station_id, {})
                    if not isinstance(measurements, dict): continue
                    
                    time_steps = measurements.get('time', [])
                    num_records = len(time_steps)
                    
                    for i in range(num_records):
                        flat_record = {
                            'station_id': station_id,
                            'latitude': station_info.get('latitude'),
                            'longitude': station_info.get('longitude'),
                            'elevation': station_info.get('elevation'),
                            'station_name': station_info.get('name'),
                            'timestamp': time_steps[i]
                        }
                        for metric, values in measurements.items():
                            if metric != 'time' and isinstance(values, list) and i < len(values):
                                flat_record[metric] = values[i]
                        exploded_records.append(flat_record)
            
            df = pd.DataFrame(exploded_records)
        else:
            # Si 'hourly' n'est pas là, on utilise le dataframe source tel quel
            logging.info("Structure 'hourly' non détectée, utilisation des données aplaties.")
            df = source_df

        cols_to_drop = [col for col in df.columns if col.startswith('_airbyte')]
        df = df.drop(columns=cols_to_drop, errors='ignore')
        return df

    except Exception as e:
        logging.error("Erreur critique lors de la transformation des données Infoclimat.", exc_info=True)
        return pd.DataFrame()

# Fonctions Utilitaires

def test_data_quality(df, source_name):
    """Effectue des tests de qualité sur le DataFrame."""
    logging.info(f"Test de qualité pour {source_name}")
    if df.empty:
        logging.warning("Le DataFrame est vide.")
        return
    logging.info(f"OK: {len(df)} lignes trouvées.")
    
    if df.isnull().values.any():
        logging.warning("Alerte: Valeurs manquantes détectées.")
        logging.warning(df.isnull().sum()[df.isnull().sum() > 0])
    
    df_for_duplicates_test = df.copy()
    unhashable_cols = []
    for col in df_for_duplicates_test.columns:
        if any(isinstance(x, (dict, list)) for x in df_for_duplicates_test[col].dropna()):
            unhashable_cols.append(col)

    if unhashable_cols:
        logging.warning(f"Les colonnes suivantes contiennent des objets non 'hashables' et seront ignorées pour le test de doublons: {unhashable_cols}")
        df_for_duplicates_test = df_for_duplicates_test.drop(columns=unhashable_cols)

    if df_for_duplicates_test.duplicated().any():
        logging.warning(f"Alerte: {df_for_duplicates_test.duplicated().sum()} doublons détectés.")
    else:
        logging.info("OK: Aucun doublon détecté.")
        
    logging.info(f"Fin du test de qualité pour {source_name}")

def clean_and_convert_data(df):
    """Nettoie, convertit les types et normalise les colonnes."""
    logging.info("Nettoyage et conversion des types de données")
    rename_map = {
        'Dew Point': 'dew_point', 'Precip. Rate.': 'precip_rate', 'Precip. Accum.': 'precip_accum',
        'Speed': 'speed', 'Gust': 'gust', 'Pressure': 'pressure', 'UV': 'uv', 'Humidity': 'humidity',
        'Wind': 'wind', 'Solar': 'solar', 'Temperature': 'temperature'
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    numeric_cols = [
        'dew_point', 'precip_rate', 'precip_accum', 'speed', 'gust', 'pressure', 'uv',
        'humidity', 'solar', 'temperature', 'latitude', 'longitude', 'elevation'
    ]
    for col in numeric_cols:
        if col in df.columns:
            # Extraire les nombres de chaînes comme '53.1 °F'
            df[col] = df[col].astype(str).str.extract(r'(-?\d+\.?\d*)')
            df[col] = pd.to_numeric(df[col], errors='coerce')

    if 'timestamp' in df.columns:
        # Pandas peut inférer automatiquement le format pour les chaînes ISO 8601
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    logging.info("Conversion terminée.")
    return df

# FONCTION PRINCIPALE 

def main():
    """
    Orchestre le téléchargement, la transformation Parquet,
    le nettoyage et la sauvegarde des données.
    """
    if os.path.exists(LOCAL_DOWNLOAD_PATH): shutil.rmtree(LOCAL_DOWNLOAD_PATH)
    if os.path.exists(TRANSFORMED_OUTPUT_PATH): shutil.rmtree(TRANSFORMED_OUTPUT_PATH)
    os.makedirs(LOCAL_DOWNLOAD_PATH); os.makedirs(TRANSFORMED_OUTPUT_PATH)
    logging.info("Répertoires locaux nettoyés.")

    s3_sources = [S3_PREFIX_INFOCLIMAT, S3_PREFIX_ICHTEGEM_WEATHER, S3_PREFIX_LA_MADELEINE_WEATHER]
    for prefix in s3_sources:
        if not download_from_s3_securise(S3_BUCKET_NAME, prefix, LOCAL_DOWNLOAD_PATH, extensions_autorisees=['.parquet']):
            logging.error(f"Échec du téléchargement pour {prefix}. Arrêt du script."); return

    df_infoclimat = transform_infoclimat_parquet(os.path.join(LOCAL_DOWNLOAD_PATH, 'infoclimat'))
    df_ichtegem = transform_station_parquet(os.path.join(LOCAL_DOWNLOAD_PATH, 'ichtegem_weather'), STATION_METADATA["IICHTE19"])
    df_la_madeleine = transform_station_parquet(os.path.join(LOCAL_DOWNLOAD_PATH, 'la_madeleine_weather'), STATION_METADATA["ILAMAD25"])
    
    all_dfs = [df for df in [df_infoclimat, df_ichtegem, df_la_madeleine] if not df.empty]
    
    if not all_dfs:
        logging.warning("Aucune donnée n'a été transformée. Vérifiez les fichiers Parquet dans S3."); return

    final_df = pd.concat(all_dfs, ignore_index=True)
    logging.info(f"Transformation terminée. {len(final_df)} enregistrements combinés.")
    
    final_df = final_df.drop(columns=['Time'], errors='ignore')
    final_df = clean_and_convert_data(final_df)
    test_data_quality(final_df, "Données transformées finales")

    output_file = os.path.join(TRANSFORMED_OUTPUT_PATH, 'data_for_mongodb.json')
    final_df.to_json(output_file, orient='records', indent=4, force_ascii=False, date_format='iso')
    logging.info(f"Résultat sauvegardé dans {output_file}")

if __name__ == '__main__':
    main()