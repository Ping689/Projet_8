import pandas as pd
import json
import os
from datetime import datetime

# Note: Les préfixes S3 ne sont plus utilisés par ce script mais conservés pour information
S3_BUCKET_NAME = 'bucket-s3-stations'
S3_PREFIX_INFOCLIMAT = 'data_stations/infoclimat/'
S3_PREFIX_ICHTEGEM_WEATHER = 'data_stations/ichtegem_weather/'
S3_PREFIX_LA_MADELEINE_WEATHER = 'data_stations/la_madeleine_weather/'
LOCAL_DOWNLOAD_PATH = 'temp_data'
TRANSFORMED_OUTPUT_PATH = 'transformed_data'
ROOT_PROJECT_PATH = '.' # Le script s'exécute depuis la racine du projet 8

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

# Fonctions de Transformation Spécifiques

def transform_excel_sources():
    """
    Traite les sources de données Excel directement pour extraire les dates des noms de feuilles.
    """
    print("\n Traitement des sources Excel")
    excel_records = []
    
    # Mapping des noms de fichiers aux ID de station
    excel_files_map = {
        "la_madeleine_weather.xlsx": "ILAMAD25",
        "ichtegem_weather.xlsx": "IICHTE19"
    }

    for filename, station_id in excel_files_map.items():
        file_path = os.path.join(ROOT_PROJECT_PATH, filename)
        
        if not os.path.exists(file_path):
            print(f"AVERTISSEMENT: Le fichier Excel {filename} n'a pas été trouvé. Il sera ignoré.")
            continue
            
        print(f"Traitement du fichier Excel : {filename}")
        
        station_info = STATION_METADATA.get(station_id, {})
        xls = pd.ExcelFile(file_path)
        
        for sheet_name in xls.sheet_names:
            try:
                # Parse la date depuis le nom de la feuille (format DDMMYY)
                sheet_date = datetime.strptime(sheet_name, '%d%m%y').date()
                
                df_sheet = pd.read_excel(xls, sheet_name=sheet_name)
                
                for _, row in df_sheet.iterrows():
                    record = row.to_dict()
                    
                    # Combine la date de la feuille et l'heure de la ligne
                    if 'Time' in record and hasattr(record['Time'], 'hour'):
                        record_time = record['Time']
                        full_timestamp = datetime.combine(sheet_date, record_time)
                        record['timestamp'] = full_timestamp
                    
                    # Ajoute les métadonnées de la station
                    record.update(station_info)
                    excel_records.append(record)

            except Exception as e:
                print(f"Erreur lors du traitement de la feuille '{sheet_name}' dans {filename}: {e}")
                
    return excel_records

def transform_jsonl_sources():
    """
    Traite les sources de données JSONL (spécifiquement Infoclimat).
    """
    print("\nTraitement des sources JSONL (Infoclimat)")
    jsonl_records = []
    
    if not os.path.exists(LOCAL_DOWNLOAD_PATH):
        print(f"AVERTISSEMENT: Le dossier {LOCAL_DOWNLOAD_PATH} n'existe pas. Les sources JSONL seront ignorées.")
        return jsonl_records

    for filename in os.listdir(LOCAL_DOWNLOAD_PATH):
        if not filename.endswith('.jsonl'):
            continue

        file_path = os.path.join(LOCAL_DOWNLOAD_PATH, filename)
        print(f"Traitement du fichier JSONL : {filename}")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    raw_record = json.loads(line)
                    airbyte_data = raw_record.get('_airbyte_data', {})

                    # Traite uniquement la structure complexe d'Infoclimat
                    if isinstance(airbyte_data, dict) and 'hourly' in airbyte_data:
                        stations_metadata = {s['id']: s for s in airbyte_data.get('stations', [])}
                        hourly_data = airbyte_data.get('hourly', {})
                        
                        for station_id, measurements in hourly_data.items():
                            station_info = stations_metadata.get(station_id, {})
                            if not isinstance(measurements, dict): continue

                            time_steps = measurements.get('time', [])
                            num_records = len(time_steps)
                            
                            for i in range(num_records):
                                flat_record = {
                                    'station_id': station_id,
                                    'station_name': station_info.get('name'),
                                    'latitude': station_info.get('latitude'),
                                    'longitude': station_info.get('longitude'),
                                    'elevation': station_info.get('elevation'),
                                    'timestamp': time_steps[i]
                                }
                                for metric, values in measurements.items():
                                    if metric != 'time' and isinstance(values, list) and len(values) == num_records:
                                        flat_record[metric] = values[i]
                                jsonl_records.append(flat_record)
                except Exception as e:
                    print(f"Erreur lors du traitement de la ligne dans {filename}: {e}")
    return jsonl_records

# Fonctions Utilitaires

def test_data_quality(df, source_name):
    """Effectue des tests de qualité sur le DataFrame."""
    print(f"\nTest de qualité pour {source_name}")
    if df.empty:
        print("Le DataFrame est vide, aucun test ne peut être effectué.")
        return
    if df.isnull().values.any():
        print("Alerte: Des valeurs manquantes ont été détectées.")
        missing_info = df.isnull().sum()
        print(missing_info[missing_info > 0])
    else:
        print("OK: Aucune valeur manquante.")
    if df.duplicated().any():
        print(f"Alerte: {df.duplicated().sum()} doublons détectés.")
    else:
        print("OK: Aucun doublon détecté.")
    print("Types de données des colonnes :")
    print(df.dtypes)
    print("Fin du test de qualité\n")

def clean_and_convert_data(df):
    """Nettoie, convertit les types de données et normalise les colonnes du DataFrame."""
    print("\n Nettoyage et conversion des types de données")
    
    rename_map = {
        'Dew Point': 'dew_point', 'Precip. Rate.': 'precip_rate',
        'Precip. Accum.': 'precip_accum', 'Speed': 'speed', 'Gust': 'gust',
        'Pressure': 'pressure', 'UV': 'uv', 'Humidity': 'humidity',
        'Wind': 'wind', 'Solar': 'solar', 'Temperature': 'temperature'
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    numeric_cols = [
        'dew_point', 'precip_rate', 'precip_accum', 'speed', 'gust',
        'pressure', 'uv', 'humidity', 'solar', 'temperature',
        'latitude', 'longitude', 'elevation'
    ]
    
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(',', '.')
            df[col] = df[col].str.extract(r'(-?\d+\.?\d*)', expand=False)
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    if 'timestamp' in df.columns:
        # Convertit les timestamps (peut être un int/float d'Unix ou un datetime déjà)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', errors='coerce')

    print("Conversion terminée.")
    return df

#  Fonction Principale

def main():
    """
    Orchestre la collecte depuis les différentes sources, la transformation,
    le nettoyage et la sauvegarde des données.
    """
    # Étape 1: Transformer les données des différentes sources
    excel_records = transform_excel_sources()
    jsonl_records = transform_jsonl_sources()
    
    all_records = excel_records + jsonl_records

    if not all_records:
        print("Aucun enregistrement n'a été transformé. Vérifiez les fichiers source.")
        return

    print(f"\n Transformation terminée. {len(all_records)} enregistrements créés au total.")
    
    # Étape 2: Créer un DataFrame et le nettoyer
    final_df = pd.DataFrame(all_records)
    final_df = final_df.drop(columns=['Time'], errors='ignore')
    
    final_df = clean_and_convert_data(final_df)

    # Étape 3: Lancer les tests de qualité
    test_data_quality(final_df, "Données transformées finales")

    # Étape 4: Sauvegarder le résultat
    if not os.path.exists(TRANSFORMED_OUTPUT_PATH):
        os.makedirs(TRANSFORMED_OUTPUT_PATH)
        
    output_file = os.path.join(TRANSFORMED_OUTPUT_PATH, 'data_for_mongodb.json')
    
    # La méthode to_json de Pandas est robuste et gère bien les types de données
    final_df.to_json(output_file, orient='records', indent=4, force_ascii=False, date_format='iso')
        
    print(f"Résultat sauvegardé dans {output_file}")


if __name__ == '__main__':
    main()
