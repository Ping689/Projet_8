import pandas as pd
import os
import json
from datetime import datetime

def convert_excel_to_json():
    """
    Lit les fichiers Excel spécifiés, traite chaque feuille comme une date distincte,
    et sauvegarde les données consolidées dans des fichiers JSON correspondants.
    """
    print("Démarrage de la conversion Excel vers JSON")

    excel_files = ["ichtegem_weather.xlsx", "la_madeleine_weather.xlsx"]
    output_path = '.' # Sauvegarde dans le répertoire courant

    for filename in excel_files:
        file_path = os.path.join(output_path, filename)
        
        if not os.path.exists(file_path):
            print(f"AVERTISSEMENT: Fichier {filename} non trouvé. Il est ignoré.")
            continue

        print(f"Traitement du fichier : {filename}...")
        
        all_records_for_file = []
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
                        # Crée un timestamp complet et le convertit en format string ISO
                        full_timestamp = datetime.combine(sheet_date, record_time)
                        record['timestamp'] = full_timestamp.isoformat()
                        
                        # Supprime l'ancien champ 'Time' qui est un objet non sérialisable
                        del record['Time']
                    
                    all_records_for_file.append(record)

            except Exception as e:
                print(f"  - Erreur lors du traitement de la feuille '{sheet_name}': {e}")
        
        # Sauvegarde du fichier JSON correspondant
        output_filename = os.path.splitext(filename)[0] + '.json'
        output_filepath = os.path.join(output_path, output_filename)
        
        print(f"Sauvegarde des données dans : {output_filename}...")
        with open(output_filepath, 'w', encoding='utf-8') as f:
            json.dump(all_records_for_file, f, indent=4, ensure_ascii=False)

    print("\nConversion terminée.")


if __name__ == '__main__':
    convert_excel_to_json()
