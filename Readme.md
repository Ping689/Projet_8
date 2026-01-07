# Projet_8 - Pipeline de Données Météorologiques

Ce projet met en place un pipeline pour collecter, transformer et unifier des données météorologiques issues de différentes sources pour l'entreprise GreenAndCoop.

Le script principal, `transformation.py`, est conçu pour traiter des données de formats hétérogènes (Excel et JSONL) et produire un fichier JSON unique, propre et prêt à être stocké dans MongoDB.

## Logique de transformation du script `transformation.py`

Le script opère en plusieurs étapes clés pour garantir une sortie de données de haute qualité.

### 1. Double Logique de Lecture des Sources

Le script utilise deux méthodes distinctes pour lire les données en fonction de leur source d'origine :

#### a) Fichiers Excel (Sources Weather Underground)
- **Fichiers cibles** : `ichtegem_weather.xlsx` et `la_madeleine_weather.xlsx`.
- **Processus** : Le script parcourt chaque fichier Excel et lit chacune des feuilles de calcul. La date est extraite directement du nom de chaque feuille (ex: '011024'). Cette date est ensuite combinée avec l'heure de la colonne `Time` pour reconstituer un timestamp complet et précis pour chaque mesure.

#### b) Fichiers JSONL (Source Infoclimat)
- **Fichiers cibles** : Tous les fichiers se terminant par `.jsonl` dans le dossier `temp_data`.
- **Processus** : Le script lit ces fichiers ligne par ligne, en extrayant les données depuis la structure JSON complexe fournie par Airbyte (données contenues dans le champ `_airbyte_data`). Il est spécifiquement adapté pour traiter le format "Infoclimat" qui contient les données de plusieurs stations dans un seul fichier.

### 2. Fusion et Nettoyage
Une fois toutes les données lues, elles sont fusionnées en une seule table. Ensuite, une étape cruciale de nettoyage et de normalisation (`clean_and_convert_data`) est appliquée :
- **Standardisation des noms de colonnes** : Les noms sont convertis en `snake_case` pour la cohérence (ex: `"Dew Point"` devient `dew_point`).
- **Extraction des valeurs numériques** : Le script extrait intelligemment les nombres à partir de chaînes de caractères contenant des unités (ex: `"8.2 mph"` devient le nombre `8.2`).
- **Conversion des types** : Les colonnes sont converties dans des types de données appropriés (`float64` pour les nombres, `datetime64[ns]` pour le timestamp).

### 3. Tests de qualité et Sauvegarde
- **Tests automatisés** : Des tests sont effectués sur le jeu de données final pour compter les valeurs manquantes et les doublons, fournissant un aperçu rapide de la qualité des données sources.
- **Sauvegarde** : Le résultat final est sauvegardé dans le fichier `transformed_data/data_for_mongodb.json`, prêt pour l'importation dans MongoDB.

## Comment fonctionne le script

1.  **Prérequis** : Python
                    Airbyte
                    Bucket S3

2.  **Structure des Fichiers** : Avant de lancer le script, vérifier que le dossier 
`Projet_8` est structuré :
    ```
    Projet_8/
    ├── ichtegem_weather.xlsx
    ├── la_madeleine_weather.xlsx
    ├── transformation.py
    ├── requirements.txt
    └── temp_data/
        └── fichier_infoclimat.jsonl
    ```

3.  **Environnement Virtuel** : Il est fortement recommandé de travailler dans un environnement virtuel.
    ```bash
    # Créer un environnement virtuel
    py -m venv venv

    # Activer l'environnement (Windows PowerShell)
    .\venv\Scripts\Activate.ps1
    ```

4.  **Installation des Dépendances** : Installez toutes les bibliothèques nécessaires.
    ```bash
    pip install -r requirements.txt
    ```

5.  **Exécution** : Lancez simplement le script.
    ```bash
    py transformation.py
    ```

6.  **Résultat** : Un fichier `data_for_mongodb.json` sera créé dans le dossier `transformed_data`.