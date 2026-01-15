# Utiliser une image Python légère
FROM python:3.9-slim

# Définir le répertoire de travail dans le conteneur
WORKDIR /app

# Copier le fichier des dépendances et l'installer
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier les scripts et les données nécessaires
COPY migrate_to_mongodb.py .
COPY transformed_data/ ./transformed_data/

# La commande pour exécuter le script lorsque le conteneur démarre
CMD ["python", "migrate_to_mongodb.py"]
