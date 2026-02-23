from pymongo import MongoClient
import certifi
import os
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

# Connexion sécurisée à MongoDB
client = MongoClient(MONGO_URI, tls=True, tlsCAFile=certifi.where())
db = client["meteo_douala"]
collection = db["donnees_meteo"]

# Supprimer toutes les données
collection.delete_many({})
print("✅ Toutes les données existantes ont été supprimées.")