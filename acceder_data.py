from flask import Flask, jsonify, request
from pymongo import MongoClient
from flask_cors import CORS
from dotenv import load_dotenv
import os
from datetime import datetime


# Charger les variables d'environnement
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

# Connexion MongoDB
client = MongoClient(MONGO_URI)
db = client["meteo_douala"]
collection = db["donnees_meteo"]

# Initialiser Flask
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "http://localhost:5173"}})
# === Route par défaut ===
@app.route("/")
def home():
    return jsonify({
        "message": "✅ API Météo Douala opérationnelle",
        "endpoints": ["/donnees", "/donnees?station=SM 2", "/donnees?limit=10"]
    })

# === Route pour accéder aux données ===
@app.route("/donnees", methods=["GET"])
def get_donnees():
    station = request.args.get("station")
    limit = int(request.args.get("limit", 20))

    query = {}
    if station:
        query["Station"] = station

    cursor = collection.find(query).sort("DateTime", -1).limit(limit)
    donnees = []
    for doc in cursor:
        doc["_id"] = str(doc["_id"])
        if "DateTime" in doc and isinstance(doc["DateTime"], datetime):
            doc["DateTime"] = doc["DateTime"].strftime("%Y-%m-%d %H:%M:%S")
        donnees.append(doc)

    return jsonify(donnees)

# === Lancer l’API ===
if __name__ == "__main__":
    app.run(debug=True, port=5000)
