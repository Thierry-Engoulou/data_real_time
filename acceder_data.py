from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
import json
import os

# 🔒 Charger les variables d'environnement (Mongo URI)
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

# 🌐 Connexion MongoDB
client = MongoClient(MONGO_URI)
db = client["meteo_douala"]
collection = db["donnees_meteo"]

# 🛠 Initialiser Flask
app = Flask(__name__)
CORS(app)

# 🔗 Lien Google Sheets CSV
URL_CSV = "https://docs.google.com/spreadsheets/d/e/2PACX-1vREYCKmqbYUqHgdE9mVY0z1JC5WnTKxqYgs1XjO9BkqtH_kugbyNVO_CDZL87SGFkvh4e4RMKSRaXRK/pub?gid=0&single=true&output=csv"

# === Page d'accueil ===
@app.route("/")
def home():
    return jsonify({
        "message": "✅ API unifiée météo (MongoDB + Google Sheets)",
        "endpoints": {
            "/donnees": "📦 Données en temps réel (MongoDB)",
            "/previsions": "📄 Prévisions météo (Google Sheets)"
        }
    })

# === Données MongoDB ===
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

# === Données Google Sheets ===
@app.route("/previsions", methods=["GET"])
def get_previsions():
    try:
        df = pd.read_csv(URL_CSV)
        df.columns = [col.strip() for col in df.columns]  # Nettoyer noms colonnes
        data = df.to_dict(orient="records")

        return Response(
            json.dumps({"status": "success", "data": data}, ensure_ascii=False, indent=2),
            content_type="application/json; charset=utf-8"
        )

    except Exception as e:
        return Response(
            json.dumps({"status": "error", "message": str(e)}),
            content_type="application/json"
        )

# === Lancer l’API ===
if __name__ == "__main__":
    app.run(debug=True, port=5000)
