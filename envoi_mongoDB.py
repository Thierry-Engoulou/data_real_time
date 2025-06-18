import os
import time
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

# Connexion à MongoDB
client = MongoClient(MONGO_URI)
db = client["meteo_douala"]
collection = db["donnees_meteo"]

# Positions de lecture des fichiers
fichier_positions = {}

# Taille limite de la base en Mo
TAILLE_MAX_MB = 350

def base_trop_grosse():
    stats = db.command("dbstats")
    taille_MB = stats["storageSize"] / (1024 * 1024)
    return taille_MB > TAILLE_MAX_MB

def sauvegarder_et_vider_base():
    print("📦 Sauvegarde et nettoyage de MongoDB...")
    data = list(collection.find({}))
    if data:
        df = pd.DataFrame(data)
        df.drop(columns="_id", inplace=True, errors="ignore")
        df.to_csv("backup_donnees.csv", index=False, encoding="utf-8")
        collection.delete_many({})
        print(f"✅ {len(df)} lignes sauvegardées dans backup_donnees.csv et supprimées de MongoDB.")
    else:
        print("ℹ️ Aucune donnée à sauvegarder.")

def lire_nouvelles_lignes(filename):
    if filename not in fichier_positions:
        fichier_positions[filename] = 0
    try:
        with open(filename, "r", encoding="utf-8") as file:
            file.seek(fichier_positions[filename])
            lignes = file.readlines()
            fichier_positions[filename] = file.tell()
            return lignes
    except FileNotFoundError:
        print(f"⚠️ Fichier introuvable : {filename}")
        return []

def traiter_donnees(station, param):
    filename = f"{station} {param}.txt"
    lignes = lire_nouvelles_lignes(filename)
    if not lignes:
        return None
    lignes_valides = [l for l in lignes if not l.strip().startswith("Date")]
    if not lignes_valides:
        return None
    try:
        df = pd.DataFrame(
            [l.strip().split("\t") for l in lignes_valides],
            columns=["Date", "Time", param, "SD"]
        )
    except Exception as e:
        print(f"❌ Erreur DataFrame {filename} : {e}")
        return None

    df = df[df[param] != "9999.999"]
    df["DateTime"] = pd.to_datetime(
        df["Date"] + " " + df["Time"],
        format='%d/%m/%Y %H:%M:%S',
        dayfirst=True,
        errors="coerce"
    )
    df = df.dropna(subset=["DateTime"])
    df = df[["DateTime", param]].copy()
    df["Station"] = station
    return df

def log_local(df):
    log_file = "log_envoi.csv"
    if not os.path.exists(log_file):
        df.to_csv(log_file, index=False, encoding="utf-8")
    else:
        df.to_csv(log_file, mode='a', index=False, header=False, encoding="utf-8")

def envoyer_donnees():
    if base_trop_grosse():
        sauvegarder_et_vider_base()

    print("🔄 Vérification des nouvelles données...")
    dfs = []
    for station in ["SM 2", "SM 3", "SM 4"]:
        for param in ["AIR TEMPERATURE", "AIR PRESSURE", "HUMIDITY"]:
            df = traiter_donnees(station, param)
            if df is not None and not df.empty:
                dfs.append(df)

    if not dfs:
        return

    df_final = pd.concat(dfs, ignore_index=True)

    # Éliminer les doublons déjà en base
    docs_existants = set()
    for doc in collection.find(
        {"DateTime": {"$in": df_final["DateTime"].tolist()}},
        {"_id": 0, "DateTime": 1, "Station": 1}
    ):
        docs_existants.add((doc["DateTime"], doc["Station"]))

    df_final = df_final[~df_final.apply(lambda row: (row["DateTime"], row["Station"]) in docs_existants, axis=1)]

    if df_final.empty:
        print("⏳ Aucun nouveau document à insérer.")
        return

    try:
        collection.insert_many(df_final.to_dict(orient="records"))
        log_local(df_final)
        print(f"✅ {len(df_final)} nouvelles entrées envoyées à MongoDB et enregistrées dans log_envoi.csv.")
    except Exception as e:
        print(f"❌ Erreur d'envoi : {e}")

if __name__ == "__main__":
    print("🟢 Suivi en temps réel activé...")
    while True:
        envoyer_donnees()
        time.sleep(5)  # Temporisation
