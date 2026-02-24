import os
import time
import pandas as pd
import numpy as np
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
from fpdf import FPDF
from pymongo.errors import ServerSelectionTimeoutError, AutoReconnect, ConfigurationError
import certifi
from utide import solve, reconstruct

# --------------------------------------------------------------------
# === 📌 CONFIGURATION ===
# --------------------------------------------------------------------
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
CHEMIN_SAUVEGARDE = "E:\\Marine Weather Data\\Data Update\\data_classe\\save_bd"
TAILLE_LIMITE_MB = 400
DOSSIER_PDF = "rapports_pdf"

coordonnees_stations = {
    "SM 1": {"Longitude": 9.4601, "Latitude": 3.8048},
    "SM 2": {"Longitude": 9.4950, "Latitude": 3.9165},
    "SM 3": {"Longitude": 9.5877, "Latitude": 3.9916},
    "SM 4": {"Longitude": 9.6857, "Latitude": 4.0539},
}

parametres = [
    "AIR TEMPERATURE", "AIR PRESSURE", "HUMIDITY",
    "DEWPOINT", "WIND SPEED", "WIND DIR", "SURGE", "TIDE HEIGHT"
]

plages_valides = {
    "AIR TEMPERATURE": (-2, 50, "°C"),
    "AIR PRESSURE": (900, 1100, "hPa"),
    "HUMIDITY": (0, 100, "%"),
    "DEWPOINT": (-60, 60, "°C"),
    "WIND SPEED": (0, 150, "m/s"),
    "WIND DIR": (0, 360, "°"),
    "SURGE": (1, 5, "m"),
    "TIDE HEIGHT": (0, 16, "m"),
}

fichier_positions = {}
if not os.path.exists(DOSSIER_PDF):
    os.makedirs(DOSSIER_PDF)

# --------------------------------------------------------------------
# 📌 LECTURE DES FICHIERS
# --------------------------------------------------------------------
def lire_lignes_incrementales(nom_fichier):
    if nom_fichier not in fichier_positions:
        fichier_positions[nom_fichier] = 0
    try:
        with open(nom_fichier, "r", encoding="utf-8") as f:
            f.seek(fichier_positions[nom_fichier])
            lignes = f.readlines()
            fichier_positions[nom_fichier] = f.tell()
            return [l for l in lignes if not l.startswith("Date")]
    except FileNotFoundError:
        print(f"⚠️ Fichier introuvable : {nom_fichier}")
        return []

def lire_fichier_param(station, param):
    nom_fichier = f"{station} {param}.txt"
    lignes = lire_lignes_incrementales(nom_fichier)
    if not lignes:
        return pd.DataFrame()
    try:
        df = pd.DataFrame(
            [l.strip().split("\t") for l in lignes],
            columns=["Date", "Time", param, "SD"]
        )
        df = df[df[param] != "9999.999"]
        df["DateTime"] = pd.to_datetime(
            df["Date"] + " " + df["Time"],
            format="%d/%m/%Y %H:%M:%S",
            errors="coerce"
        )
        return df[["DateTime", param]].dropna()
    except Exception as e:
        print(f"❌ Erreur lecture {nom_fichier} : {e}")
        return pd.DataFrame()

# --------------------------------------------------------------------
# 🌊 MODÉLISATION MARÉE ASTRONOMIQUE (SHOM-LIKE)
# --------------------------------------------------------------------
def modeliser_maree_astronomique(df, lat):
    if "TIDE HEIGHT" not in df.columns or df.empty:
        return df

    df = df.copy()
    df["DateTime"] = pd.to_datetime(df["DateTime"])
    df = df.sort_values("DateTime")

    # Rééchantillonnage 5 min
    df = df.set_index("DateTime").resample("5min").mean().interpolate()

    t = df.index.to_numpy()
    h = df["TIDE HEIGHT"].to_numpy()

    if len(h) < 300:  # ≈ 1 jour minimum
        return df.reset_index()

    t_days = (t - t[0]) / np.timedelta64(1, "D")

    coef = solve(t_days, h, lat=lat, method="ols", conf_int="none")
    tide = reconstruct(t_days, coef)

    df["TIDE HEIGHT"] = tide.h
    return df.reset_index()

# --------------------------------------------------------------------
# 📌 FUSION DES DONNÉES PAR STATION
# --------------------------------------------------------------------
def fusionner_donnees_station(station):
    dfs = []
    for param in parametres:
        df = lire_fichier_param(station, param)
        if df.empty:
            continue
        dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    df_merged = dfs[0]
    for df in dfs[1:]:
        df_merged = pd.merge(df_merged, df, on="DateTime", how="outer")

    df_merged["Station"] = station
    df_merged["Longitude"] = coordonnees_stations[station]["Longitude"]
    df_merged["Latitude"] = coordonnees_stations[station]["Latitude"]

    # 🌊 Marée théorique (au lieu du bruit)
    df_merged = modeliser_maree_astronomique(
        df_merged,
        coordonnees_stations[station]["Latitude"]
    )

    for param in parametres:
        if param in df_merged.columns and param in plages_valides:
            df_merged[param] = pd.to_numeric(df_merged[param], errors="coerce")
            min_val, max_val, _ = plages_valides[param]
            df_merged = df_merged[
                (df_merged[param] >= min_val) &
                (df_merged[param] <= max_val)
            ]

    return df_merged.dropna()

# --------------------------------------------------------------------
# 📌 MONGODB
# --------------------------------------------------------------------
def taille_bdd(client):
    stats = client["meteo_douala"].command("dbstats")
    taille_MB = stats["storageSize"] / (1024 * 1024)
    print(f"📦 Taille MongoDB : {taille_MB:.2f} Mo")
    return taille_MB

def sauvegarder_et_vider(collection):
    data = list(collection.find({}))
    if data:
        df = pd.DataFrame(data)
        df.drop(columns="_id", inplace=True, errors="ignore")
        if not os.path.exists(CHEMIN_SAUVEGARDE):
            os.makedirs(CHEMIN_SAUVEGARDE)
        nom_fichier = f"backup_meteo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        chemin = os.path.join(CHEMIN_SAUVEGARDE, nom_fichier)
        df.to_csv(chemin, index=False, encoding="utf-8")
        collection.delete_many({})
        print(f"✅ Sauvegarde : {chemin}")

def inserer_dans_mongo(df, collection):
    if df.empty:
        return

    docs_existants = set(
        (d["DateTime"], d["Station"])
        for d in collection.find(
            {"DateTime": {"$in": df["DateTime"].tolist()}},
            {"_id": 0, "DateTime": 1, "Station": 1}
        )
    )

    df_unique = df[
        ~df.apply(lambda row: (row["DateTime"], row["Station"]) in docs_existants, axis=1)
    ]

    if df_unique.empty:
        print("⏳ Aucun nouveau document.")
        return

    collection.insert_many(df_unique.to_dict("records"))
    print(f"✅ {len(df_unique)} documents insérés.")

# --------------------------------------------------------------------
# 📌 PDF
# --------------------------------------------------------------------
def generer_rapport_pdf(df, station):
    if df.empty:
        return
    fichier_pdf = os.path.join(DOSSIER_PDF, f"rapport_{station}_{datetime.now().strftime('%Y%m%d')}.pdf")
    stats = df.describe().loc[["mean", "min", "max"]].round(2).reset_index()

    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", "B", 14)
    pdf.cell(0, 10, f"Rapport météo - {station}", ln=True)
    pdf.set_font("Arial", "", 11)

    for _, row in stats.iterrows():
        pdf.cell(0, 8, str(row.to_dict()), ln=True)

    pdf.output(fichier_pdf)
    print(f"📝 PDF généré : {fichier_pdf}")

# --------------------------------------------------------------------
# 📌 BOUCLE PRINCIPALE
# --------------------------------------------------------------------
def connexion_mongo():
    return MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=3000,
        tls=True,
        tlsCAFile=certifi.where()
    )

def boucle_suivi():
    print("🟢 Suivi en cours...")
    while True:
        try:
            client = connexion_mongo()
            client.server_info()

            db = client["meteo_douala"]
            collection = db["donnees_meteo"]

            if taille_bdd(client) > TAILLE_LIMITE_MB:
                sauvegarder_et_vider(collection)

            for station in ["SM 1", "SM 2", "SM 3", "SM 4"]:
                df = fusionner_donnees_station(station)
                inserer_dans_mongo(df, collection)
                generer_rapport_pdf(df, station)

            time.sleep(10)

        except (ServerSelectionTimeoutError, AutoReconnect, OSError, ConfigurationError) as e:
            print(f"🔌 Problème réseau : {e}")
            time.sleep(5)

if __name__ == "__main__":
    boucle_suivi()
