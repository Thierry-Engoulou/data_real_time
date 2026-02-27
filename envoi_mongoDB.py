# ==========================================================
# VERSION FINALE STABLE 24H/24
# ==========================================================

import os
import time
import gc
import pandas as pd
import numpy as np
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
from fpdf import FPDF
from pymongo.errors import ServerSelectionTimeoutError, AutoReconnect, ConfigurationError
from scipy.signal import savgol_filter
import certifi

# 🔒 IMPORTANT : backend non graphique (évite erreur Tkinter)
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# ==========================================================
# CONFIGURATION
# ==========================================================

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

DOSSIER_PDF = "rapports_pdf"
if not os.path.exists(DOSSIER_PDF):
    os.makedirs(DOSSIER_PDF)

coordonnees_stations = {
    "SM 1": {"Longitude": 9.4601, "Latitude": 3.8048},
    "SM 2": {"Longitude": 9.4950, "Latitude": 3.9165},
    "SM 3": {"Longitude": 9.5877, "Latitude": 3.9916},
    "SM 4": {"Longitude": 9.6857, "Latitude": 4.0539},
}

parametres = [
    "AIR TEMPERATURE", "AIR PRESSURE", "HUMIDITY",
    "DEWPOINT", "WIND SPEED", "WIND DIR",
    "SURGE", "TIDE HEIGHT"
]

plages_valides = {
    "AIR TEMPERATURE": (-2, 50),
    "AIR PRESSURE": (900, 1100),
    "HUMIDITY": (0, 100),
    "DEWPOINT": (-60, 60),
    "WIND SPEED": (0, 150),
    "WIND DIR": (0, 360),
    "SURGE": (1, 5),
    "TIDE HEIGHT": (0, 16),
}

fichier_positions = {}

# ==========================================================
# LECTURE FICHIERS
# ==========================================================

def lire_lignes_incrementales(nom_fichier):
    if nom_fichier not in fichier_positions:
        fichier_positions[nom_fichier] = 0

    try:
        with open(nom_fichier, "r", encoding="utf-8") as f:
            f.seek(fichier_positions[nom_fichier])
            lignes = f.readlines()
            fichier_positions[nom_fichier] = f.tell()
            return [l for l in lignes if not l.startswith("Date")]
    except:
        return []

def lire_fichier_param(station, param):
    nom = f"{station} {param}.txt"
    lignes = lire_lignes_incrementales(nom)

    if not lignes:
        return pd.DataFrame()

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

    df[param] = pd.to_numeric(df[param], errors="coerce")

    return df[["DateTime", param]].dropna()

# ==========================================================
# FUSION + INTERPOLATION + LISSAGE
# ==========================================================

def fusionner_donnees_station(station):

    dfs = []
    for p in parametres:
        df = lire_fichier_param(station, p)
        if not df.empty:
            dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    df = dfs[0]
    for other in dfs[1:]:
        df = pd.merge(df, other, on="DateTime", how="outer")

    df["Station"] = station
    df["Longitude"] = coordonnees_stations[station]["Longitude"]
    df["Latitude"] = coordonnees_stations[station]["Latitude"]

    df = df.sort_values("DateTime")
    df = df.set_index("DateTime")

    # 🔹 NE SUPPRIME PLUS LES LIGNES
    for p in parametres:
        if p in df.columns:
            if p in plages_valides:
                minv, maxv = plages_valides[p]
                df.loc[(df[p] < minv) | (df[p] > maxv), p] = np.nan

            df[p] = df[p].interpolate(method="time")

            if len(df[p].dropna()) >= 5:
                window = min(11, len(df[p]) // 2 * 2 + 1)
                df[p] = savgol_filter(df[p], window_length=window, polyorder=2)

    # 🔹 Détection marées
    if "TIDE HEIGHT" in df.columns:
        tide = df["TIDE HEIGHT"]
        df["TIDE_HIGH"] = (tide.shift(1) < tide) & (tide.shift(-1) < tide)
        df["TIDE_LOW"] = (tide.shift(1) > tide) & (tide.shift(-1) > tide)

    df.reset_index(inplace=True)
    return df

# ==========================================================
# MONGODB
# ==========================================================

def connexion_mongo():
    return MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=3000,
        tls=True,
        tlsCAFile=certifi.where()
    )

def inserer_dans_mongo(df, collection):
    if df.empty:
        return

    for doc in df.to_dict("records"):
        collection.update_one(
            {"DateTime": doc["DateTime"], "Station": doc["Station"]},
            {"$set": doc},
            upsert=True
        )

# ==========================================================
# PDF + GRAPHIQUE
# ==========================================================

def generer_rapport_pdf(df, station):

    if df.empty:
        print(f"📭 Aucun rapport PDF pour {station}")
        return

    df = df.sort_values("DateTime")

    plt.figure(figsize=(12,6))

    for p in parametres:
        if p in df.columns:
            plt.plot(df["DateTime"].values, df[p].values, label=p)

    if "TIDE HEIGHT" in df.columns:
        plt.scatter(
            df["DateTime"][df["TIDE_HIGH"]],
            df["TIDE HEIGHT"][df["TIDE_HIGH"]],
            color="red", marker="^"
        )
        plt.scatter(
            df["DateTime"][df["TIDE_LOW"]],
            df["TIDE HEIGHT"][df["TIDE_LOW"]],
            color="blue", marker="v"
        )

    plt.legend(fontsize=7)
    plt.grid(True)
    plt.tight_layout()

    graph_path = f"{station}_graph.png"
    plt.savefig(graph_path)
    plt.clf()
    plt.close('all')
    gc.collect()

    fichier_pdf = os.path.join(
        DOSSIER_PDF,
        f"rapport_{station}_{datetime.now().strftime('%Y%m%d')}.pdf"
    )

    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", "B", 14)
    pdf.cell(0, 10, f"Rapport Météo - {station}", ln=True, align="C")
    pdf.image(graph_path, x=10, y=30, w=190)
    pdf.output(fichier_pdf)

    os.remove(graph_path)

    print(f"✅ Rapport généré : {fichier_pdf}")

# ==========================================================
# BOUCLE PRINCIPALE
# ==========================================================

def boucle_suivi():

    print("🟢 Suivi en cours... Ctrl+C pour quitter.")

    while True:

        try:
            client = connexion_mongo()
            client.server_info()

            db = client["meteo_douala"]
            collection = db["donnees_meteo"]

            for station in coordonnees_stations.keys():
                df = fusionner_donnees_station(station)
                inserer_dans_mongo(df, collection)
                generer_rapport_pdf(df, station)


            gc.collect()
            time.sleep(10)

        except (ServerSelectionTimeoutError, AutoReconnect, ConfigurationError):
            print("🔌 Connexion perdue. Reconnexion...")
            time.sleep(5)

# ==========================================================

if __name__ == "__main__":
    boucle_suivi()

  
