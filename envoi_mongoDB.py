import os
import time
import pandas as pd
import certifi
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
from fpdf import FPDF
from pymongo.errors import ServerSelectionTimeoutError, AutoReconnect, ConfigurationError
from scipy.signal import savgol_filter

# === 📌 CONFIGURATION ===
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
CHEMIN_SAUVEGARDE = "C:\\Valeport Software\\TideMaster Express\\Data\\Daily"
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

# ============================
# LECTURE FICHIERS
# ============================

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
    nom_fichier = os.path.join(CHEMIN_SAUVEGARDE, f"{station} {param}.txt")  # ✅ CORRIGÉ
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

        df[param] = pd.to_numeric(df[param], errors="coerce")
        df = df[["DateTime", param]].dropna()

        # === TRAITEMENT MARÉE ===
        if param == "TIDE HEIGHT":
            df = df.set_index("DateTime")
            df = df.resample("5min").mean().interpolate()

            # suppression sauts non physiques
            df = df[(df[param].diff().abs() < 1) | (df[param].diff().isna())]

            # 🔁 re-interpolation pour garder grille 5 min
            df = df.resample("5min").mean().interpolate()

            if len(df) >= 11:
                df[param] = savgol_filter(df[param], window_length=11, polyorder=2)

            df = df.reset_index()

        return df

    except Exception as e:
        print(f"❌ Erreur lecture {nom_fichier} : {e}")
        return pd.DataFrame()

# ============================
# FUSION
# ============================

def fusionner_donnees_station(station):
    dfs = []
    for param in parametres:
        df = lire_fichier_param(station, param)
        if not df.empty:
            dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    df_merged = dfs[0]
    for df in dfs[1:]:
        df_merged = pd.merge(df_merged, df, on="DateTime", how="outer")

    df_merged["Station"] = station
    df_merged["Longitude"] = coordonnees_stations[station]["Longitude"]
    df_merged["Latitude"] = coordonnees_stations[station]["Latitude"]

    df_merged = df_merged.dropna()

    for param in parametres:
        if param in df_merged.columns and param in plages_valides:
            min_val, max_val, _ = plages_valides[param]
            df_merged = df_merged[
                (df_merged[param] >= min_val) &
                (df_merged[param] <= max_val)
            ]

    return df_merged

# ============================
# MONGODB
# ============================

def taille_bdd(client):
    stats = client["meteo_douala"].command("dbstats")
    taille_MB = stats["storageSize"] / (1024 * 1024)
    print(f"📦 Taille MongoDB : {taille_MB:.2f} Mo")
    return taille_MB

def inserer_dans_mongo(df, collection):
    if df.empty:
        return

    existants = set(
        (d["DateTime"], d["Station"])
        for d in collection.find(
            {
                "DateTime": {"$in": df["DateTime"].tolist()},
                "Station": {"$in": df["Station"].tolist()}
            },
            {"_id": 0, "DateTime": 1, "Station": 1}
        )
    )

    df_unique = df[
        ~df.apply(lambda row: (row["DateTime"], row["Station"]) in existants, axis=1)
    ]

    if df_unique.empty:
        print("⏳ Aucun nouveau document à insérer.")
        return

    collection.insert_many(df_unique.to_dict("records"))
    print(f"✅ {len(df_unique)} documents insérés.")

# ============================
# PDF
# ============================

def generer_rapport_pdf(df, station):
    if df.empty:
        print(f"📭 Aucun rapport PDF pour {station}.")
        return

    fichier_pdf = os.path.join(
        DOSSIER_PDF,
        f"rapport_{station}_{datetime.now().strftime('%Y%m%d')}.pdf"
    )

    stats = df.describe().loc[["mean", "min", "max"]].round(2).reset_index()

    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", "B", 16)
    pdf.cell(0, 10, f"Rapport météo - {station}", ln=True, align="C")
    pdf.ln(10)

    pdf.set_font("Arial", "", 11)
    for _, row in stats.iterrows():
        pdf.cell(0, 8, str(row.to_dict()), ln=True)

    pdf.output(fichier_pdf)
    print(f"📝 Rapport PDF généré : {fichier_pdf}")

# ============================
# BOUCLE PRINCIPALE
# ============================

def boucle_suivi():
    print("🟢 Suivi en cours... Ctrl+C pour quitter.")
    while True:
        try:
            client = MongoClient(
                MONGO_URI,
                serverSelectionTimeoutMS=3000,
                tls=True,
                tlsCAFile=certifi.where()
            )

            client.server_info()

            db = client["meteo_douala"]
            collection = db["donnees_meteo"]

            if taille_bdd(client) > TAILLE_LIMITE_MB:
                print("⚠️ Taille limite dépassée.")

            for station in ["SM 1", "SM 2", "SM 3", "SM 4"]:
                df = fusionner_donnees_station(station)
                inserer_dans_mongo(df, collection)
                generer_rapport_pdf(df, station)

            time.sleep(10)

        except (ServerSelectionTimeoutError, AutoReconnect, OSError, ConfigurationError) as e:
            print(f"🔌 Connexion perdue : {e}")
            time.sleep(5)

if __name__ == "__main__":
    boucle_suivi()
