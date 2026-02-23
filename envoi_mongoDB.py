import os
import time
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
from fpdf import FPDF
from pymongo.errors import ServerSelectionTimeoutError, AutoReconnect, ConfigurationError
import certifi

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
# 📌 NETTOYAGE ET LISSEMENT
# --------------------------------------------------------------------
def nettoyer_tide(df, param="TIDE HEIGHT", window=7):
    if param not in df.columns or df.empty:
        return df
    df[param] = pd.to_numeric(df[param], errors="coerce")
    df = df.dropna(subset=[param])
    df[param + "_med"] = df[param].rolling(window=window, center=True).median()
    df[param + "_smooth"] = df[param + "_med"].rolling(window=window, center=True).mean()
    df[param + "_smooth"].fillna(df[param], inplace=True)
    df[param] = df[param + "_smooth"]
    df.drop(columns=[param + "_med", param + "_smooth"], inplace=True)
    return df

def nettoyer_autres(df, window=3):
    for param in parametres:
        if param in df.columns and param != "TIDE HEIGHT":
            df[param] = pd.to_numeric(df[param], errors="coerce")
            df[param] = df[param].rolling(window=window, center=True).mean().fillna(df[param])
    return df

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
    
    # Nettoyage / lissage avant filtrage
    df_merged = nettoyer_tide(df_merged, "TIDE HEIGHT", window=7)
    df_merged = nettoyer_autres(df_merged, window=3)
    
    # Filtrage des plages valides avec conversion numérique
    for param in parametres:
        if param in df_merged.columns and param in plages_valides:
            df_merged[param] = pd.to_numeric(df_merged[param], errors='coerce')
            min_val, max_val, _ = plages_valides[param]
            df_merged = df_merged[(df_merged[param] >= min_val) & (df_merged[param] <= max_val)]
    
    return df_merged.dropna()

# --------------------------------------------------------------------
# 📌 MONGODB + SAUVEGARDE
# --------------------------------------------------------------------
def taille_bdd(client):
    stats = client["meteo_douala"].command("dbstats")
    taille_MB = stats["storageSize"] / (1024 * 1024)
    print(f"📦 Taille MongoDB : {taille_MB:.2f} Mo")
    return taille_MB

def sauvegarder_et_vider(collection):
    print("⚠️ Taille limite dépassée. Sauvegarde en cours...")
    data = list(collection.find({}))
    if data:
        df = pd.DataFrame(data)
        df.drop(columns="_id", inplace=True, errors="ignore")
        if not os.path.exists(CHEMIN_SAUVEGARDE):
            os.makedirs(CHEMIN_SAUVEGARDE)
        nom_fichier = f"backup_meteo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        chemin_complet = os.path.join(CHEMIN_SAUVEGARDE, nom_fichier)
        df.to_csv(chemin_complet, index=False, encoding="utf-8")
        collection.delete_many({})
        print(f"✅ Données sauvegardées dans : {chemin_complet}")
    else:
        print("ℹ️ Aucune donnée à sauvegarder.")

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
    df_unique = df[~df.apply(lambda row: (row["DateTime"], row["Station"]) in docs_existants, axis=1)]
    if df_unique.empty:
        print("⏳ Aucun nouveau document à insérer.")
        return
    try:
        collection.insert_many(df_unique.to_dict(orient="records"))
        print(f"✅ {len(df_unique)} documents insérés.")
    except Exception as e:
        print(f"❌ Erreur insertion MongoDB : {e}")

# --------------------------------------------------------------------
# 📌 GENERATION PDF
# --------------------------------------------------------------------
def generer_rapport_pdf(df, station):
    if df.empty:
        print(f"📭 Aucun rapport PDF pour {station}.")
        return
    date_du_jour = datetime.now().strftime("%Y-%m-%d")
    fichier_pdf = os.path.join(DOSSIER_PDF, f"rapport_{station}_{datetime.now().strftime('%Y%m%d')}.pdf")
    coords = coordonnees_stations.get(station, {"Longitude": "N/A", "Latitude": "N/A"})
    
    stats = df.describe().loc[["mean", "min", "max"]].round(2).reset_index()
    stats.rename(columns={"index": "Statistique"}, inplace=True)
    
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", "B", 16)
    logo_path = "logo_pad.png"
    if os.path.exists(logo_path):
        pdf.image(logo_path, 10, 8, 33)
    
    pdf.cell(80)
    pdf.cell(30, 10, f"Rapport météo - {station}", ln=True, align="C")
    pdf.set_font("Arial", "", 12)
    pdf.ln(10)
    pdf.cell(0, 10, f"Date : {date_du_jour}", ln=True)
    pdf.cell(0, 10, f"Coordonnées : Lat {coords['Latitude']} / Lon {coords['Longitude']}", ln=True)
    pdf.ln(5)
    
    pdf.set_font("Arial", "B", 12)
    for col in stats.columns:
        pdf.cell(40, 10, col, 1, 0, "C")
    pdf.ln()
    
    pdf.set_font("Arial", "", 11)
    for _, row in stats.iterrows():
        for val in row:
            pdf.cell(40, 10, str(val), 1, 0, "C")
        pdf.ln()
    
    pdf.output(fichier_pdf)
    print(f"📝 Rapport PDF généré : {fichier_pdf}")

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
    print("🟢 Suivi en cours... Ctrl+C pour quitter.")
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
            print(f"🔌 Connexion perdue. Attente réseau... ({e})")
            while True:
                try:
                    client = connexion_mongo()
                    client.server_info()
                    print("🔁 Connexion rétablie.")
                    break
                except Exception as err:
                    print(f"⏳ Hors ligne... Réessai dans 5 sec ({err})")
                    time.sleep(5)

if __name__ == "__main__":
    boucle_suivi()
