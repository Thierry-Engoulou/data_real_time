# ==========================================================
# SYSTEME MAREGRAPHIQUE 24H/24 – VERSION FINALE COMPLETE
# ==========================================================

import os, time
import pandas as pd
import numpy as np
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
from datetime import datetime
from pymongo.errors import ServerSelectionTimeoutError, AutoReconnect, ConfigurationError
from scipy.signal import savgol_filter
import certifi

# ==========================================================
# CONFIGURATION
# ==========================================================

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

coordonnees_stations = {
    "SM 1": {"Longitude": 9.4601, "Latitude": 3.8048},
    "SM 2": {"Longitude": 9.4950, "Latitude": 3.9165},
    "SM 3": {"Longitude": 9.5877, "Latitude": 3.9916},
    "SM 4": {"Longitude": 9.6857, "Latitude": 4.0539},
}

parametres = [
    "AIR TEMPERATURE","AIR PRESSURE","HUMIDITY","DEWPOINT",
    "WIND SPEED","WIND DIR","SURGE","TIDE HEIGHT"
]

plages_valides = {
    "AIR TEMPERATURE": (-2,50),
    "AIR PRESSURE": (900,1100),
    "HUMIDITY": (0,100),
    "DEWPOINT": (-60,60),
    "WIND SPEED": (0,150),
    "WIND DIR": (0,360),
    "SURGE": (1,5),
    "TIDE HEIGHT": (0,16),
}

fichier_positions = {}

# ==========================================================
# OUTILS TAILLE BASE
# ==========================================================

def convertir_taille_octets(taille):
    if taille < 1024:
        return f"{taille} o"
    elif taille < 1024**2:
        return f"{taille/1024:.2f} Ko"
    elif taille < 1024**3:
        return f"{taille/1024**2:.2f} Mo"
    else:
        return f"{taille/1024**3:.2f} Go"

def afficher_statistiques_base(client, db, collection):

    print("\n📊 ===== STATISTIQUES BASE =====")

    total_docs = collection.count_documents({})
    print(f"📦 Total documents : {total_docs}")

    stats = db.command("dbStats")

    print(f"💾 Données : {convertir_taille_octets(stats.get('dataSize',0))}")
    print(f"🗄️ Stockage : {convertir_taille_octets(stats.get('storageSize',0))}")
    print(f"📁 Total base : {convertir_taille_octets(stats.get('totalSize',0))}")

    pipeline = [
        {"$group": {"_id": "$Station", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]

    for r in collection.aggregate(pipeline):
        print(f"📍 {r['_id']} : {r['count']} documents")

    print("================================\n")

# ==========================================================
# ANALYSE HARMONIQUE
# ==========================================================

def analyse_harmonique_complete(df, colonne):

    df_valid = df.dropna(subset=[colonne])
    if len(df_valid) < 3:
        return df

    t0 = df_valid.index.min()
    t = (df_valid.index - t0).total_seconds().values
    y = df_valid[colonne].values
    y_mean = np.mean(y)
    y = y - y_mean

    omega = {
        "M2": 2*np.pi/(12.42*3600),
        "S2": 2*np.pi/(12*3600),
        "K1": 2*np.pi/(23.93*3600),
        "O1": 2*np.pi/(25.82*3600),
    }

    X = [np.ones(len(t))]
    for w in omega.values():
        X.append(np.sin(w*t))
        X.append(np.cos(w*t))
    X = np.column_stack(X)

    coeffs, _, _, _ = np.linalg.lstsq(X, y, rcond=None)

    t_all = (df.index - t0).total_seconds().values
    X_all = [np.ones(len(t_all))]
    for w in omega.values():
        X_all.append(np.sin(w*t_all))
        X_all.append(np.cos(w*t_all))
    X_all = np.column_stack(X_all)

    if X_all.shape[1] > len(coeffs):
        X_all = X_all[:, :len(coeffs)]

    df[colonne] = X_all @ coeffs + y_mean

    return df

# ==========================================================
# LECTURE INCREMENTALE
# ==========================================================

def lire_lignes_incrementales(nom_fichier):
    if nom_fichier not in fichier_positions:
        fichier_positions[nom_fichier] = 0
    try:
        with open(nom_fichier,"r",encoding="utf-8") as f:
            f.seek(fichier_positions[nom_fichier])
            lignes = f.readlines()
            fichier_positions[nom_fichier] = f.tell()
        return [l for l in lignes if not l.startswith("Date")]
    except:
        return []

def lire_fichier_param(station,param):
    nom = f"{station} {param}.txt"
    lignes = lire_lignes_incrementales(nom)
    if not lignes:
        return pd.DataFrame()

    df = pd.DataFrame(
        [l.strip().split("\t") for l in lignes],
        columns=["Date","Time",param,"SD"]
    )

    df = df[df[param]!="9999.999"]

    df["DateTime"] = pd.to_datetime(
        df["Date"]+" "+df["Time"],
        format="%d/%m/%Y %H:%M:%S",
        errors="coerce"
    )

    df[param] = pd.to_numeric(df[param],errors="coerce")

    return df[["DateTime",param]].dropna()

# ==========================================================
# TRAITEMENT COMPLET STATION
# ==========================================================

def fusionner_donnees_station(station):

    dfs=[]
    for p in parametres:
        dfp = lire_fichier_param(station,p)
        print(f"[DEBUG] {station} - {p}: {len(dfp)} points")
        if not dfp.empty:
            dfs.append(dfp)

    if not dfs:
        return pd.DataFrame()

    df=dfs[0]
    for other in dfs[1:]:
        df=pd.merge(df,other,on="DateTime",how="outer")

    df.sort_values("DateTime",inplace=True)
    df.set_index("DateTime",inplace=True)

    for p in parametres:
        if p in df.columns:

            minv,maxv = plages_valides[p]
            df.loc[(df[p]<minv)|(df[p]>maxv),p]=np.nan

            df[p]=df[p].interpolate(method="time")

            if df[p].count()>=3:
                window=min(11,len(df[p]))
                if window%2==0:
                    window-=1
                if window>=3:
                    tmp=df[p].interpolate(limit_direction="both")
                    df[p]=savgol_filter(tmp,window_length=window,polyorder=2)

    if "TIDE HEIGHT" in df.columns:
        df=analyse_harmonique_complete(df,"TIDE HEIGHT")
        tide=df["TIDE HEIGHT"]
        df["TIDE_HIGH"]=(tide.shift(1)<tide)&(tide.shift(-1)<tide)
        df["TIDE_LOW"]=(tide.shift(1)>tide)&(tide.shift(-1)>tide)

    df["Station"]=station
    df["Longitude"]=coordonnees_stations[station]["Longitude"]
    df["Latitude"]=coordonnees_stations[station]["Latitude"]

    df.reset_index(inplace=True)
    return df.where(pd.notnull(df),None)

# ==========================================================
# MONGO
# ==========================================================

def connexion_mongo():
    return MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=3000,
        tls=True,
        tlsCAFile=certifi.where()
    )

def inserer_dans_mongo(df,collection):
    if df.empty:
        return 0
    ops=[UpdateOne(
        {"DateTime":d["DateTime"],"Station":d["Station"]},
        {"$set":d},
        upsert=True
    ) for d in df.to_dict("records")]

    if ops:
        collection.bulk_write(ops)
    return len(ops)

# ==========================================================
# BOUCLE 24H/24
# ==========================================================

def boucle_suivi():

    print("🟢 SYSTEME ACTIF 24H/24")

    client=connexion_mongo()
    db=client["meteo_douala"]
    collection=db["donnees_meteo"]

    while True:
        try:
            print(f"\n🕒 Cycle {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            total_cycle=0

            for station in coordonnees_stations.keys():
                df=fusionner_donnees_station(station)
                nb=inserer_dans_mongo(df,collection)
                total_cycle+=nb
                print(f"✅ {station} : {nb} documents")

            print(f"🔄 Total cycle : {total_cycle} documents")

            afficher_statistiques_base(client,db,collection)

            time.sleep(10)

        except (ServerSelectionTimeoutError,AutoReconnect,ConfigurationError):
            print("🔌 Reconnexion Mongo...")
            time.sleep(5)

        except Exception as e:
            print("⚠️ Erreur :",e)
            time.sleep(5)

# ==========================================================

if __name__=="__main__":
    boucle_suivi()
