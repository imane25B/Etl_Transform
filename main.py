import pandas as pd
import os
from datetime import datetime
import logging

# 🎯 Configuration du logger
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = os.path.join(log_dir, f"log_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logging.info("Début du pipeline de traitement de données")

# 1️⃣ Chargement des données
chemin = './jeu_donnees_etl_5000_lignes.csv'
df_original = pd.read_csv(chemin)
logging.info(f"Chargement du fichier : {chemin} — {df_original.shape[0]} lignes, {df_original.shape[1]} colonnes")

# 2️⃣ Visualisation des premières lignes
print("🔍 Aperçu des données brutes :")
print(df_original.head())

# 3️⃣ Informations générales
print("\n🧱 Infos structurelles :")
df_original.info()

# 4️⃣ Statistiques générales
print("\n📊 Statistiques descriptives :")
print(df_original.describe(include='all'))

print(f"\n📐 Dimensions initiales : {df_original.shape}")

# 5️⃣ Nettoyage des doublons
df_clean = df_original.drop_duplicates()
nb_doublons = df_original.shape[0] - df_clean.shape[0]
logging.info(f"Suppression de {nb_doublons} doublons")
print(f"✅ Après suppression des doublons : {df_clean.shape}")

# 6️⃣ Traitement des valeurs extrêmes (winsorisation)
def limiter_extremes(colonne):
    q1 = colonne.quantile(0.25)
    q3 = colonne.quantile(0.75)
    iqr = q3 - q1
    borne_basse = q1 - 1.5 * iqr
    borne_haute = q3 + 1.5 * iqr
    return colonne.clip(lower=borne_basse, upper=borne_haute)

df_clean['Quantite_vendue'] = limiter_extremes(df_clean['Quantite_vendue'])
df_clean['Prix_unitaire'] = limiter_extremes(df_clean['Prix_unitaire'])
logging.info("Application de la winsorisation sur 'Quantite_vendue' et 'Prix_unitaire'")

# 7️⃣ Suppression des lignes avec des champs critiques manquants
df_clean.dropna(subset=['Nom_produit', 'Prix_unitaire'], inplace=True)
logging.info("Suppression des lignes avec valeurs manquantes dans 'Nom_produit' ou 'Prix_unitaire'")

# 🔎 Contrôle qualité après nettoyage
rapport = {
    "doublons_restants": df_clean.duplicated().sum(),
    "valeurs_nulles": df_clean.isna().sum().to_dict(),
    "quantites_negatives": (df_clean['Quantite_vendue'] < 0).sum(),
    "prix_negatifs": (df_clean['Prix_unitaire'] < 0).sum()
}
logging.info(f"Validation après nettoyage : {rapport}")
print("\n📋 État après nettoyage :")
print(rapport)

# 8️⃣ Création d’une colonne calculée (avec gestion des erreurs)
try:
    df_clean['Total_HT'] = df_clean['Quantite_vendue'] * df_clean['Prix_unitaire']
    logging.info("Colonne 'Total_HT' créée avec succès")
except Exception as err:
    logging.error(f"Erreur lors du calcul de 'Total_HT' : {err}")

# 9️⃣ Normalisation min-max
val_min = df_clean['Total_HT'].min()
val_max = df_clean['Total_HT'].max()
df_clean['Total_HT_normalise'] = (df_clean['Total_HT'] - val_min) / (val_max - val_min)
logging.info("Normalisation Min-Max de la colonne 'Total_HT'")

# 🔁 Agrégation par produit
df_resume = df_clean.groupby("Nom_produit")["Total_HT"].sum().reset_index()
df_resume.rename(columns={"Total_HT": "Chiffre_affaires"}, inplace=True)
logging.info("Agrégation des ventes par produit (Total_HT)")

# 🔄 Documentation finale
doc_resume = {
    "Étapes réalisées": [
        "Suppression des doublons",
        "Nettoyage des valeurs manquantes",
        "Winsorisation sur les colonnes quantitatives",
        "Création de colonnes dérivées",
        "Agrégation des ventes par produit"
    ],
    "Contrôles finaux": {
        "doublons_restants": df_clean.duplicated().sum(),
        "valeurs_manquantes": df_clean.isna().sum().to_dict(),
        "quantites_negatives": (df_clean['Quantite_vendue'] < 0).sum(),
        "prix_negatifs": (df_clean['Prix_unitaire'] < 0).sum()
    }
}
print("\n📘 Résumé des traitements effectués :")
print(doc_resume)
logging.info(f"Résumé final des traitements : {doc_resume}")

# 💾 Sauvegarde du jeu de données nettoyé
os.makedirs("archives", exist_ok=True)
horodatage = datetime.now().strftime("%Y%m%d_%H%M%S")
fichier_sortie = f"archives/donnees_filtrees_{horodatage}.csv"
df_clean.to_csv(fichier_sortie, index=False)
print(f"\n✅ Données sauvegardées dans : {fichier_sortie}")
logging.info(f"Fichier sauvegardé : {fichier_sortie}")
