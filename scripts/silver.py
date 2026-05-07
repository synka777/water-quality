from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, StringType, DateType

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_FILE = BASE_DIR / "data" / "water_quality_2026.json"
bronze_path = str(INPUT_FILE)
spark = SparkSession.builder.appName("RawJSON").getOrCreate()
df_raw = spark.read.json(bronze_path) # Détection automatique de l'encodage et du schéma
print("✅ Données brutes chargées depuis JSON")

def clean_date(date_str):
    """Converts ISO string to DateType"""
    if not date_str:
        return None
    return date_str.split("T")[0]

def parse_numeric_result(text_val):
    """
    Parses strings like '<0,005' or '0,010' into floats.
    Handles decimal comma.
    """
    if text_val is None:
        return None
    
    text = str(text_val).replace(",", ".").strip()
    
    if text.startswith("<"):
        # Si "<0.005", on le traite comme 0.005 pour les analyses quantitatives,
        # mais selon la logique métier, on pourrait aussi choisir de le traiter comme une valeur manquante 
        try:
            return float(text[1:])
        except ValueError:
            return None
    else:
        try:
            return float(text)
        except ValueError:
            return None

def categorize_parameter(param_name):
    """Logique de catégorisation basée sur des mots-clés dans le nom du paramètre"""
    if not param_name:
        return "Autre"
    
    p = str(param_name).lower()
    
    if any(x in p for x in ["bactérie", "coli", "enterocoque", "coliforme", "spore"]):
        return "Microbiologie"
    elif any(x in p for x in ["nitrate", "nitrite", "pesticide", "atrazine", "glyphosate", "terbutylazine"]):
        return "Chimie_Pesticides"
    elif any(x in p for x in ["plomb", "cuivre", "aluminium", "chrome", "baryum", "fer", "magnésium"]):
        return "Chimie_Metaux"
    elif "ph" in p or "conductivité" in p or "température" in p:
        return "Physico-Chimie"
    elif "radio" in p or "tritium" in p or "dose" in p:
        return "Radioactivité"
    else:
        return "Autre"
    
# Nettoyage et conversion de la date
df_clean = df_raw.withColumn(
    "date_prelevement_clean", 
    F.to_date(F.col("date_prelevement"))
).withColumn(
    "annee", 
    F.year(F.col("date_prelevement_clean"))
)

# Output: Affichage d'un échantillon des dates nettoyées et des années extraites
# print("Echantillon: Date brute -> Date nettoyée -> Année")
# df_clean.select(
#     "date_prelevement", 
#     "date_prelevement_clean", 
#     "annee"
# ).limit(5).show(truncate=False)

# Vérification de la distribution des années
# print("\nRépartition des années:")
# df_clean.groupBy("annee").count().orderBy("annee").show()

# Traitement des résultats alphanumériques
df_clean = df_clean.withColumn(
    "resultat_valeur_float",
    F.udf(parse_numeric_result, FloatType())(F.col("resultat_alphanumerique"))
)

# Output: Affichage d'un échantillon de la comparaison entre le texte alphanumérique et la valeur flottante parsée
print("Echantillon: Texte alphanumérique -> Valeur flottante parsée")
df_clean.select(
    "libelle_parametre",
    "resultat_alphanumerique", 
    "resultat_valeur_float",
    "libelle_unite"
).limit(10).show(truncate=False)

# Vérification des cas où le parsing a échoué (valeur alphanumérique non nulle mais valeur float nulle)
failed_parse = df_clean.filter(
    (F.col("resultat_alphanumerique").isNotNull()) & 
    (F.col("resultat_valeur_float").isNull())
).count()
print(f"\n⚠️  Lignes avec échec de parsing: {failed_parse}")

df_clean = df_clean.withColumnRenamed("libelle_parametre", "parametre_nom") \
                   .withColumnRenamed("nom_commune", "commune") \
                   .withColumnRenamed("nom_departement", "departement") \
                   .withColumnRenamed("code_departement", "code_dept")

print("✅ Noms de colonnes standardisés")

# Catégorisation des paramètres
df_clean = df_clean.withColumn(
    "categorie",
    F.udf(categorize_parameter)(F.col("parametre_nom"))
)

# Output: Affichage d'un échantillon des paramètres avec leurs catégories assignées
print("Echantillon: Parameter Name -> Assigned Category")
df_clean.select(
    "parametre_nom", 
    "categorie"
).distinct().limit(15).show(truncate=False)

# Décompte du nombre de mesures par catégorie pour vérifier la répartition
print("\nCount by Category:")
df_clean.groupBy("categorie").count().orderBy(F.desc("count")).show()

# Filtrage final pour ne garder que les lignes avec les champs essentiels non nuls
df_final = df_clean.filter(
    (F.col("code_prelevement").isNotNull()) &
    (F.col("parametre_nom").isNotNull()) &
    (F.col("commune").isNotNull())
)

# Déduplication basée sur les champs clés (code prélevement, code paramètre, date de prélèvement)
rows_before = df_final.count()
df_final = df_final.dropDuplicates(
    subset=["code_prelevement", "code_parametre", "date_prelevement_clean"]
)
rows_after = df_final.count()

# Output: Affichage de statistiques de nettoyage et déduplication
print(f"Lignes avant déduplication: {rows_before}")
print(f"Lignes après déduplication:  {rows_after}")
print(f"Lignes supprimées:        {rows_before - rows_after}")

# Affichage du schéma final et d'un échantillon de données nettoyées
print("\nÉchantillon de données nettoyées:")
df_final.select(
    "code_prelevement", "commune", "departement", "parametre_nom", 
    "resultat_valeur_float", "categorie", "annee"
).limit(5).show(truncate=False)

# Ecriture dans un répertoire local
# Création du répertoire de sortie s'il n'existe pas
import os
os.makedirs("../data/silver", exist_ok=True)

output_path = "../data/silver/water_quality_clean"
df_final.write.mode("overwrite").parquet(output_path)

print(f"\n✅ Données argent sauvegardées dans: {output_path}")

# Quick verification
df_verify = spark.read.parquet(output_path)
df_verify.groupBy("categorie").count().show()
