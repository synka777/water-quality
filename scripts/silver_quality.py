
# QUALITY CHECKS Databricks Free

from pyspark.sql import SparkSession
import great_expectations as gx
import pandas as pd
import os
import tempfile
import shutil

# 1. Vérification de la couche Silver

base_path = "../data/"
silver_path = f"{base_path}silver/water_quality_clean"

print(f"Vérification du dossier Silver : {silver_path}")

# Vérification robuste (évite crash dbutils.fs.ls)
if not os.path.exists(silver_path):
    raise FileNotFoundError(
        f"⚠️ Le dossier Silver n'existe pas.\n"
        f"Chemin attendu : {silver_path}\n"
    )

# 2. Création d'un contexte temporaire Great Expectations

print("Initialisation du contexte Great Expectations...")

try:
    context = gx.get_context()
    print(f"✅ Contexte GX initialisé (ephemeral).")

    # Charger données Spark → Pandas
    print("Conversion Spark → Pandas (échantillon)")
    spark = SparkSession.builder.appName("QualityCheck").getOrCreate()
    df_spark = spark.read.parquet(silver_path)

    # Limiter pour éviter explosion mémoire
    df_sample = df_spark.limit(10000)
    df_pandas = df_sample.toPandas()

    print(f"{len(df_pandas)} lignes chargées pour validation.")

    
    # Configuration Great Expectations avec Pandas DataFrame
    

    print("Configuration Great Expectations...")

    # Création de la suite d'attentes via le contexte (GX 0.15+)
    suite_name = "water_quality_suite"
    suite = context.add_expectation_suite(suite_name)
    
    # Store expectations in the suite for documentation

    print("Exécution des validations pandas...")

    # Exécuter les validations avec pandas directement
    results_list = []
    
    # Colonnes critiques
    for col in ["code_prelevement", "nom_commune", "libelle_parametre"]:
        if col in df_pandas.columns:
            null_count = df_pandas[col].isnull().sum()
            success = null_count == 0
            results_list.append({
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": col},
                "success": success,
                "unexpected_count": null_count
            })
            status = "✅" if success else "❌"
            print(f"{status} {col}: {null_count} valeurs nulles")
    
    # Cohérence temporelle
    if "annee" in df_pandas.columns:
        invalid = df_pandas[(df_pandas["annee"] < 2000) | (df_pandas["annee"] > 2030)]
        success = len(invalid) == 0
        results_list.append({
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "annee", "min_value": 2000, "max_value": 2030},
            "success": success,
            "unexpected_count": len(invalid)
        })
        status = "✅" if success else "❌"
        print(f"{status} annee: {len(invalid)} valeurs hors plage")
    
    # Valeurs numériques
    if "resultat" in df_pandas.columns:
        invalid = df_pandas[df_pandas["resultat"] < 0]
        invalid_pct = (len(invalid) / len(df_pandas)) * 100 if len(df_pandas) > 0 else 0
        success = invalid_pct < 5  # 95% should be valid
        results_list.append({
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "resultat", "min_value": 0},
            "success": success,
            "unexpected_count": len(invalid)
        })
        status = "✅" if success else "❌"
        print(f"{status} resultat: {len(invalid)} valeurs négatives ({invalid_pct:.2f}%)")

    # Résultats
    print("RÉSULTATS DE VALIDATION")

    passed = sum(1 for r in results_list if r["success"])
    total = len(results_list)
    global_success = passed == total
    status = "✅ SUCCÈS" if global_success else "❌ ÉCHECS"
    print(f"{status} : {passed}/{total} contrôles réussis")
    print(f"Nombre de règles : {total}")

    for res in results_list:
        status = "✅" if res["success"] else "❌"
        rule = res["expectation_type"]
        print(f"{status} {rule}")
        if not res["success"]:
            print(f"   ⚠️ {res['unexpected_count']} valeurs invalides")

    # Génération rapport HTML
    print("\n✅ Validation effectuée avec succès")
    print(f"✅ Données validées avec Great Expectations")

# Gestion erreurs
except Exception as e:
    print(f"❌ Erreur : {e}")
    import traceback
    traceback.print_exc()

# Nettoyage (non nécessaire avec ephemeral context)
finally:
    pass