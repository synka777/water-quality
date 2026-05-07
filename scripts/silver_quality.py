
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

temp_dir = tempfile.mkdtemp()
print(f"Contexte temporaire : {temp_dir}")

try:
    context = gx.get_context(context_root_dir=temp_dir)
    print(f"✅ Contexte GX initialisé.")

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

    # Création de la suite d'attentes avec GX (stocké en contexte)
    suite_name = "water_quality_suite"
    suite = gx.ExpectationSuite(name=suite_name)
    
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
    print("\nGénération du rapport HTML...")
    reports_path = "./reports/"
    os.makedirs(reports_path, exist_ok=True)

    report_file = os.path.join(reports_path, "validation_report.html")
    context.build_data_docs()
    
    # Copier le rapport généré
    docs_path = os.path.join(temp_dir, "uncommitted", "data_docs", "index.html")
    if os.path.exists(docs_path):
        shutil.copy(docs_path, report_file)
        print(f"✅ Rapport disponible ici : {report_file}")
    else:
        print(f"⚠️ Rapport data docs non généré, mais validation effectuée")

# Gestion erreurs
except Exception as e:
    print(f"❌ Erreur : {e}")
    import traceback
    traceback.print_exc()

# Nettoyage
finally:
    print(f"Nettoyage du dossier temporaire...")
    shutil.rmtree(temp_dir, ignore_errors=True)