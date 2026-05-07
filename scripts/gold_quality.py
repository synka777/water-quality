
# GOLD QUALITY CHECKS

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

# Spark session

spark = SparkSession.builder \
    .appName("GoldQualityChecks") \
    .getOrCreate()

BASE_DIR = Path(__file__).resolve().parent.parent
base_path = str(BASE_DIR / "data" / "gold")


# Helper

def print_check(title, success, details=""):
    status = "✅" if success else "❌"
    print(f"{status} {title}")

    if details:
        print(f"   {details}")



# 1. Conformité par commune


print("\n==============================")
print("CHECKING: conformite_commune")
print("==============================")

path = os.path.join(base_path, "conformite_commune")

df = spark.read.parquet(path)


# Check: taux_conformite between 0 and 1


invalid_rate = df.filter(
    (col("taux_conformite") < 0) |
    (col("taux_conformite") > 1)
)

invalid_count = invalid_rate.count()

print_check(
    "taux_conformite between 0 and 1",
    invalid_count == 0,
    f"{invalid_count} invalid rows"
)


# Check: total_tests > 0


invalid_total = df.filter(col("total_tests") <= 0)

invalid_total_count = invalid_total.count()

print_check(
    "total_tests > 0",
    invalid_total_count == 0,
    f"{invalid_total_count} invalid rows"
)


# Check: no null communes


null_communes = df.filter(col("commune").isNull())

null_communes_count = null_communes.count()

print_check(
    "commune not null",
    null_communes_count == 0,
    f"{null_communes_count} null rows"
)


# 2. Evolution temporelle


print("\n==============================")
print("CHECKING: evolution_temporelle")
print("==============================")

path = os.path.join(base_path, "evolution_temporelle")

df = spark.read.parquet(path)


# Check: valid years


invalid_years = df.filter(
    (col("annee") < 2000) |
    (col("annee") > 2030)
)

invalid_years_count = invalid_years.count()

print_check(
    "annee between 2000 and 2030",
    invalid_years_count == 0,
    f"{invalid_years_count} invalid rows"
)


# Check: measurements count > 0


invalid_measures = df.filter(col("nb_mesures") <= 0)

invalid_measures_count = invalid_measures.count()

print_check(
    "nb_mesures > 0",
    invalid_measures_count == 0,
    f"{invalid_measures_count} invalid rows"
)


# 3. Non conformités


print("\n==============================")
print("CHECKING: non_conformites_parametre")
print("==============================")

path = os.path.join(base_path, "non_conformites_parametre")

df = spark.read.parquet(path)


# Check: counts positive


invalid_non_conform = df.filter(col("nb_non_conformes") < 0)

invalid_non_conform_count = invalid_non_conform.count()

print_check(
    "nb_non_conformes >= 0",
    invalid_non_conform_count == 0,
    f"{invalid_non_conform_count} invalid rows"
)


# Check: parameter names not null


null_params = df.filter(col("parametre_nom").isNull())

null_params_count = null_params.count()

print_check(
    "parametre_nom not null",
    null_params_count == 0,
    f"{null_params_count} null rows"
)


# FINAL SUMMARY


print("\n==============================")
print("GOLD QUALITY CHECKS FINISHED")
print("==============================")