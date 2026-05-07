# gold.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, avg

def run_gold():
    spark = SparkSession.builder \
        .appName("water-quality-gold") \
        .getOrCreate()

    # Load Silver
    df = spark.read.parquet("data/silver/water_quality_clean")

    df.printSchema()

    # -----------------------------
    # 1. Conformité par commune
    # -----------------------------
    df_commune = (
        df.groupBy("commune")
        .agg(
            count("*").alias("total_tests"),
            count(
                when(col("categorie") == "Conforme", True)
            ).alias("nb_conformes"),
            count(
                when(col("categorie") != "Conforme", True)
            ).alias("nb_non_conformes")
        )
        .withColumn(
            "taux_conformite",
            col("nb_conformes") / col("total_tests")
        )
    )

    df_commune.write.mode("overwrite").parquet(
        "data/gold/conformite_commune/"
    )

    # -----------------------------
    # 2. Évolution temporelle
    # -----------------------------
    df_time = (
        df.groupBy("annee", "parametre_nom")
        .agg(
            avg("resultat_valeur_float").alias("moyenne_resultat"),
            count("*").alias("nb_mesures")
        )
    )

    df_time.write.mode("overwrite").parquet(
        "data/gold/evolution_temporelle/"
    )

    # -----------------------------
    # 3. Top 10 communes les moins conformes
    # -----------------------------
    df_worst = (
        df_commune
        .orderBy(col("taux_conformite").asc())
        .limit(10)
    )

    df_worst.write.mode("overwrite").parquet(
        "data/gold/top_10_pires_communes/"
    )

    # -----------------------------
    # 4. Non-conformités par paramètre
    # -----------------------------
    df_non_conform = (
        df.filter(col("categorie") != "Conforme")
        .groupBy("parametre_nom")
        .agg(
            count("*").alias("nb_non_conformes")
        )
        .orderBy(col("nb_non_conformes").desc())
    )

    df_non_conform.write.mode("overwrite").parquet(
        "data/gold/non_conformites_parametre/"
    )

    print("Gold layer successfully created.")


if __name__ == "__main__":
    run_gold()