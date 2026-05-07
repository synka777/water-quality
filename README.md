# Water Quality Data Pipeline

Pipeline de traitement et d’analyse de données de qualité de l’eau potable en France à partir de l’API publique Hubeau.

Le projet implémente une architecture de type **Medallion** :
- Bronze → ingestion brute
- Silver → nettoyage et standardisation
- Gold → agrégations analytiques

Le pipeline est développé en Python avec PySpark et peut être exécuté localement ou dans Databricks.

---

# Architecture du projet

```text
API Hubeau
    ↓
Bronze Layer (JSON brut)
    ↓
Silver Layer (nettoyage / standardisation)
    ↓
Gold Layer (analytics métier)
```

---

# Technologies utilisées

- Python 3.11
- PySpark
- Databricks Free Edition
- GitHub Actions
- Great Expectations
- Parquet
- Git / GitHub

---

# Structure du projet

```text
water-quality/

├── requirements.txt
├── README.md

├── data/
│   └── water_quality_2026.json

├── scripts/
│   ├── bronze.py
│   ├── silver.py
│   ├── silver_quality.py
│   ├── gold.py
│   └── gold_quality.py

├── notebooks/
│   ├── explo.ipynb
│   ├── silver.ipynb
│   ├── silver_quality.ipynb
│   └── gold.ipynb

├── reports/
│   └── validation_report.html

└── .github/
    └── workflows/
        └── pipeline.yaml
```

---

# Source des données

API publique Hubeau :

https://hubeau.eaufrance.fr/page/api-qualite-eau-potable#/

---

# Fonctionnement des couches

## Bronze Layer

Récupération des données brutes depuis l’API Hubeau.

Format :
- JSON Lines (`.jsonl`)

Contient :
- données non modifiées
- pagination API
- ingestion brute

⚠️ IMPORTANT :

Le script Bronze / ingestion doit être exécuté localement avant l’utilisation des notebooks Databricks.

Exemple :

```bash
python ingestion.py
```

Les données générées seront ensuite utilisées par les notebooks Silver et Gold.

## Bronze Layer

Récupération des données brutes depuis l'API Hubeau.

Format :
- JSON (`.json`)

Contient :
- données non modifiées
- pagination API
- ingestion brute complète

---

## Silver Layer

Nettoyage et standardisation des données :
- suppression des doublons
- filtrage des valeurs nulles
- normalisation des types
- nettoyage des dates
- conversion numérique

⚠️ Limitation rencontrée: Great expectations a été implémenté partiellement; en réalité, les contrôles sont fait en mode "manuel" car l'exécution ne fonctionnait pas sur ma machine à cause de problème de lancement des vérifications. J'ai tenté de changer de version de Python et de Great expectations (Plusieurs versions 18 et une version 17) avant de finalement opter pour une implémentation partielle.

Format :
- Parquet

---

## Gold Layer

Production de tables analytiques :
- taux de conformité par commune
- évolution temporelle des mesures
- top 10 des communes les moins conformes
- analyse des non-conformités par paramètre

Format :
- Parquet

---

# Databricks Free Edition

Le projet peut être synchronisé avec Databricks Free Edition via GitHub Repos.

Les notebooks Databricks permettent :
- l’exécution cloud des traitements Spark
- l’exploration interactive des données
- la visualisation des résultats

⚠️ Limitation rencontrée :

Les scripts d’appel API (ingestion Bronze) ne fonctionnent pas correctement dans Databricks Free Edition.

Des problèmes d’accès réseau ou de restrictions de l’environnement Databricks Free empêchent les appels API de fonctionner de manière fiable.

Pour cette raison :
- l’ingestion Bronze est exécutée localement
- les données générées sont ensuite utilisées dans Databricks

---

# Data Quality

Des contrôles qualité sont exécutés sur les données nettoyées :
- colonnes critiques non nulles
- cohérence temporelle
- validation des valeurs numériques
- cohérence des agrégations métier

---

# Exécution locale

## Installation

Créer un environnement virtuel :

```bash
python -m venv .venv
source .venv/bin/activate
```

Installer les dépendances :

```bash
pip install -r requirements.txt
```

---

# Lancer le pipeline

## Ingestion Bronze

```bash
python ./scripts/bronze.py
```

## Silver Layer

```bash
python ./scripts/silver.py
```

## Silver Quality Checks

```bash
python ./scripts/silver_quality.py
```

## Gold Layer

```bash
python ./scripts/gold.py
```

## Gold Quality Checks

```bash
python ./scripts/gold_quality.py
```

---

# GitHub Actions

Le projet inclut un workflow GitHub Actions permettant :
- l’installation automatique des dépendances
- l’exécution du pipeline
- la validation du code

Workflow :

```text
.github/workflows/pipeline.yaml
```

Étapes du pipeline :
1. Bronze - Récupération des données depuis l'API Hubeau
2. Silver - Nettoyage et standardisation
3. Silver Quality Checks - Validation des données nettoyées
4. Gold - Agrégations analytiques
5. Gold Quality Checks - Validation des agrégations

---

# Exemple d’analyses produites

- communes avec les plus faibles taux de conformité
- paramètres les plus souvent non conformes
- évolution annuelle des mesures
- statistiques agrégées sur la qualité de l’eau

---

# Perspectives d’amélioration

- stockage cloud Azure Data Lake
- Delta Lake
- automatisation Databricks Jobs
- API FastAPI d’exposition des résultats
- Nouvelle tentative d'implémentation Great expectations

---

# Auteur

Projet réalisé dans le cadre d’un projet Data Engineering utilisant PySpark et Databricks.
