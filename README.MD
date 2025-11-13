# Pipeline de Données Financières E2E

![Statut du Pipeline](https://github.com/caphey/real-estate-pipeline/actions/workflows/daily_pipeline.yml/badge.svg)

Ce projet est un pipeline de données **End-to-End (E-L-T)** complet.

L'objectif est de collecter quotidiennement des données boursières depuis une API publique, de les stocker dans un Data Warehouse, de les transformer en modèles analytiques propres, et d'orchestrer ce flux de manière fiable et automatisée.

## 🏛️ Architecture du Pipeline

Ce projet suit une architecture E-L-T moderne orchestrée par un outil de CI/CD.

```
[GitHub Actions (Scheduler)]
       |
       v
[Python Script (Extract-Load)] ----------------> [API (Alpha Vantage)]
       | (Données brutes)
       v
[Docker: PostgreSQL (Schéma: public)]
       |
       v
[dbt (Transform)]
       | (Modèles Staging & Marts)
       v
[Docker: PostgreSQL (Schéma: dbt_analytics)]
       |
       v
[dbt (Test)] ---> (Alerte si échec)
```

## 🛠️ Stack Technique

* **Extraction & Chargement (E-L) :** **Python** (`requests`, `psycopg2`, `python-dotenv`)
* **Infrastructure (DWH) :** **PostgreSQL** (tournant sur **Docker**)
* **Infrastructure as Code (IaC) :** **Docker-Compose**
* **Transformation (T) :** **dbt (Data Build Tool)**
* **Orchestration & CI/CD :** **GitHub Actions**
* **Tests de Qualité :** **dbt test**

## ✨ Fonctionnalités Clés

* **Infrastructure Reproductible :** `docker-compose.yml` permet de lancer une base de données PostgreSQL identique en une seule commande (`docker-compose up`).
* **Tests de Qualité des Données :** Des tests `dbt` sont en place pour garantir la fiabilité des données (ex: `not_null`, `unique`, `accepted_values` et des tests singuliers comme `closing_price > 0`).
* **Orchestration Automatisée :** Le workflow GitHub Actions exécute l'intégralité du pipeline (E-L puis T) tous les jours, et exécute les tests pour valider la production.
* **Gestion des Secrets :** Les clés d'API et mots de passe de BDD sont gérés de manière sécurisée via un fichier `.env` en local et les `GitHub Secrets` pour la production.
* **Documentation & Lignage :** Le projet dbt est documenté. Le lignage des données (de la source brute au "mart" final) peut être visualisé avec `dbt docs generate` & `dbt docs serve`.

## 🚀 Comment l'exécuter en local

1.  **Cloner le dépôt :**
    ```bash
    git clone [https://github.com/](https://github.com/)caphey/real-estate-pipeline.git
    cd real-estate-pipeline
    ```

2.  **Prérequis :**
    * Avoir **Docker Desktop** installé et lancé.
    * Avoir **Python 3.9+**.

3.  **Créer les secrets :**
    Créez un fichier `.env` à la racine du projet (sur la base de `.env.example`) et remplissez-le avec vos identifiants.
    ```bash
    # Exemple pour .env
    API_KEY=VOTRE_CLE_ALPHA_VANTAGE
    DB_USER=postgres
    DB_PASSWORD=realreal
    DB_NAME=stock_data
    ```

4.  **Lancer la Base de Données :**
    ```bash
    docker-compose up -d
    ```

5.  **Installer les dépendances Python :**
    (Il est recommandé de créer un environnement virtuel : `python -m venv venv`)
    ```bash
    pip install -r requirements.txt
    ```

6.  **Configurer le profil dbt :**
    `dbt` cherche son profil dans `~/.dbt/profiles.yml`. Assurez-vous d'avoir un profil pointant vers la base Docker. `dbt init` peut vous y aider.
    ```yaml
    # Exemple de ~/.dbt/profiles.yml
    dbt_stock_project:
      target: dev
      outputs:
        dev:
          type: postgres
          host: localhost
          port: 5432
          user: postgres       # Doit correspondre à votre .env
          pass: realreal # Doit correspondre à votre .env
          dbname: stock_data  # Doit correspondre à votre .env
          schema: dbt_analytics
    ```

7.  **Exécuter le pipeline manuellement :**

    * **Étape E-L (Extract & Load) :**
        ```bash
        python src/extract_load.py
        ```

    * **Étape T (Transform) :**
        ```bash
        dbt run --project-dir ./dbt_stock_project
        ```

    * **Étape QA (Tests) :**
        ```bash
        dbt test --project-dir ./dbt_stock_project
        ```

8.  **Voir la Documentation :**
    ```bash
    dbt docs generate --project-dir ./dbt_stock_project
    dbt docs serve --project-dir ./dbt_stock_project
    ```
    Ouvrez ensuite `http://localhost:8080` dans votre navigateur.