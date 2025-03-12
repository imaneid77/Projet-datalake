# PROJET DATA LAKE - DOCUMENTATION TECHNIQUE 


# 1. Introduction
---
Ce projet met en place un **Data Lake** pour l’ingestion, le nettoyage et l’exposition de données de tweets (BigTech) en trois couches : Raw, Staging et Curated.
Une API Gateway (basée sur FastAPI) permet de consulter les données à chaque étape du pipeline, ainsi que de récupérer des informations de santé et de statistiques.
Enfin, un pipeline Airflow assure l’automatisation et la reproductibilité de l’alimentation du Data Lake.


# 2. Architecture Générale
---
### 1. Couche Raw
Les données brutes (tweets) sont stockées dans un bucket S3 aws simulé par **LocalStack** pour l’environnement de développement.\\
Le script ``unpack_to_raw.py`` récupère les CSV depuis Kaggle (via kagglehub) et les téléverse dans ce bucket.

### 2. Couche Staging
Les données nettoyées sont insérées dans une base **MySQL** pour une première structuration.\\
Le script ``preprocess_to_staging.py`` nettoie et transforme les données avant de les charger dans MySQL (et éventuellement dans un bucket **staging**).


### 3. Couche Curated
Les dernières transformations sont appliquées aux données finales : enrichissement, tokenisation (via un modèle BERT), etc. Ces données sont ensuite stockées dans MongoDB pour plus de souplesse.\\
Le script ``process_to_curated.py`` réalise ces étapes et téléverse également un fichier Parquet final dans le bucket **curated**.


### 4. API Gateway
On a également mis à disposition un service **FastAPI** qui expose différents endpoints :
**/raw** : retourne les données brutes (CSV sur S3).\\
**/staging** : retourne les données staging (MySQL).\\
**/curated** : retourne les données finales (MongoDB).\\
**/health** : vérifie la santé du service (connectivité S3, MySQL, MongoDB).\\
**/stats** : fournit quelques métriques (par exemple, le nombre de lignes par couche).

### 5. Orchestration (Airflow)
Un **DAG Airflow** coordonne l’exécution **séquentielle (ou parallèle)** des scripts unpack_to_raw.py, preprocess_to_staging.py et process_to_curated.py.
Ainsi, les tâches sont automatisées et déclenchées régulièrement (ou à la demande) pour alimenter le Data Lake de manière continue et reproductible.

# 3. Choix Techniques
---
### Langage & Framework
- **Python 3** pour l’ensemble du pipeline.
- **FastAPI** pour l’API Gateway (documentation Swagger auto-générée).

### Stockage
- **S3** (via LocalStack en environnement local) pour la couche Raw.
- **MySQL** pour la couche Staging (structuration relationnelle).
- **MongoDB** pour la couche Curated (souplesse pour les données enrichies).

### Pipeline de scripts
- ``unpack_to_raw.py `` : Télécharge et combine les CSV depuis kagglehub, puis les stocke dans le bucket (s3) Raw.
- ``preprocess_to_staging.py`` : Nettoie et transforme les données, puis les insère dans MySQL sous format table et dans le bucket (s3) Staging sous format csv.
- ``process_to_curated.py`` : Tokenise et enrichit les données pour insertion dans MongoDB.

### API
- Uvicorn (serveur ASGI) pour exécuter FastAPI.


# 4. Procédures d'installation et de Build
---
### 4.1. Prérequis
- Python 3.9+
- Docker et Docker Compose (docker-compose.yml pour LocalStack, MySQL, MongoDB, etc.)
- pip pour installer les dépendances Python


### 4.2. Installer les dépendances
``pip install -r requirements.txt``

(On doit s'assurer que requirements.txt contienne fastapi, uvicorn, boto3, mysql-connector-python, pymongo, pandas, emoji, etc.)


### 4.3. Lancer les services Docker (LocalStack, MySQL, MongoDB)
Lancer le fichier **docker-compose.yml** : ``docker-compose up -d``

Vérifiez que LocalStack écoute sur http://localhost:4566, MySQL sur port 3306, MongoDB sur port 27017, etc.


### 4.4. Créer les buckets et initialiser les bases
#### 1. Créer les bucket "raw", "staging" et "curated" sur LocalStack 
``aws --endpoint-url=http://localhost:4566 s3 mb s3://raw``
``aws --endpoint-url=http://localhost:4566 s3 mb s3://staging``
``aws --endpoint-url=http://localhost:4566 s3 mb s3://curated``

#### 2. Créer la base MySQL (inclut dans le script 'preprocess_to_staging')

#### 3. Créer la base MongoDB (MongoDB se crée à l’insertion dans 'process_to_curated').


### 4.5. Exécuter les scripts d’ingestion
#### 1. unpack_to_raw.py 
``python build/unpack_to_raw.py --output-dir local_dataset/raw --upload-s3``

#### 2. preprocess_to_staging.py
``python3 src/preprocess_to_staging.py --endpoint-url http://localhost:4566 --bucket_raw raw --file-name bigtech_combined.csv --bucket_staging staging --s3_key bigtech_staging.csv --db_host localhost --db_user root --db_password root --db_database staging``

#### 3. process_to_curated.py
``python3 src/process_to_curated.py --bucket_staging staging --bucket_curated curated --input_file bigtech_staging.csv --output_file bigtech_curated.parquet --model_name bert-base-uncased --mongo_uri mongodb://localhost:27017/ --mongo_db bigtech_db --mongo_collection tweets`` 

(Insère les données finalisées dans MongoDB.)


### 4.6. Lancer l’API Gateway
``uvicorn api:app --reload --host 0.0.0.0 --port 8000``

Ensuite, ouvrez http://localhost:8000/docs pour accéder à l’interface Swagger.


# 5. Utilisation
---
- **Endpoint /raw** :
Retourne les tweets bruts depuis le bucket S3 (LocalStack). Paramètre limit possible.

- **Endpoint /staging** :
Retourne les tweets en base MySQL (possibilité de filtrer par date, sentiment, etc.).

- **Endpoint /curated** :
Retourne les tweets finalisés stockés dans MongoDB.

- **Endpoint /health** :
Vérifie la connectivité à S3, MySQL et MongoDB.

- **Endpoint /stats** :
Fournit des métriques sur le nombre de lignes/objets dans chaque couche (Raw, Staging, Curated).

### Exemple de requête via navigateur
``http://localhost:8000/raw?limit=10``
ou test via Swagger UI :
``http://localhost:8000/docs``


# 6. Conclusion
Cette solution met en place un pipeline complet pour l’ingestion de tweets BigTech, leur prétraitement et leur exposition via une API unifiée. Elle repose sur :

- LocalStack (S3) pour la couche Raw,
- MySQL pour la couche Staging,
- MongoDB pour la couche Curated,
- FastAPI pour l’API Gateway.

### Points Forts
- Architecture modulaire, facile à étendre.
- Scripts Python autonomes pour chaque étape.
- Documentation Swagger auto-générée.

### Prochaines étapes
- Optimiser la gestion des volumes de données (batching, partitions).
- Mettre en place un orchestrateur (Airflow, DVC, etc.) pour automatiser le pipeline.
