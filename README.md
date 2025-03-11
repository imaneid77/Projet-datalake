# PROJET DATA LAKE - DOCUMENTATION TECHNIQUE 


# 1. Introduction
---
Ce projet met en place un Data Lake pour l’ingestion, le nettoyage et l’exposition de données de tweets (BigTech) en trois couches : Raw, Staging et Curated.
Une API Gateway (basée sur FastAPI) permet de consulter les données à chaque étape du pipeline, ainsi que de récupérer des informations de santé et de statistiques.

# 2. Architecture Générale
---
### 1. Couche Raw
Les données brutes (tweets) sont stockées dans un bucket S3 aws simulé par LocalStack, pour l’environnement de développement.

### 2. Couche Staging
Les données nettoyées sont insérées dans une base MySQL pour une première structuration.

### 3. Couche Curated
On applique les dernières transformations aux données finales. Celles-ci sont par la suite enrichies, tokenisées, puis stockées dans MongoDB.

### 4. API Gateway
On a également mis à disposition un service FastAPI qui expose différents endpoints :
**/raw** : retourne les données brutes (CSV sur S3).\\
**/staging** : retourne les données staging (MySQL).\\
**/curated** : retourne les données finales (MongoDB).\\
**/health** : vérifie la santé du service.\\
**/stats** : fournit quelques métriques (par exemple, le nombre de lignes par couche).

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
#### 1. Créer le bucket "raw" sur LocalStack 
