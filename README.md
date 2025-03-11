# PROJET DATA LAKE - DOCUMENTATION TECHNIQUE 
---

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
**/raw** : retourne les données brutes (CSV sur S3).
**/staging** : retourne les données staging (MySQL).
**/curated** : retourne les données finales (MongoDB).
**/health** : vérifie la santé du service.
**/stats** : fournit quelques métriques (par exemple, le nombre de lignes par couche).

# 3. Choix Techniques
---