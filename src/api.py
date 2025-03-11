import os
import json
import boto3
import pandas as pd
import mysql.connector
from datetime import datetime
from pymongo import MongoClient
from typing import List, Optional
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, FileResponse



app = FastAPI(
    title="Tweets Data Lake API",
    description="Endpoints pour accéder aux données RAW, Staging et Curated du Data Lake ainsi que des informations sur les métriques de remplissage des buckets(stats)",
    version="1.0"
)

# ************ CONFIGURATION DES CONNEXIONS ************
class DatabaseConnections:
    def __init__(self):
        # S3 (LocalStack)
        self.s3_client = boto3.client(
            's3',
            endpoint_url='http://localstack:4566'
        )
            
        # MySQL
        self.mysql_config = {
            'host': 'mysql',            # nom_container = mysql et host = localhost !!
            'user': 'root',
            'password': 'root',
            'database': 'staging'
        }
        
        # MongoDB
        self.mongo_uri = 'mongodb://mongodb:27017/'
        self.mongo_client = MongoClient(self.mongo_uri)
        self.mongo_db = self.mongo_client['bigtech_db']                # nom_de_votre_db !!

db = DatabaseConnections()


# ************ ENDPOINT RAW ************
@app.get("/raw/", response_model=List[dict], summary="Accès aux données brutes (RAW)")
# A defnir =================================
async def get_raw_tweets(limit: Optional[int] = Query(10, description="Nombre maximum de tweets à retourner")):
    """
    Récupère les tweets bruts depuis le bucket S3 'raw'. 
    Le fichier utilisé est 'bigtech_combined.csv'.
    """
    try:
        response = db.s3_client.get_object(Bucket='raw', Key='bigtech_combined.csv')
        content = response['Body'].read().decode('utf-8')
        # Utilisation de pandas pour lire le CSV et renvoyer quelques lignes sous forme de JSON
        df = pd.read_csv(StringIO(content))
        tweets = df.head(limit).to_dict(orient='records')
        return tweets
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur lors de la lecture depuis S3: {str(e)}")



@app.get("/staging", response_model=List[dict], summary="Accès aux données intermédiaires (Staging)")
async def get_staging_tweets(
    start_date: Optional[str] = Query(None, description="Date de début (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Date de fin (YYYY-MM-DD)"),
    sentiment: Optional[str] = Query(None, description="Filtrer par sentiment (positive, neutral, negative)")
):
    """
    Récupère les tweets staging depuis la table MySQL 'tweets_staging'.
    Vous pouvez filtrer par date ou par sentiment.
    """
    try:
        conn = mysql.connector.connect(**db.mysql_config)
        cursor = conn.cursor(dictionary=True)
        query = "SELECT * FROM tweets_staging WHERE 1=1"
        params = []
        if start_date:
            query += " AND DATE(created_at) >= %s"
            params.append(start_date)
        if end_date:
            query += " AND DATE(created_at) <= %s"
            params.append(end_date)
        if sentiment:
            query += " AND sentiment = %s"
            params.append(sentiment)
        cursor.execute(query, params)
        tweets = cursor.fetchall()
        cursor.close()
        conn.close()
        for tweet in tweets:
            if tweet.get("created_at"):
                tweet["created_at"] = tweet["created_at"].isoformat()
        return tweets
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur lors de la lecture depuis MySQL: {str(e)}")





@app.get("/curated", response_model=List[dict], summary="Accès aux données finales (Curated)")
async def get_curated_tweets(limit: Optional[int] = Query(10, description="Nombre maximum de tweets à retourner")):
    """
    Récupère les tweets finalisés depuis MongoDB.
    """
    try:
        collection = db.mongo_db.tweets_curated
        tweets = list(collection.find({}, {"_id": 0}).limit(limit))
        for tweet in tweets:
            if tweet.get("created_at") and isinstance(tweet["created_at"], datetime):
                tweet["created_at"] = tweet["created_at"].isoformat()
        return tweets
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur lors de la lecture depuis MongoDB: {str(e)}")



@app.get("/health", summary="Vérification de l'état des services")
async def health_check():
    """
    Vérifie la santé de l'API et des connexions aux services (S3, MySQL, MongoDB).
    """
    status = {
        "api_status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "services": {}
    }
    try:
        db.s3_client.list_buckets()
        status["services"]["s3"] = True
    except Exception as e:
        status["services"]["s3"] = False
    try:
        conn = mysql.connector.connect(**db.mysql_config)
        conn.close()
        status["services"]["mysql"] = True
    except Exception as e:
        status["services"]["mysql"] = False
    try:
        db.mongo_client.server_info()
        status["services"]["mongodb"] = True
    except Exception as e:
        status["services"]["mongodb"] = False
    return status

@app.get("/stats", summary="Métriques sur les données et services")
async def get_stats():
    """
    Fournit quelques métriques sur les données stockées dans les différents niveaux du Data Lake.
    """
    try:
        metrics = {}
        # Statistiques pour le bucket RAW
        response = db.s3_client.get_object(Bucket='raw', Key='bigtech_combined.csv')
        content = response['Body'].read().decode('utf-8')
        df_raw = pd.read_csv(StringIO(content))
        metrics["raw_row_count"] = df_raw.shape[0]
        
        # Statistiques pour le staging (MySQL)
        conn = mysql.connector.connect(**db.mysql_config)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM tweets_staging")
        metrics["staging_row_count"] = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        
        # Statistiques pour les données curated (MongoDB)
        metrics["curated_row_count"] = db.mongo_db.tweets_curated.count_documents({})
        
        return metrics
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur lors de la récupération des statistiques: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)