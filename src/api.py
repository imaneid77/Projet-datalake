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



@app.get("/staging/", response_model=List[dict])
# A defnir =================================





@app.get("/curated/", response_model=List[dict])
# A defnir =================================






@app.get("/health")
async def health_check():
    """
    Vérifie la santé de l'API et des connexions aux bases de données
    """
    status = {
        "api_status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "connections": {}
    }
    
    try:
        # Test S3
        db.s3_client.list_buckets()
        status["connections"]["s3"] = True
    except:
        status["connections"]["s3"] = False
    
    try:
        # Test MySQL
        conn = mysql.connector.connect(**db.mysql_config)
        conn.close()
        status["connections"]["mysql"] = True
    except:
        status["connections"]["mysql"] = False
    
    try:
        # Test MongoDB
        db.mongo_client.server_info()
        status["connections"]["mongodb"] = True
    except:
        status["connections"]["mysql"] = False
    
    return status

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)