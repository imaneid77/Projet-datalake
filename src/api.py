from fastapi import FastAPI, HTTPException, Query
from typing import List, Optional
import boto3
import json
import mysql.connector
from pymongo import MongoClient
from datetime import datetime

app = FastAPI(title="Data Lake API")

# Configuration des connexions avec les bons paramètres
class DatabaseConnections:
    def __init__(self):
        # S3 (LocalStack)
        self.s3_client = boto3.client(
            's3',
            endpoint_url='http://localstack:4566'
        )
            
        
        # MySQL
        self.mysql_config = {
            'host': 'mysql',
            'user': 'root',
            'password': 'root',
            'database': 'nom_de_votre_bdd'
        }
        
        # MongoDB
        self.mongo_uri = 'mongodb://mongodb:27017/'
        self.mongo_client = MongoClient(self.mongo_uri)
        self.mongo_db = self.mongo_client['nom_de_votre_db']

db = DatabaseConnections()


@app.get("/raw/", response_model=List[dict])
# A defnir =================================




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