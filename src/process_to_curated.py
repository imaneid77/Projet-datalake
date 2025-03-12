import os
import io
import numpy as np
import pandas as pd
import boto3
import mysql.connector

from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
from sklearn.preprocessing import MinMaxScaler

# ====== TOKENISATION MULTI-CPU ======
from joblib import Parallel, delayed
import torch
from transformers import AutoTokenizer


def get_data_from_mysql(mysql_host, mysql_user, mysql_password, mysql_database):
    """
    Récupère toutes les données depuis MySQL (table 'tweets_staging').
    """
    try:
        connection = mysql.connector.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
            database=mysql_database
        )
        query = "SELECT * FROM tweets_staging;"
        df = pd.read_sql(query, connection)  # On a un avertissement dans le terminal : AVERTISSEMENT: "Non-SQLAlchemy connectable" - pas grave.
        connection.close()
        print(f"{df.shape[0]} lignes récupérées depuis MySQL.")
        return df
    except mysql.connector.Error as err:
        print(f"Erreur MySQL : {err}")
        return None


def parallel_tokenize_batch(tokenizer, texts, max_length=256):
    """
    Tokenise un lot de tweets"
    """
    tokens = tokenizer(
        texts,
        padding="max_length",
        truncation=True,
        max_length=max_length,
        return_tensors="np"
    )["input_ids"].astype(np.int32)
    return tokens


def tokenize_tweets(data, model_name="bert-base-uncased", batch_size=2000, n_jobs=2):
    """
    Tokenise la colonne 'text' de 'data' par batchs, en parallèle. 
    Retourne un DataFrame 'tokenized_df'.
    """
    print(f"Chargement du tokenizer : {model_name}...")
    tokenizer = AutoTokenizer.from_pretrained(model_name)

    all_texts = data["text"].tolist()
    total = len(all_texts)
    print(f"Tokenisation en parallèle sur {total} tweets, batch_size={batch_size}, n_jobs={n_jobs}")

    # Partitionne en sous-listes
    chunks = [all_texts[i:i+batch_size] for i in range(0, total, batch_size)]

    # Tokenisation en parallèle
    results = Parallel(n_jobs=n_jobs, backend="threading")(
        delayed(parallel_tokenize_batch)(tokenizer, chunk) for chunk in chunks
    )

    # results est une liste de tableaux NumPy. Il faut les concaténer
    token_arrays = np.concatenate(results, axis=0)

    # Conversion en DataFrame
    tokenized_df = pd.DataFrame(token_arrays)
    tokenized_df.columns = [f"token_{i}" for i in range(tokenized_df.shape[1])]

    print(f"Tokenisation terminée : {tokenized_df.shape[0]} tweets tokenisés.")
    return tokenized_df


def insert_data_in_batches_mongodb(collection, df, batch_size=2000):
    """
    Insère ou met à jour les données dans MongoDB par batchs (bulk_write + upsert).
    """
    total_rows = len(df)
    for start in range(0, total_rows, batch_size):
        end = min(start + batch_size, total_rows)
        batch = df.iloc[start:end]

        # On crée les opérations d'upsert
        operations = []
        for _, row in batch.iterrows():
            doc = row.dropna().to_dict()  # Convertit la ligne en dict
            operations.append(
                UpdateOne(
                    {"twitter_id": doc.get("twitter_id")},  # Filtre sur 'twitter_id'
                    {"$set": doc},
                    upsert=True
                )
            )

        if not operations:
            continue

        try:
            # Pour ignorer les erreurs de duplicat : bulk_write(operations, ordered=False)
            result = collection.bulk_write(operations, ordered=True)
            print(f"Batch {start} -> {end} inséré/mis à jour : "
                  f"{result.upserted_count} upserts, {result.modified_count} modifiés.")
        except BulkWriteError as bwe:
            print(f"Erreur batch {start}-{end} : {bwe}")


def upload_to_mongodb(mongo_uri, database_name, collection_name, data, batch_size=10000):
    """
    Insertions par batch dans MongoDB, 
    avec création d'index.
    """
    print("Connexion MongoDB...")
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]

    # Création des index
    print("Ajout des index pour optimiser MongoDB...")
    collection.create_index("twitter_id", unique=True)
    collection.create_index("created_at")
    collection.create_index("username")
    print("Index ajoutés avec succès.")

    print("Envoi des données par batchs vers MongoDB...")
    insert_data_in_batches_mongodb(collection, data, batch_size=batch_size)


def process_to_curated(mysql_host, mysql_user, mysql_password, mysql_database,
                       bucket_curated, output_file, model_name="bert-base-uncased",
                       mongo_uri=None, mongo_db=None, mongo_collection=None):
    """
    Pipeline:
    1. Récupération des données de MySQL
    2. Nettoyage + ajout de nouvelles colonnes
    3. Tokenisation des tweets
    4. Fusion dans un DataFrame final
    5. Sauvegarde local + upload S3
    6. Envoi MongoDB
    """
    # 1) Récupération MySQL
    data = get_data_from_mysql(mysql_host, mysql_user, mysql_password, mysql_database)
    if data is None or data.empty:
        print("Aucune donnée récupérée depuis MySQL.")
        return

    # 2) Nettoyage
    print("Nettoyage et ajout nouvelles colonnes...")
    data = data[data["followers"] > 0]
    data = data[data["friends"] > 0]
    data = data[data["polarity"].between(-1, 1)]

    scaler = MinMaxScaler()
    data[["followers", "friends", "retweet_count"]] = scaler.fit_transform(
        data[["followers", "friends", "retweet_count"]]
    )

    data["day"] = data["created_at"].dt.dayofweek
    data["hour"] = data["created_at"].dt.hour
    data["is_weekend"] = data["day"].apply(lambda x: 1 if x >= 5 else 0)
    data["score"] = data["retweet_count"] / (data["followers"] + 1)

    # 3) Tokenisation
    tokenized_df = tokenize_tweets(data, model_name=model_name, batch_size=10000, n_jobs=2)
    processed_data = pd.concat([data.reset_index(drop=True), tokenized_df.reset_index(drop=True)], axis=1)
    print(f"Shape final : {processed_data.shape}")

    # 5) Sauvegarde local + upload S3
    local_output_path = f"/tmp/{output_file}"
    processed_data.to_parquet(local_output_path, index=False)
    print(f"Données sauvegardées localement : {local_output_path}")

    s3 = boto3.client('s3', endpoint_url='http://localhost:4566')
    print(f"📤 Upload vers S3 bucket={bucket_curated}, key={output_file} ...")
    with open(local_output_path, "rb") as f:
        s3.upload_fileobj(f, bucket_curated, output_file)
    print(f"Données envoyées dans s3://{bucket_curated}/{output_file}")

    # 6)Insertion MongoDB
    print(f"Envoi des données vers MongoDB '{mongo_uri}' collection {mongo_db}.{mongo_collection}...")
    upload_to_mongodb(mongo_uri, mongo_db, mongo_collection, processed_data, batch_size=10000)
    print("Insertion MongoDB terminée.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pipeline Curated : MySQL -> Nettoyage -> Tokenisation -> S3 -> MongoDB")
    parser.add_argument("--mysql_host", type=str, default="localhost", help="Hôte MySQL")
    parser.add_argument("--mysql_user", type=str, default="root", help="Utilisateur MySQL")
    parser.add_argument("--mysql_password", type=str, default="root", help="Mot de passe MySQL")
    parser.add_argument("--mysql_database", type=str, default="staging", help="Base de données MySQL")
    parser.add_argument("--bucket_curated", type=str, default="curated", help="Nom du bucket S3 Curated")
    parser.add_argument("--output_file", type=str, default="bigtech_curated.parquet", help="Nom du fichier de sortie")
    parser.add_argument("--model_name", type=str, default="bert-base-uncased", help="Modèle de tokenizer HF")
    parser.add_argument("--mongo_uri", type=str, default="mongodb://localhost:27017/", help="URI de connexion MongoDB (laisser vide pour skip)")
    parser.add_argument("--mongo_db", type=str, default="bigtech_db", help="Base MongoDB")
    parser.add_argument("--mongo_collection", type=str, default="tweets", help="Collection MongoDB")

    args = parser.parse_args()

    process_to_curated(
        mysql_host=args.mysql_host,
        mysql_user=args.mysql_user,
        mysql_password=args.mysql_password,
        mysql_database=args.mysql_database,
        bucket_curated=args.bucket_curated,
        output_file=args.output_file,
        model_name=args.model_name,
        mongo_uri=args.mongo_uri,
        mongo_db=args.mongo_db,
        mongo_collection=args.mongo_collection
    )
