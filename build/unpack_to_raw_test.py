import os
import io
import boto3
import pandas as pd
from pathlib import Path
import kagglehub
from sqlalchemy import create_engine


def download_kaggle_dataset(kaggle_dataset: str) -> str:
    """
    Télécharge un dataset Kaggle et retourne le chemin local où il est stocké.
    Utilise la librairie kagglehub pour simplifier le download.
    """
    print("=== Téléchargement du dataset depuis Kaggle ===")
    local_kaggle_path = kagglehub.dataset_download(kaggle_dataset)
    print(f"Dataset téléchargé dans : {local_kaggle_path}\n")
    return local_kaggle_path


import os
import io
import boto3
import pandas as pd
from pathlib import Path
import kagglehub
from sqlalchemy import create_engine

def unpack_pipeline(
    kaggle_dataset="wjia26/big-tech-companies-tweet-sentiment",
    mysql_conn_str="mysql+mysqlconnector://root:root@mysql:3306/staging",
    table_name="tweets_staging",
    endpoint_url=None,
    s3_bucket=None,
    s3_key_prefix="bigtech_chunk",
    chunk_size=20_000
):
    """
    1) Télécharge le dataset Kaggle
    2) Parcourt les CSV par chunks
    3) Alimente la base MySQL (table 'table_name')
    4) (Optionnel) envoie chaque chunk sur S3
    """
    print(f"=== Téléchargement du dataset '{kaggle_dataset}' depuis Kaggle ===")
    local_kaggle_path = kagglehub.dataset_download(kaggle_dataset)
    print(f"Dataset téléchargé dans : {local_kaggle_path}\n")

    # Lister tous les CSV
    print("=== Recherche des fichiers CSV ===")
    csv_files = []
    for root, dirs, files in os.walk(local_kaggle_path):
        for file in files:
            if file.endswith(".csv"):
                csv_path = os.path.join(root, file)
                csv_files.append(csv_path)
                print(f"  - Trouvé : {csv_path}")

    if not csv_files:
        raise FileNotFoundError("Aucun fichier CSV trouvé dans le dataset Kaggle.")

    print(f"\n=== Connexion MySQL : {mysql_conn_str} ===")
    engine = create_engine(mysql_conn_str)

    s3_client = None
    if endpoint_url and s3_bucket:
        s3_client = boto3.client("s3", endpoint_url=endpoint_url,aws_access_key_id="test",aws_secret_access_key="test",region_name="us-east-1")
        print(f"=== Upload S3 Activé : bucket={s3_bucket}, endpoint={endpoint_url} ===")
    else:
        print("=== Upload S3 Désactivé (pas de endpoint_url ou s3_bucket) ===")

    total_rows = 0
    chunk_index = 0

    for csv_path in csv_files:
        print(f"\n--- Traitement du fichier : {csv_path} ---")
        # Lire le CSV par chunks
        for chunk_df in pd.read_csv(csv_path, chunksize=chunk_size):
            nb_rows = len(chunk_df)
            total_rows += nb_rows
            print(f"  -> Chunk #{chunk_index} : {nb_rows} lignes")

            # Insertion MySQL
            chunk_df.to_sql(
                name=table_name,
                con=engine,
                if_exists='append',  # Ajoute les lignes
                index=False
            )

            # Optionnel : envoi sur S3
            if s3_client:
                csv_buffer = io.StringIO()
                chunk_df.to_csv(csv_buffer, index=False)
                csv_buffer.seek(0)

                object_key = f"{s3_key_prefix}_{chunk_index}.csv"
                s3_client.put_object(
                    Bucket=s3_bucket,
                    Key=object_key,
                    Body=csv_buffer.getvalue()
                )
                print(f"  -> Chunk #{chunk_index} envoyé sur s3://{s3_bucket}/{object_key}")

            chunk_index += 1

    print(f"\n=== Fin de l'ingestion ===")
    print(f"Total de lignes insérées : {total_rows}")
    print(f"{chunk_index} chunks ont été traités.")



def main():
    """
    Exemple d'utilisation complet :
      - Télécharge Kaggle dataset
      - Ingest dans MySQL
      - Upload S3 si configuré
    """
    # Paramètres à adapter :
    kaggle_dataset = "wjia26/big-tech-companies-tweet-sentiment"
    local_kaggle_path = download_kaggle_dataset(kaggle_dataset)

    # Chaine de connexion MySQL via SQLAlchemy (Docker : 'mysql' = nom conteneur)
    mysql_conn_str = "mysql+mysqlconnector://root:root@mysql:3306/staging"
    table_name = "tweets_staging"

    # Paramètres S3 (LocalStack ou AWS).
    # Si tu laisses endpoint_url=None ou s3_bucket=None, ça ne fera pas d'upload S3.
    endpoint_url = "http://localstack:4566"  
    s3_bucket = "raw"
    s3_key_prefix = "bigtech_chunk"

    # Lancement
    unpack_pipeline(
        local_kaggle_path=local_kaggle_path,
        mysql_conn_str=mysql_conn_str,
        table_name=table_name,
        endpoint_url=endpoint_url,
        s3_bucket=s3_bucket,
        s3_key_prefix=s3_key_prefix,
        chunk_size=20_000
    )


if __name__ == "__main__":
    main()
