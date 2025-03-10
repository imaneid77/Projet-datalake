import io
import pandas as pd
import boto3
from transformers import AutoTokenizer
from sklearn.preprocessing import MinMaxScaler

def process_to_curated(bucket_staging,bucket_curated,input_file,output_file,model_name="bert-base-uncased"):
    """
    Prépare les données pour la zone curated avec des traitements avancés.

    1. Télécharge les données nettoyées de STAGING.
    2. Effectue des transformations avancées :
       - Suppression des valeurs aberrantes
       - Normalisation des colonnes numériques
       - Ajout de nouvelles colonnes
       - Filtrage des données non pertinentes
       - Tokenisation des tweets
    3. Sauvegarde les données localement
    4. Upload vers le bucket CURATED.

    Arguments :
    - bucket_staging (str) : Nom du bucket STAGING.
    - bucket_curated (str) : Nom du bucket CURATED.
    - input_file (str) : Nom du fichier dans le bucket STAGING.
    - output_prefix (str) : Préfixe du fichier de sortie.
    """

    # Initialize S3 client
    s3 = boto3.client('s3', endpoint_url='http://localhost:4566')

    #Téléchargement des données de STAGING
    print(f"Téléchargement de {input_file} depuis le bucket staging")
    response = s3.get_object(Bucket=bucket_staging, Key=input_file)
    data = pd.read_csv(io.BytesIO(response['Body'].read()))

    #suppression des valeurs aberrantes
    data=data[data["followers"]>0]
    data=data[data["friends"]>0]
    data = data[(data["polarity"] >= -1) & (data["polarity"] <= 1)]

    #Normalisation des colonnes numériques pour faciliter l'entrainement des modèles de ML
    scaler=MinMaxScaler()
    data[["followers", "friends", "retweet_count"]] = scaler.fit_transform(data[["followers", "friends", "retweet_count"]])

    #Ajout de nouvelles colonnes pour l'analyse
    data["day"] = data["created_at"].dt.dayofweek
    data["hour"] = data["created_at"].dt.hour
    data["is_weekend"] = data["day_of_week"].apply(lambda x: 1 if x >= 5 else 0)

    #l'engagement score=interactions/audience, ça permet d'évaluer l'impact d'un tweet en fonction du nombre d'interactions qu'il génère
    #ici on met +1 pour éviter une division par 0 si la personne n'a aucun followers
    data["engagement_score"] = data["retweet_count"] / (data["followers"] + 1)

    #Tokenisation des tweets
    #1.chargement du tokenizer
    print(f"Chargement du tokenizer : {model_name}...")
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    
    #2.tokenisation des tweets
    print("Tokenisation des tweets...")
    tokenized_data = []
    for text in data["text"]:
        tokens = tokenizer(text, truncation=True, padding="max_length", max_length=256, return_tensors="np")
        tokenized_data.append(tokens["input_ids"][0])  # récupération des IDs des tokens
    
    # Convertir en dataframe
    tokenized_data = pd.DataFrame(tokenized_data)
    tokenized_data.columns = [f"token_{i}" for i in range(tokenized_data.shape[1])]

    # Fusion avec les métadonnées
    print("Fusion des tokens avec les métadonnées...")
    processed_data = pd.concat([data, tokenized_data], axis=1)

    #Sauvegarde local
    local_output_path = f"/tmp/{output_file}"
    processed_data.to_parquet(local_output_path, index=False)
    print(f"Données sauvegardées localement à : {local_output_path}.")

    #Upload vers curated
    print(f"Versement de {output_file} vers le bucket curated...")
    with open(local_output_path, "rb") as f:
        s3.upload_fileobj(f, bucket_curated,output_file)

    print(f"Les données traitées ont été téléchargées avec succès dans le bucket organisé sous le nom de {output_file}.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Traiter les données depuis le bucket staging jusqu'au bucket curated")
    parser.add_argument("--bucket_staging", type=str, required=True, help="Nom du bucket staging S3")
    parser.add_argument("--bucket_curated", type=str, required=True, help="Nom du bucket curated S3")
    parser.add_argument("--input_file", type=str, required=True, help="Nom de l'input dans le bucket staging")
    parser.add_argument("--output_file", type=str, required=True, help="Nom de l'output dans le bucket curated")
    parser.add_argument("--model_name", type=str, default="bert-base-uncased", help="Nom du modèle de tokenizer")
    args = parser.parse_args()

    process_to_curated(args.bucket_staging, args.bucket_curated, args.input_file, args.output_file, args.model_name)