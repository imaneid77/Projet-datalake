import os
import boto3
import argparse
import pandas as pd
from pathlib import Path
import kagglehub       

def download_and_combine_kaggle_dataset(output_dir: str) -> str:
    """
    1) Télécharge le dataset 'wjia26/big-tech-companies-tweet-sentiment' depuis Kaggle
    2) Recherche tous les .csv (même s’ils sont dans des sous-dossiers)
    3) Combine le tout en un seul DataFrame
    4) Sauvegarde le CSV final dans `output_dir`
    
    Retourne : chemin absolu du fichier CSV combiné
    """
    print("=== Téléchargement du dataset depuis Kaggle ===")
    local_kaggle_path = kagglehub.dataset_download("wjia26/big-tech-companies-tweet-sentiment")
    print(f"Dataset téléchargé dans : {local_kaggle_path}\n")

    # Lister tous les .csv
    print("=== Recherche des fichiers CSV ===")
    csv_files = []
    for root, dirs, files in os.walk(local_kaggle_path):
        for file in files:
            if file.endswith(".csv"):
                csv_path = os.path.join(root, file)
                csv_files.append(csv_path)
                print(f"  - Trouvé : {csv_path}")

    if not csv_files:
        raise FileNotFoundError("Aucun fichier CSV n'a été trouvé dans le dataset Kaggle.")

    # Charger et concaténer les CSV
    print("\n=== Chargement et concaténation des CSV ===")
    dfs = []
    for csv_path in csv_files:
        try:
            df = pd.read_csv(csv_path)
            dfs.append(df)
            print(f"  - OK : {csv_path} ({df.shape[0]} lignes)")
        except Exception as e:
            print(f"  - ERREUR lors de la lecture : {csv_path} => {e}")

    combined_df = pd.concat(dfs, ignore_index=True)
    print(f"\nNombre total de lignes combinées : {combined_df.shape[0]}")

    # Sauvegarder le DataFrame combiné
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    combined_csv_path = os.path.join(output_dir, "bigtech_combined.csv")
    combined_df.to_csv(combined_csv_path, index=False, encoding="utf-8")
    print(f"Fichier combiné sauvegardé dans : {combined_csv_path}\n")

    return combined_csv_path


def upload_to_s3(file_path: str, endpoint_url: str, bucket: str = "raw", key: str = None) -> None:
    """
    Téléverse un fichier local vers un bucket S3 (ou LocalStack).
    Par défaut : bucket = "raw", key = nom du fichier si non spécifié.
    """
    if key is None:
        key = os.path.basename(file_path)

    print(f"=== Téléversement du fichier vers S3 (bucket={bucket}) ===")
    print(f"  endpoint_url : {endpoint_url}")
    print(f"  file_path    : {file_path}")
    print(f"  key          : {key}")

    s3_client = boto3.client("s3", endpoint_url=endpoint_url)

    try:
        # S’assurer que le bucket existe (à créer manuellement si besoin)
        s3_client.upload_file(
            Filename=file_path, 
            Bucket=bucket, 
            Key=key)
        print(f"Fichier téléversé avec succès : s3://{bucket}/{key}\n")
    except Exception as e:
        print(f"Erreur lors du téléversement : {e}")


def main():
    parser = argparse.ArgumentParser(description="Script d’insertion des données Big Tech Twitter dans la couche RAW")
    parser.add_argument("--output-dir", type=str, default="local_dataset/raw",
                        help="Répertoire local où stocker le CSV combiné")      # ou output-dir = data/raw
    parser.add_argument("--endpoint-url", type=str, default="http://localhost:4566",
                        help="URL du endpoint S3 (LocalStack ou AWS)")
    parser.add_argument("--upload-s3", action="store_true",
                        help="Si présent, on envoie aussi le fichier dans un bucket S3/LocalStack")
    parser.add_argument("--bucket", type=str, default="raw",
                        help="Nom du bucket S3 (par défaut 'raw')")
    parser.add_argument("--key", type=str, default=None,
                        help="Chemin/nom de l’objet dans S3 (par défaut = nom du fichier)")

    args = parser.parse_args()

    # 1) Télécharger et combiner les CSV
    combined_csv_path = download_and_combine_kaggle_dataset(args.output_dir)

    # 2) (Optionnel) Uploader sur S3/LocalStack
    if args.upload_s3:
        upload_to_s3(
            file_path=combined_csv_path,
            endpoint_url=args.endpoint_url,
            bucket=args.bucket,
            key=args.key
        )

if __name__ == "__main__":
    main()