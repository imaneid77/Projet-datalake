import argparse
import os
import shutil
import kagglehub
from datasets import load_dataset
import boto3
from pathlib import Path

def download_kaggle_csv(output_dir: str):
    """
    Télécharge le dataset 'wjia26/big-tech-companies-tweet-sentiment' via kagglehub,
    puis copie le fichier 'Bigtech - 12-07-2020 till 19-09-2020.csv' dans output_dir.
    """
    print("=== Téléchargement du dataset depuis KaggleHub ===")
    local_kaggle_path = kagglehub.dataset_download("wjia26/big-tech-companies-tweet-sentiment")
    print(f"Dataset téléchargé dans : {local_kaggle_path}")

    # Assurez-vous que le dossier de sortie existe
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Nom du fichier que vous voulez cibler
    target_filename = "Bigtech - 12-07-2020 till 19-09-2020.csv"

    # Rechercher le CSV dans local_kaggle_path
    print(f"=== Recherche de {target_filename} ===")
    for root, dirs, files in os.walk(local_kaggle_path):
        for f in files:
            if f == target_filename:
                source_path = os.path.join(root, f)
                dest_path = os.path.join(output_dir, f)
                # Copier le fichier dans output_dir
                shutil.copyfile(source_path, dest_path)
                print(f"Fichier copié dans : {dest_path}\n")
                return  # On arrête dès qu'on trouve le fichier

    # Si on sort de la boucle sans trouver
    raise FileNotFoundError(
        f"Le fichier '{target_filename}' n'a pas été trouvé dans {local_kaggle_path}"
    )


def upload(input_dir: str, endpoint_url: str):
    """
    Téléverse le fichier 'Bigtech - 12-07-2020 till 19-09-2020.csv' présent dans input_dir vers le bucket 'raw'.
    
    Remarque : Cette fonction prend le premier fichier CSV lu dans le dossier.
    """
    print("=== Téléversement du fichier vers S3 (bucket 'raw') ===")
    s3_client = boto3.client('s3', endpoint_url=endpoint_url)

    # Nom exact du fichier que l'on souhaite uploader
    csv_filename = "Bigtech - 12-07-2020 till 19-09-2020.csv"
    file_path = os.path.join(input_dir, csv_filename)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Fichier introuvable : {file_path}")

    try:
        s3_client.upload_file(
            Filename=file_path,
            Bucket="raw",
            Key=csv_filename
        )
        print(f"Fichier téléversé avec succès dans s3://raw/{csv_filename}\n")
    except Exception as e:
        print(f"Erreur lors du téléversement : {e}")


def main():
    parser = argparse.ArgumentParser(description='Télécharge et traite les données Bigtech - 12-07-2020 till 19-09-2020.csv')
    parser.add_argument('--output-dir', type=str, default='data/raw',
                        help='Répertoire de sortie pour les données')
    parser.add_argument('--endpoint-url', type=str, default='http://localhost:4566',
                        help='URL du endpoint S3 (LocalStack)')
    
    args = parser.parse_args()
    
    print("=== Téléchargement et copie du CSV localement ===")
    download_kaggle_csv(args.output_dir)

    print("=== Téléversement des données dans le bucket 'raw' ===")
    upload(args.output_dir, args.endpoint_url)

    print("=== Traitement terminé ===")

if __name__ == "__main__":
    main()
