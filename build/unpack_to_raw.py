import argparse
import os
from datasets import load_dataset
import boto3
from pathlib import Path


def upload(input_dir,endpoint_url):
    """téléverse le dataset dans le bucket raw."""
    # Initialiser le client S3
    s3_client = boto3.client('s3', endpoint_url=endpoint_url)
    for filename in os.listdir(input_dir):
        file_path = os.path.join(input_dir, filename)
        with open(file_path,'r',encoding='utf-8')as f:
            content=f.readlines()

    # Sauvegarder le fichier temporairement
    file = os.path.join(input_dir, 'Bigtech - 12-07-2020 till 19-09-2020.csv')
    with open(file, 'w', encoding='utf-8') as f:
        f.writelines(content)
    
    # Téléverser vers S3
    try:
        s3_client.upload_file(
            file,
            'raw',
            'Bigtech - 12-07-2020 till 19-09-2020.csv'
        )
        print(f"Fichier téléversé avec succès dans s3://raw/Bigtech - 12-07-2020 till 19-09-2020.csv")
    except Exception as e:
        print(f"Erreur lors du téléversement : {e}")
    
    # Nettoyer le fichier temporaire
    os.remove(file)

def main():
    parser = argparse.ArgumentParser(description='Télécharge et traite les données Bigtech - 12-07-2020 till 19-09-2020.csv')
    parser.add_argument('--output-dir', type=str, default='data/raw',
                        help='Répertoire de sortie pour les données')
    parser.add_argument('--endpoint-url', type=str, default='http://localhost:4566',
                        help='URL du endpoint S3 (LocalStack)')
    
    args = parser.parse_args()
    
    print("Téléversement des données dans raw...")
    upload(args.output_dir, args.endpoint_url)
    print("Traitement terminé.")

if __name__ == "__main__":
    main()
