import os
import boto3
import argparse
import pandas as pd
from pathlib import Path

def download_tweet_data(output_dir):
    """Télécharge les données Tweet sentiment et les organise dans les sous dossiers train et test."""
    
    # Créer les répertoires nécessaires
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    for split in ['train', 'test']:
        Path(os.path.join(output_dir, split)).mkdir(exist_ok=True)
    
    # Télécharger le dataset
    path_data : r"local_dataset\Bigtech - 12-07-2020 till 19-09-2020.csv"      # relative_path = local_dataset\Bigtech - 12-07-2020 till 19-09-2020.csv
    # path_data : r"local_dataset"
    dataset = pd.read_csv(path_data)
    
    # Sauvegarder chaque split dans son dossier respectif
    for split in dataset.keys():
        output_file = os.path.join(output_dir, split, f'tweet-sentiment-{split}.csv')       # f'tweet-sentiment-{split}.txt'
        
        with open(output_file, 'w', encoding='utf-8') as f:
            for item in dataset[split]['text']:             # Spliter selonnnn les lignes !!!
                if item.strip():  # Éviter les lignes vides
                    f.write(item + '\n')



def combine_and_upload(input_dir, endpoint_url):
    """Combine les fichiers et les téléverse dans le bucket raw."""
    # Initialiser le client S3
    s3_client = boto3.client('s3', endpoint_url=endpoint_url)
    
    # Combiner tous les fichiers
    combined_content = []
    for split in ['train', 'test']:
        split_dir = os.path.join(input_dir, split)
        for filename in os.listdir(split_dir):
            file_path = os.path.join(split_dir, filename)
            with open(file_path, 'r', encoding='utf-8') as f:
                combined_content.extend(f.readlines())
    
    # Sauvegarder le fichier combiné temporairement
    combined_file = os.path.join(input_dir, 'tweet-sentiment-tmp.csv')
    with open(combined_file, 'w', encoding='utf-8') as f:
        f.writelines(combined_content)
    
    # Téléverser vers S3
    try:
        s3_client.upload_file(
            combined_file,
            'raw',
            'tweet-sentiment-tmp.csv'
        )
        print(f"Fichier téléversé avec succès dans s3://raw/tweet-sentiment-tmp.csv")
    except Exception as e:
        print(f"Erreur lors du téléversement : {e}")
    
    # Nettoyer le fichier temporaire
    os.remove(combined_file)

def main():
    parser = argparse.ArgumentParser(description='Télécharge et traite les données tweet-sentiment')
    parser.add_argument('--output-dir', type=str, default='local_dataset/raw',
                        help='Répertoire de sortie pour les données')
    parser.add_argument('--endpoint-url', type=str, default='http://localhost:4566',
                        help='URL du endpoint S3 (LocalStack)')
    
    args = parser.parse_args()
    
    print("Téléchargement des données...")
    download_tweet_data(args.output_dir)
    print("Données téléchargées et organisées.")
    
    print("Combinaison et téléversement des fichiers des dossiers train et test...")
    combine_and_upload(args.output_dir, args.endpoint_url)
    print("Traitement terminé.")

if __name__ == "__main__":
    main()