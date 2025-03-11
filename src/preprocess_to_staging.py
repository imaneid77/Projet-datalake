import re
import boto3
import argparse
import pandas as pd
import mysql.connector
from io import StringIO
from pathlib import Path
from mysql.connector import Error


# **************** RECUPERER LES DONNEES DEPUIS LE BUCKET RAW ********************
def get_data_from_raw(endpoint_url, bucket_name, file_name="bigtech_combined.csv"):
    """
    Récupère les données depuis le bucket raw.
    params : 
    - file_name = bigtech_combined.csv dans le cas ou l'on exécute unpack_to_raw_v
    sinon file_name = Bigtech - 12-07-2020 till 19-09-2020.csv
    """
    try:
        s3_client = boto3.client('s3', endpoint_url=endpoint_url)
        response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        content = response['Body'].read().decode('utf-8')
        return content
    except Exception as e:
        print(f"Erreur lors de la récupération des données depuis S3: {e}")
        return None
    

#  **************** EXTRACTION DES '#' ****************
def extract_hashtags(text):
    return re.findall(r"#\w+", text)


# **************** DEFINITION DU 'SENTIMENT' ****************
def polarity_to_sentiment(polarity):
        if pd.isnull(polarity):
            return None
        if polarity == 0.0:
            return 'neutral'
        elif polarity >= 0.5:
            return 'positive'
        else:
            return 'negative'


# **************** NETTOYAGE DES DONNEES -1- ******************
def clean_text_func(text):
    """
    Nettoie un texte en supprimant les URLs, les hashtags, les emojis,
    les caractères spéciaux (conserve lettres, chiffres et espaces),
    les espaces multiples
    """
    if not isinstance(text, str):
        return text

    text = re.sub(r'http\S+', '', text)              # supprime les URLs
    text = re.sub(r"#\w+", '', text)                 # supprime les hashtags
    # Supprime les emojis (motif couvrant plusieurs plages Unicode)
    emoji_pattern = re.compile("["
                           u"\U0001F600-\U0001F64F"  # émoticônes
                           u"\U0001F300-\U0001F5FF"  # symboles et pictogrammes
                           u"\U0001F680-\U0001F6FF"  # transport et symboles
                           u"\U0001F1E0-\U0001F1FF"  # drapeaux
                           "]+", flags=re.UNICODE)
    
    text = emoji_pattern.sub(r'', text)
    text = re.sub(r"[^\w\s]", "", text)              # supprime les caractères spéciaux restants (conserver lettres, chiffres et espaces)
    text = re.sub(r"\s+", " ", text)                 # supprime les espaces multiples

    return text.strip()

    

# **************** NETTOYAGE DES DONNEES -2- ******************
def clean_data(content):
    """
    Nettoie les données en appliquant plusieurs transformations pour garantir
    un formatage standardisé.
    """
    
    df = pd.read_csv(StringIO(content))
    print(f"df initial : {df.shape}, colonnes : {df.columns.tolist()}, dtypes :\n{df.dtypes}")
    if 'created_at' in df_preprocessed.columns:
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    
    # Suppression des lignes vides dans la colonne 'text'
    df = df.dropna(subset=['text'])
    print(f"Lignes vides : {df['text'].isna()}")

    # Suppression des colonnes inutiles car redondants
    print(f"==== Supprimer file_name, partition_0, partition_1, search_query ====")
    columns_to_drop = ["file_name", "partition_1", "partition_0", "search_query"]
    df.drop(columns=[col for col in columns_to_drop if col in df.columns],inplace=True)
    
    # Suppression des doublons et réinitialisation de l’index
    df_no_duplicated = df.drop_duplicates().reset_index(drop=True)
    print(f"Doublons : {df_no_duplicated['text'].duplicated()}")
    
    # Suppression des lignes avec valeurs manquantes dans 'location'
    df_no_duplicated = df_no_duplicated.dropna(subset=['location'])
    print(f"Valeurs manquantes dans 'location' : {df_no_duplicated['location'].isna()}")

    # On supprime tous les espaces vides en trop dans toutes les colonnes
    df_no_duplicated = df_no_duplicated.applymap(lambda x:x.strip() if isinstance(x,str)else x)

    # Extraction des hashtags depuis la colonne 'text' 
    df_w_keyword =  df_no_duplicated.copy()
    df_w_keyword['hashtags_list'] = df_w_keyword['text'].apply(lambda x: extract_hashtags(x) if isinstance(x, str) else [])
    max_tags = df_w_keyword['hashtags_list'].apply(len).max()
    print(f"Nombre de hashtags max : {max_tags}")
    print(f"Hashtag list : {df_w_keyword['hashtags_list']}")

    # Création de colonnes distinctes pour chaque hashtag (une colonne par hashtag)
    for i in range(max_tags):
        df_w_keyword[f'keyword_{i+1}'] = df_w_keyword['hashtags_list'].apply(lambda tags: tags[i] if len(tags) > i else None)
    print(f"Colonnes avec les mots clés nrmlt : {df_w_keyword.columns}")
    print(df_w_keyword.head(5))
    # df_w_keyword = df_w_keyword.drop(["hashtags_list"], axis=1)             # on supprime ensuite cette liste ou on garde qd mm ici et on la met pas dans la table sql ?

    # Nettoyage de la colonne 'text' : on retire les hashtags, emojis, caracteres speciaux...
    df_preprocessed = df_w_keyword.copy()
    df_preprocessed['clean_text'] = df_preprocessed['text'].apply(clean_text_func)
    print(f"Colonnes avec 'clean_text' nrmlt : {df_preprocessed.columns}")

    # Conversion des colonnes numériques : 'followers', 'friends', 'retweet_count', 'twitter_id', 'polarity'
    # retweet_count :float, 'twitter_id': long?, polarity:'float'...
    for col in ['followers', 'friends', 'retweet_count', 'polarity']:
        if col in df_preprocessed.columns:
            df_preprocessed[col] = pd.to_numeric(df_preprocessed[col], errors='coerce')

    # Création de la colonne 'sentiment' à partir de la polarité pour une meilleure interprétation
    df_preprocessed['sentiment'] = df_preprocessed['polarity'].apply(polarity_to_sentiment)
    print(f"Sentiment column : {df_preprocessed['sentiment'].value_counts()}")

    # On met 'clean_text" a la suite de 'text'
    cols = list(df_preprocessed.columns)
    if 'text' in cols and 'clean_text' in cols:
        cols.remove('clean_text')
        idx = cols.index('text') + 1
        cols.insert(idx, 'clean_text')
        df_preprocessed = df_preprocessed[cols]
    
    print("=== Exemple de 'clean_text' ===")
    print(df_preprocessed['clean_text'].head(5))

    return df_preprocessed



# *************** SAUVEGARDE DANS S3 STAGING ***************
def upload_to_s3(file_path, bucket, key, endpoint_url):
    """
    Téléverse un fichier local vers un bucket S3.
    """
    s3_client = boto3.client("s3", endpoint_url=endpoint_url)
    try:
        s3_client.upload_file(Filename=file_path, Bucket=bucket, Key=key)
        print(f"Fichier téléversé avec succès : s3://{bucket}/{key}")
    except Exception as e:
        print(f"Erreur lors du téléversement vers S3: {e}")


# **************** CONNECTION A MYSQL ******************
def create_mysql_connection(host, user, password, database):
    """
    Crée une connexion MySQL.
    """
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        return connection
    except Error as e:
        print(f"Erreur lors de la connexion à MySQL: {e}")
        return None


# **************** CREATION DE LA TABLE SQL ******************
def create_table(connection):
    """
    Crée la table tweets_staging dans MySQL. 
    """
    try:
        cursor = connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tweets_staging (
                id INT AUTO_INCREMENT PRIMARY KEY,
                created_at DATETIME,
                followers INT,
                friends INT,
                location VARCHAR(255),
                retweet_count INT,
                screenname VARCHAR(255),
                text TEXT,
                clean_text TEXT,
                twitter_id VARCHAR(255),
                username VARCHAR(255),
                polarity FLOAT,
                partition_0 VARCHAR(255),
                -- Les colonnes keywords (exemple, jusqu'à keyword_42)
                keyword_1 VARCHAR(255),
                keyword_2 VARCHAR(255),
                keyword_3 VARCHAR(255),
                keyword_4 VARCHAR(255),
                keyword_5 VARCHAR(255),
                keyword_6 VARCHAR(255),
                keyword_7 VARCHAR(255),
                keyword_8 VARCHAR(255),
                keyword_9 VARCHAR(255),
                keyword_10 VARCHAR(255),
                keyword_11 VARCHAR(255),
                keyword_12 VARCHAR(255),
                keyword_13 VARCHAR(255),
                keyword_14 VARCHAR(255),
                keyword_15 VARCHAR(255),
                keyword_16 VARCHAR(255),
                keyword_17 VARCHAR(255),
                keyword_18 VARCHAR(255),
                keyword_19 VARCHAR(255),
                keyword_20 VARCHAR(255),
                keyword_21 VARCHAR(255),
                keyword_22 VARCHAR(255),
                keyword_23 VARCHAR(255),
                keyword_24 VARCHAR(255),
                keyword_25 VARCHAR(255),
                keyword_26 VARCHAR(255),
                keyword_27 VARCHAR(255),
                keyword_28 VARCHAR(255),
                keyword_29 VARCHAR(255),
                keyword_30 VARCHAR(255),
                keyword_31 VARCHAR(255),
                keyword_32 VARCHAR(255),
                keyword_33 VARCHAR(255),
                keyword_34 VARCHAR(255),
                keyword_35 VARCHAR(255),
                keyword_36 VARCHAR(255),
                keyword_37 VARCHAR(255),
                keyword_38 VARCHAR(255),
                keyword_39 VARCHAR(255),
                keyword_40 VARCHAR(255),
                keyword_41 VARCHAR(255),
                keyword_42 VARCHAR(255),
                sentiment VARCHAR(20)
            )
        """)
        connection.commit()
    except Error as e:
        print(f"Erreur lors de la création de la table: {e}")


# **************** INSERTION DANS LA TABLE SQL ******************
def insert_data(connection, df):
    """
    Insère les données du DataFrame dans la table tweets_staging.
    Seules certaines colonnes principales sont insérées.
    """
    try:
        cursor = connection.cursor()
        # Colonnes ciblées pour l'insertion (à adapter selon vos besoins)
        cols = [
            "created_at", "followers", "friends", "location", "retweet_count",
            "screenname", "text", "clean_text", "twitter_id", "username",
            "polarity", "partition_0",
            "keyword_1", "keyword_2", "keyword_3", "keyword_4", "keyword_5", "keyword_6",
            "keyword_7", "keyword_8", "keyword_9", "keyword_10", "keyword_11", "keyword_12",
            "keyword_13", "keyword_14", "keyword_15", "keyword_16", "keyword_17", "keyword_18",
            "keyword_19", "keyword_20", "keyword_21", "keyword_22", "keyword_23", "keyword_24",
            "keyword_25", "keyword_26", "keyword_27", "keyword_28", "keyword_29", "keyword_30",
            "keyword_31", "keyword_32", "keyword_33", "keyword_34", "keyword_35", "keyword_36",
            "keyword_37", "keyword_38", "keyword_39", "keyword_40", "keyword_41", "keyword_42",
            "sentiment"
        ]
        insert_query = f"INSERT INTO tweets_staging ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})"
        
        # Préparation des valeurs pour insertion
        values = []
        for _, row in df.iterrows():
            row_values = []
            for col in cols:
                val = row.get(col, None)
                row_values.append(val)
            values.append(tuple(row_values))
        
        cursor.executemany(insert_query, values)
        connection.commit()
        print(f"{cursor.rowcount} lignes insérées avec succès dans MySQL.")
    except Error as e:
        print(f"Erreur lors de l'insertion des données: {e}")


# **************** VALLIDATION DES DONNEES ******************
def validate_data(connection):
    """Valide les données insérées en exécutant quelques requêtes SQL."""
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM tweets_staging")
        total_count = cursor.fetchone()[0]
        print(f"Nombre total de lignes dans tweets_staging: {total_count}")
        
        cursor.execute("SELECT id, text, sentiment FROM tweets_staging LIMIT 5")
        print("\nExemple de 5 premières lignes:")
        for row in cursor.fetchall():
            print(f"ID: {row[0]}, Text: {row[1][:50]}..., Sentiment: {row[2]}")
    except Error as e:
        print(f"Erreur lors de la validation des données: {e}")



def main():
    parser = argparse.ArgumentParser(
        description="Prétraitement des données Big Tech Twitter et envoi dans S3 staging (CSV) et insertion dans MySQL (couche Staging)"
    )
    parser.add_argument("--endpoint-url", type=str, default="http://localhost:4566",
                        help="Endpoint S3 (LocalStack ou AWS)")
    parser.add_argument("--bucket_raw", type=str, default="raw",
                        help="Nom du bucket RAW pour récupérer le fichier initial")
    parser.add_argument("--file-name", type=str, default="bigtech_combined.csv",
                        help="Nom du fichier dans le bucket RAW")
    parser.add_argument("--bucket_staging", type=str, default="staging",
                        help="Nom du bucket S3 pour stocker le CSV en staging")
    parser.add_argument("--s3_key", type=str, default="bigtech_staging.csv",
                        help="Clé (nom) du fichier dans le bucket staging")
    parser.add_argument("--db_host", type=str, required=True, help="Hôte MySQL")
    parser.add_argument("--db_user", type=str, required=True, help="Utilisateur MySQL")
    parser.add_argument("--db_password", type=str, required=True, help="Mot de passe MySQL")
    parser.add_argument("--db_database", type=str, default="staging", help="Base de données MySQL")
    
    args = parser.parse_args()

    # Récupération des données depuis le bucket RAW
    print("Récupération des données depuis le bucket RAW...")
    content = get_data_from_raw(args.endpoint_url, args.bucket_raw, args.file_name)
    if content is None:
        print("Aucune donnée récupérée depuis S3.")
        return
    
    # Application du prétraitement
    print("Nettoyage et transformation des données...")
    df_clean = clean_data(content)

    # Sauvegarde locale du CSV en staging
    staging_csv = "local_dataset/staging/bigtech_staging.csv"
    Path(Path(staging_csv).parent).mkdir(parents=True, exist_ok=True)
    df_clean.to_csv(staging_csv, index=False, encoding='utf-8')
    print(f"Fichier CSV prétraité sauvegardé localement dans : {staging_csv}")

    # Téléversement du CSV dans le bucket S3 staging
    print("Téléversement du CSV dans le bucket S3 staging...")
    upload_to_s3(staging_csv, args.bucket_staging, args.s3_key, args.endpoint_url)

    # Connexion à MySQL
    print("Connexion à MySQL...")
    connection = create_mysql_connection(args.db_host, args.db_user, args.db_password, args.db_database)
    if connection is None:
        return
    
    # Création de la table si nécessaire
    print("Création de la table tweets_staging si elle n'existe pas...")
    create_table(connection)
    
    # Insertion des données dans MySQL
    print("Insertion des données dans MySQL...")
    insert_data(connection, df_clean)
    
    # Validation des données insérées
    print("Validation des données insérées...")
    validate_data(connection)
    
    connection.close()
    print("Traitement terminé.")
    

if __name__ == "__main__":
    main()






