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
    text = re.sub(r"[^\w\s]", "", text)  # supprime les caractères spéciaux restants (conserver lettres, chiffres et espaces)
    text = re.sub(r"\s+", " ", text)    # supprime les espaces multiples

    return text.strip()

    

# **************** NETTOYAGE DES DONNEES -2- ******************
def clean_data(content):
    """
    Nettoie les données (suppression des doublons, des lignes vides etc.)
    Diverses transformations seront appliquer pour garantir un formattage 
    et une standardisation des données.
    """
    
    df = pd.read_csv(StringIO(content))
    print(f"df initial : {df.shape}, colonnes : {df.columns.tolist()}, dtypes :\n{df.dtypes}")
    
    # Suppression des lignes vides dans la colonne 'text'
    df = df.dropna(subset=['text'])
    print(f"Lignes vides : {df['text'].isna()}")

    # Suppression des colonnes inutiles
    print(f"==== Supprimer file_name et partition_1 ====")
    columns_to_drop = ["file_name","partition_1"]
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
    # On conserve le symbole '#' pour les repérer  
    df_w_keyword =  df_no_duplicated.copy()
    df_w_keyword['hashtags_list'] = df_w_keyword['text'].apply(lambda x: extract_hashtags(x) if isinstance(x, str) else [])
    max_tags = df_w_keyword['hashtags_list'].apply(len).max()
    print(f"Nombre de hashtags max : {max_tags}")

    # Création de colonnes distinctes pour chaque hashtag (une colonne par hashtag)
    for i in range(max_tags):
        df_w_keyword[f'keyword_{i+1}'] = df_w_keyword['hashtags_list'].apply(lambda tags: tags[i] if len(tags) > i else None)
    print(f"Colonnes avec les mots clés nrmlt : {df_w_keyword.columns}")
    print(df_w_keyword.head(5))

    # Nettoyage de la colonne 'text' : d'abord on retire les hashtags
    df_preprocessed = df_w_keyword.copy()
    df_preprocessed['clean_text'] = df_preprocessed['text'].apply(clean_text_func)
    print(f"Colonnes avec 'clean_text' nrmlt : {df_preprocessed.columns}")

    # Conversion de la colonne 'polarity' en numérique (float)
    df_preprocessed['polarity'] = pd.to_numeric(df_preprocessed['polarity'], errors='coerce')

    # Création de la colonne 'sentiment' à partir de la polarité pour une meilleure interprétation
    def polarity_to_sentiment(polarity):
        if pd.isnull(polarity):
            return None
        if polarity == 0.0:
            return 'neutral'
        elif polarity >= 0.5:
            return 'positive'
        else:
            return 'negative'
    df_preprocessed['sentiment'] = df_preprocessed['polarity'].apply(polarity_to_sentiment)
    print(f"Sentiment column : {df_preprocessed['sentiment'].value_counts()}")

    # Correction des types de colonnes
    if 'created_at' in df_preprocessed.columns:
        df_preprocessed['created_at'] = pd.to_datetime(df_preprocessed['created_at'], errors='coerce')

    # Conversion des colonnes numériques : 'followers', 'friends', 'retweet_count', 'twitter_id', 'polarity'
    # retweet_count :float, 'twitter_id': long?, polarity:'float'...
    for col in ['followers', 'friends', 'retweet_count']:
        if col in df_preprocessed.columns:
            df_preprocessed[col] = pd.to_numeric(df_preprocessed[col], errors='coerce')
    print("Df nettoyé\n")
    print(f"Version finale : {df_preprocessed.head(5)}")


    # ====== Encodage des variables catégoriques ======
    print("==== encodage des var catégoriques ====")
    # categorical_cols=["group_name","location","search_query","partition_0"]     # partition_0 a que 1 seule valeur unique donc inutile de l'encoder
    # Pour 'group_name' et 'search_query', le one-hot encoding est raisonnable
    categorical_cols = ["group_name", "search_query"]
    df_encoded = pd.get_dummies(df_preprocessed, columns=[col for col in categorical_cols if col in df_preprocessed.columns])

    # Pour 'location', avec 84k valeurs uniques, il est préférable de ne pas appliquer get_dummies ici
    # Vous pourrez appliquer un encodage (ex : label encoding ou top N) lors de la phase ML (ou curated) si nécessaire.
    print("=== Df prétraité final (après encodage) ===:")
    print(df_encoded['clean_text'].head(5))
    # ===================================================

    # On met 'clean_text" a la suite de 'text'
    cols = list(df_encoded.columns)
    if 'text' in cols and 'clean_text' in cols:
        cols.remove('clean_text')
        idx = cols.index('text') + 1
        cols.insert(idx, 'clean_text')
        df_encoded = df_encoded[cols]

    return df_encoded


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
    Crée la table tweets si elle n'existe pas.
    """
    try:
        cursor = connection.cursor()
        # hashtags_list TEXT, a modifier !!!!!!!!!!!!!!
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tweets_staging (
                id INT AUTO_INCREMENT PRIMARY KEY,
                created_at DATETIME,
                followers INT,
                friends INT,
                group_name VARCHAR(255),
                location VARCHAR(255),
                retweet_count INT,
                screenname VARCHAR(255),
                search_query VARCHAR(255),
                text TEXT,
                twitter_id VARCHAR(255),
                username VARCHAR(255),
                polarity FLOAT,
                partition_0 VARCHAR(255),
                hashtags_list TEXT,         
                clean_text TEXT,
                sentiment VARCHAR(20)
            )
        """)
        connection.commit()
    except Error as e:
        print(f"Erreur lors de la création de la table: {e}")


def insert_data(connection, df):
    """
    Insère les données du DataFrame dans la table tweets_staging.
    Seules certaines colonnes principales sont insérées.
    """
    try:
        cursor = connection.cursor()
        # Colonnes ciblées pour l'insertion (à adapter selon vos besoins)
        cols = [
            "created_at", "followers", "friends", "group_name", "location",
            "retweet_count", "screenname", "search_query", "text", "twitter_id",
            "username", "polarity", "partition_0", "hashtags_list", "clean_text", "sentiment"
        ]
        insert_query = f"INSERT INTO tweets_staging ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})"
        
        # Préparation des valeurs
        values = []
        for _, row in df.iterrows():
            row_values = []
            for col in cols:
                val = row.get(col, None)
                # Pour hashtags_list, convertit la liste en chaîne de caractères
                if col == "hashtags_list" and isinstance(val, list):
                    val = str(val)
                row_values.append(val)
            values.append(tuple(row_values))
        
        cursor.executemany(insert_query, values)
        connection.commit()
        print(f"{cursor.rowcount} lignes insérées avec succès dans MySQL.")
    except Error as e:
        print(f"Erreur lors de l'insertion des données: {e}")


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
        description="Prétraitement des données Big Tech Twitter et insertion dans MySQL (couche Staging)"
    )
    parser.add_argument("--endpoint-url", type=str, default="http://localhost:4566",
                        help="Endpoint S3 (LocalStack ou AWS)")
    parser.add_argument("--bucket", type=str, default="raw",
                        help="Nom du bucket RAW")
    parser.add_argument("--file-name", type=str, default="bigtech_combined.csv",
                        help="Nom du fichier dans le bucket RAW")
    parser.add_argument("--db_host", type=str, required=True, help="Hôte MySQL")
    parser.add_argument("--db_user", type=str, required=True, help="Utilisateur MySQL")
    parser.add_argument("--db_password", type=str, required=True, help="Mot de passe MySQL")
    parser.add_argument("--db_database", type=str, default="staging", help="Base de données MySQL")
    
    args = parser.parse_args()

    # Récupération des données depuis le bucket RAW
    print("Récupération des données depuis le bucket RAW...")
    content = get_data_from_raw(args.endpoint_url, args.bucket, args.file_name)
    if content is None:
        print("Aucune donnée récupérée depuis S3.")
        return
    
    # Application du prétraitement
    print("Nettoyage et transformation des données...")
    df_clean = clean_data(content)
    
    # Réorganiser les colonnes pour placer 'clean_text' juste après 'text'
    cols = list(df_clean.columns)
    if 'text' in cols and 'clean_text' in cols:
        cols.remove('clean_text')
        idx = cols.index('text') + 1
        cols.insert(idx, 'clean_text')
        df_clean = df_clean[cols]
    
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






