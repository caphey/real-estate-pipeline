import os
import psycopg2
import pandas as pd
import requests
from dotenv import load_dotenv
from psycopg2 import sql

load_dotenv()

API_KEY = os.getenv("API_KEY")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_HOST = 'localhost'
DB_PORT = '5432'

SYMBOLS = ['AAPL', 'MSFT', 'GOOGL']
TABLE_NAME = 'raw_stock_data'

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print(f"Connexion réussie à la base de données {DB_NAME} sur {DB_HOST}:{DB_PORT}")
        return conn
    except psycopg2.Error as e:
        print(f"Erreur de connexion à la base de données {DB_NAME} sur {DB_HOST}:{DB_PORT} : {e}")
        return None

def fetch_stock_data(symbols):
    print(f"Récupération des données pour les symboles : {symbols}")
    url = (f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbols}&outputsize=compact&apikey={API_KEY}")

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        if 'Time Series (Daily)' not in data:
            print(f"Erreur de l'API pour {symbols}: {data.get('Note', 'Clé non trouvée')}")
            return None
        
        # Conversion des données en DataFrame plutôt qu'en dictionnaire JSON
        df = pd.DataFrame.from_dict(data['Time Series (Daily)'], orient='index')

        # Renommage des colonnes pour enlever les préfixes (ex : 1. open -> open)
        df = df.rename(columns=lambda x: x.split('. ')[1])

        df['symbol'] = symbols
        df['date'] = pd.to_datetime(df.index)

        print(f"Données pour {symbols} récupérées ({len((df))} lignes)")
        return df
    
    except requests.exceptions.RequestException as e:
        print(f"Erreur de requête HTTP pour {symbols}: {e}")
        return None
    except Exception as e:
        print(f"Erreur lors du traitement de données pour {symbols} : {e}")
        return None
    
def load_data_to_db(df, conn):
    df_load = df[['date', 'symbol', 'open', 'high', 'low', 'close', 'volume',]]
    
    # Créer la table SI ELLE N'EXISTE PAS (Idempotence)
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id SERIAL PRIMARY KEY,
                date DATE,
                symbol VARCHAR(10),
                open NUMERIC,
                high NUMERIC,
                low NUMERIC,
                close NUMERIC,
                volume BIGINT,
                
                UNIQUE(date, symbol) 
            );
        """)
        conn.commit()
    
    print(f"Chargement de {len(df_load)} lignes pour {df_load['symbol'].iloc[0]}...")

    with conn.cursor() as cur:
        for _, row in df_load.iterrows():
            cur.execute(
                f"""
                INSERT INTO {TABLE_NAME} (date, symbol, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date, symbol) DO NOTHING; 
                """,
                tuple(row)
            )
        conn.commit()
    print(f"Chargement pour {df_load['symbol'].iloc[0]} terminé")


def main():
    print("--- Début de la pipeline E-L ---")

    conn = get_db_connection()
    if conn is None:
        print("Echec de la connexion à la BDD")
        return
    
    all_data = []

    for symbol in SYMBOLS:
        data = fetch_stock_data(symbol)
        if data is not None:
            all_data.append(data)
        
    if not all_data:
        print("Aucune donnée n'a été récupérée. Arrêt")
        conn.close()
        return
    
    for df in all_data:
        try:
            load_data_to_db(df, conn)
        except Exception as e:
            print(f"Erreur lors du chargement des données pour {df['symbol'].iloc[0]}: {e}")            
            conn.rollback() # Annuler en cas d'erreur
    
    conn.close()
    print("--- Pipeline terminé")

if __name__ == "__main__":
    main()
        

        
