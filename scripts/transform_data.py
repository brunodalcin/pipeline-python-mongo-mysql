from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from extract_and_save_data import connect_mongo, create_connect_db, create_connect_collection
import pandas as pd
import requests
from dotenv import load_dotenv
import os

# load .env variables
load_dotenv()

# load user and password from .env
m_user = os.getenv("MONGO_USER")
m_password = os.getenv("MONGO_PASSWORD")

def visualize_collection(col):
    for doc in col.find():
      print(doc)

def rename_column(col, col_name, new_name):
    col.update_many({}, {"$rename": {f"{col_name}": f"{new_name}"}})

def select_category(col,category):   
   query = {"Categoria do Produto":f"{category}"}
   category_list = []

   for doc in col.find(query):
     category_list.append(doc)

   return category_list

def make_regex(col,regex):
    query = {"Data da Compra": {"$regex": f"{regex}" }}
    regex_list = []

    for doc in col.find(query):
        regex_list.append(doc)   

    return regex_list

def create_dataframe(list):
  df = pd.DataFrame(list)
  return df

def format_date(df):
  df["Data da Compra"] = pd.to_datetime(df["Data da Compra"], format="%d/%m/%Y")
  df["Data da Compra"] = df["Data da Compra"].dt.strftime("%Y-%m-%d")
  
def save_csv(df, path):
    df.to_csv(path, index=False)
    print(f"\nFile {path} was saved")  
   



if __name__ == "__main__":

    client = connect_mongo(f"mongodb+srv://{m_user}:{m_password}@cluster-pipeline.ahef7.mongodb.net/?retryWrites=true&w=majority&appName=Cluster-pipeline") # connection string from my cluster on MONGO ATLAS    
    db = create_connect_db(client, "db")
    col = create_connect_collection(db, "products")

    rename_column(col, "lat", "Latitude")
    rename_column(col, "lon", "Longitude")

    lst_livros = select_category(col, "livros")
    df_livros = create_dataframe(lst_livros)
    format_date(df_livros)
    save_csv(df_livros, "../data/tb_books.csv")

    lst_produtos = make_regex(col, "/202[1-9]")
    df_produtos = create_dataframe(lst_produtos)
    format_date(df_produtos)
    save_csv(df_produtos, "../data/tb_products.csv")   

