from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests
from dotenv import load_dotenv
import os

# load .env variables
load_dotenv()

# load user and password from .env
m_user = os.getenv("MONGO_USER")
m_password = os.getenv("MONGO_PASSWORD")

def connect_mongo(uri):
    client = MongoClient(uri, server_api=ServerApi('1'))
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)
    return client

def create_connect_db(client,db_name):
    db = client[db_name]
    return db

def create_connect_collection(db,col_name):
    collection = db[col_name]
    return collection

def extract_api_data(url): 
    return requests.get(url).json()

def insert_data(col, data):
     docs = col.insert_many(data)
     return len(docs.inserted_ids)



if __name__=="__main__":

    client = connect_mongo(f"mongodb+srv://{m_user}:{m_password}@cluster-pipeline.ahef7.mongodb.net/?retryWrites=true&w=majority&appName=Cluster-pipeline") # connection string from my cluster on MONGO ATLAS
    db = create_connect_db(client,"db")
    collection = create_connect_collection(db,"products")
    
    data = extract_api_data("https://labdados.com/produtos") # fictitious data
    print(f"\nQty extracted data: {len(data)}")
    
    qty_docs = insert_data(collection,data)
    print(f"\nQty inserted data: {qty_docs}") 
    
    client.close