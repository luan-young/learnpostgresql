import os
import psycopg2
from dotenv import load_dotenv


load_dotenv()

def create_connection():
    database_uri = os.environ.get("DATABASE_URI")
    return psycopg2.connect(database_uri)