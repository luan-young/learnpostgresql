import os
from psycopg2.pool import SimpleConnectionPool
from dotenv import load_dotenv


load_dotenv()
database_uri = os.environ.get("DATABASE_URI")

pool = SimpleConnectionPool(minconn=1, maxconn=10, dsn=database_uri)