import duckdb
from langchain.agents import create_agent
from langchain_openai import ChatOpenAI
import threading
from dotenv import load_dotenv

load_dotenv()
thread_local = threading.local()
DB_PATHS = "/media/edward/SSD-Data/My Folder/text-to-sql-ai-agent/olist.db"


def get_db_connection():    
    if not hasattr(thread_local, "conn") or thread_local.conn is None:
        for db_path in DB_PATHS:
            try:
                conn = duckdb.connect(database=db_path, read_only=False)
                thread_local.conn = conn
                thread_local.db_path = db_path
                print(f" ! Database connected: {db_path}")
                break
            except Exception as e:
                print(f" ! Failed to connect to {db_path}: {e}")
                continue
        else:
            raise Exception("Failed to connect to any database path")
    return thread_local.conn