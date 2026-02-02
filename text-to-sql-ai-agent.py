import duckdb
from langchain.agents import create_agent
from langchain_openai import ChatOpenAI
from langchain_core.tools import tool
from langchain_core.messages import SystemMessage
import threading
from dotenv import load_dotenv
import os

load_dotenv()
thread_local = threading.local()



def get_db_connection():    
    if not hasattr(thread_local, "conn") or thread_local.conn is None:
        for db_path in os.getenv("DB_PATHS").split(":"):
            try:
                conn = duckdb.connect(database=db_path, read_only=False)
                thread_local.conn = conn
                thread_local.db_path = db_path
                print(f" * Database connected: {db_path}")
                break
            except Exception as e:
                print(f" ! Failed to connect to {db_path}: {e}")
                continue
        else:
            raise Exception("Failed to connect to any database path")
    return thread_local.conn

def get_table_name():
    conn = get_db_connection()
    result = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main';").fetchall()
    table_names = [row[0] for row in result]
    return table_names


if __name__ == "__main__":
    conn = get_db_connection()