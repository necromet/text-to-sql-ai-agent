import duckdb
from langchain.agents import create_agent
from langchain_openai import ChatOpenAI
from langchain_core.tools import tool
from langchain_core.messages import SystemMessage
import threading
from dotenv import load_dotenv
import os
import re
from pathlib import Path

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

def get_column_types():
    conn = get_db_connection()
    result = conn.execute("SELECT table_name, column_name, data_type FROM information_schema.columns").fetchall()
    column_types = [(row[0], row[1], row[2]) for row in result]
    for table_name, column_name, data_type in column_types:
        print(f"- {table_name}.{column_name}: {data_type}")
    return column_types

def rel_db_relationship():
    return """
## Database Schema Relationships
- orders.customer_id = customers.customer_id
- orders.order_id = order_items.order_id
- orders.order_id = order_reviews.order_id
- orders.order_id = order_payments.order_id
- order_items.product_id = products.product_id
- order_items.seller_id = sellers.seller_id
- customers.customer_zip_code_prefix = geolocation.zip_code_prefix
- sellers.seller_zip_code_prefix = geolocation.zip_code_prefix

## Important Domain Notes
- customer_id is unique per order
- Use customer_unique_id to identify repeat customers
- Product categories are stored in Portuguese
- Use product_category_name_translation when English labels are required
"""

@tool
def generate_sql(user_query: str, table_names: str, schema_info: str, db_relationship: str, agent_model) -> str:
    """Generate SQL query from natural language."""
    dialect = "DuckDB"

    prompt = f"""
    User Query = {user_query}

    # System Prompt:
    You are an expert {dialect} SQL generator. Given a user query, generate the corresponding SQL query.
    Ensure the SQL is syntactically correct and optimized for {dialect}.
    Use the following database schema information to inform your SQL generation:

    # Tables:
    {table_names}

    # Columns and Types:
    {schema_info}

    # Relationships:
    {db_relationship}

    # Rules:
    - Only generate SELECT statements; do not use INSERT, UPDATE, DELETE, or other DML operations.
    - Ensure all table and column names are valid as per the schema provided.
    - Limit results to 100 rows
    - Returns only the SQL query without any additional text.
    """

    agent = agent_model

    response = agent.invoke(input=prompt)
    sql_query = response.content.strip()
    return sql_query

@tool
def validate_sql(sql_query: str) -> str:
    """Validate generated SQL query to ensure no forbidden statements are present."""
    sql_query = sql_query.strip()
    
    clean_query = re.sub(r';\s*$', '', sql_query)
    clean_query = re.sub(r'```sql\s*', '', clean_query, flags = re.IGNORECASE)

    if clean_query.count(";") > 0 or (clean_query.endswith(";") and ";" in clean_query[:-1]):
        return "ERROR: Multiple SQL statements detected. Only single SELECT statements are allowed."
    
    clean_query = clean_query.rstrip(";").strip()

    dangerous_patterns = [
        r'\b(INSERT|UPDATE|DELETE|ALTER|DROP|CREATE|REPLACE|TRUNCATE)\b',
        r'\b(EXEC|EXECUTE)\b',
        r'--',  # SQL comments
        r'/\*',  # Block comments
    ]

    for pattern in dangerous_patterns:
        if re.search(pattern, clean_query, re.IGNORECASE):
            return f"Error: Unsafe SQL pattern detected"
    
    print("! Query validation passed")
    return f"Valid: {clean_query}"


if __name__ == "__main__":
    conn = get_db_connection()

    table_names = get_table_name()
    column_types = get_column_types()
    rel_db_relationship_info = rel_db_relationship()

    agent_name = "gpt-4o-mini-2024-07-18"
    agent_model = ChatOpenAI(
        model = agent_name,
        temperature = 0.2,
        max_tokens = 5000
    )

    text_to_sql_agent = create_agent(
        agent = agent_model,
        tools = [generate_sql, validate_sql],
        system_message = SystemMessage(content="You are a helpful assistant that translates natural language to SQL queries.")
    )