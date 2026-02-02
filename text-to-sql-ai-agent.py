import duckdb
from langchain.agents import create_agent
from langchain_openai import ChatOpenAI
from langchain_core.tools import tool
from langchain_core.messages import SystemMessage
import threading
from dotenv import load_dotenv
import os
import re
import sqlparse
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
def generate_sql(user_query: str, table_names: str, schema_info: str, db_relationship: str, llm_model) -> str:
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

    llm = llm_model

    response = llm.invoke(input=prompt)
    sql_query = response.content.strip()
    return sql_query

def validate_sql(sql_query: str) -> str:
    """Validate generated SQL query to ensure no forbidden statements are present using both sqlparse and regex."""
    sql_query = sql_query.strip()
    
    # Clean the query
    clean_query = re.sub(r';\s*$', '', sql_query)
    clean_query = re.sub(r'```sql\s*', '', clean_query, flags=re.IGNORECASE)

    # Check for multiple statements
    if clean_query.count(";") > 0 or (clean_query.endswith(";") and ";" in clean_query[:-1]):
        return "ERROR: Multiple SQL statements detected. Only single SELECT statements are allowed."
    
    clean_query = clean_query.rstrip(";").strip()

    # Use sqlparse to detect forbidden statements
    forbidden_types = {
        'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 
        'ALTER', 'TRUNCATE', 'GRANT', 'REVOKE', 'MERGE', 
        'COMMIT'
    }
    
    found_statements = []
    
    try:
        parsed = sqlparse.parse(clean_query)
        
        for statement in parsed:
            root_keyword = statement.get_type()
            
            if root_keyword in forbidden_types:
                found_statements.append({
                    "statement": root_keyword,
                    "full_query": str(statement).strip()
                })
            else:
                for token in statement.flatten():
                    if token.is_keyword and token.value.upper() in forbidden_types:
                        found_statements.append({
                            "statement": token.value.upper(),
                            "full_query": "Detected inside sub-query or block"
                        })
                        break
        
        if found_statements:
            forbidden_list = ', '.join([s['statement'] for s in found_statements])
            return f"ERROR: Forbidden SQL statements detected: {forbidden_list}"
    except Exception as e:
        print(f"Warning: sqlparse failed ({e}), falling back to regex")

    # Additional regex-based validation as backup
    dangerous_patterns = [
        r'\b(INSERT|UPDATE|DELETE|ALTER|DROP|CREATE|REPLACE|TRUNCATE|GRANT|REVOKE|MERGE|COMMIT)\b',
        r'\b(EXEC|EXECUTE)\b',
        r'--',  # SQL comments
        r'/\*',  # Block comments
    ]

    for pattern in dangerous_patterns:
        if re.search(pattern, clean_query, re.IGNORECASE):
            return "ERROR: Unsafe SQL pattern detected"
    
    print("! Query validation passed")
    return f"Valid: {clean_query}"

@tool
def execute_sql(sql_query: str) -> str:
    """Execute a SELECT query and return results as a string representation of the data."""
    try:
        # Validate first
        validation_result = validate_sql(sql_query)
        if validation_result.startswith("ERROR"):
            return f"""ERROR: Cannot execute SQL. {validation_result}

            Problematic SQL:
            {sql_query}

            Please regenerate the SQL query without these forbidden operations."""
        
        # Get thread-safe connection
        conn = get_db_connection()
        
        # Execute and immediately materialize to DataFrame
        result = conn.execute(sql_query)
        df = result.fetchdf()
        
        # Return string representation for tool output
        return df.to_string()
    
    except Exception as e:
        error_msg = str(e)
        return f"""ERROR: Failed to execute SQL query.

        Error Details: {error_msg}

        Problematic SQL:
        {sql_query}

        Please analyze the error and regenerate a corrected SQL query. Common issues:
        - Invalid table/column names (check schema)
        - Syntax errors
        - Type mismatches
        - Missing JOIN conditions
        """

@tool
def fix_sql_error(sql_query: str, error_message: str, llm_model) -> str:
    """Fix SQL query based on error message using the language model."""
    prompt = f"""
    The following SQL query resulted in an error when executed:

    SQL Query:
    {sql_query}

    Error Message:
    {error_message}

    Please analyze the error and provide a corrected SQL query. Ensure the new query adheres to the following rules:
    - Fix the issues that caused the error.
    - Avoid any DML operations.
    - Ensure all table and column names are valid as per the database schema.
    - Limit results to 100 rows.
    - Return only the corrected SQL query without any additional text.
    """

    llm = llm_model
    response = llm.invoke(input=prompt)
    return response.content.strip()

@tool
def result_analyzer(user_query: str, sql_query: str, result: str, llm_model) -> str:
    """Analyze the SQL execution result and provide insights."""
    prompt = f"""
    # User Query:
    {user_query}

    # Executed SQL Query:
    {sql_query}

    # SQL Execution Result:
    {result}

    # Responding Rules:
    - Analyze the SQL execution result in the context of the original user query.
    - Provide a concise summary of the findings from the result.
    - Use bullet points for clarity.
    - Use tables for structured data representation when applicable.
    - Suggest modifications to the SQL query to better align with the user's intent.
    - If the result is empty or does not address the user query, suggest specific changes to improve it.
    """

    llm = llm_model
    response = llm.invoke(input=prompt)
    return response.content.strip()


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
        tools = [generate_sql, validate_sql, execute_sql, fix_sql_error, result_analyzer],
        system_message = SystemMessage(content="You are a helpful assistant that translates natural language to SQL queries.")
    )