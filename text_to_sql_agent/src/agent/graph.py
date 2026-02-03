import duckdb
from langchain.agents import create_agent
from langchain_openai import ChatOpenAI
from langchain_core.tools import tool
import threading
from dotenv import load_dotenv
import os
import re
import sqlparse
from langgraph.graph import StateGraph, MessagesState, START, END
from agent.system_prompt import (
    generate_sql_prompt,
    fix_sql_error_prompt,
    result_analyzer_prompt,
    text_to_sql_agent_prompt
)

llm_name = "gpt-4o-mini-2024-07-18"
llm_model = ChatOpenAI(
    model = llm_name,
    temperature = 0.2,
    max_tokens = 5000
)

load_dotenv()

# Database paths (try in order)
DB_PATHS = [
    "C:\\Users\\OSVALDO-SOFTENG\\Documents\\edward-portfolio\\GIT\\ai-data-analyzer\\olist.db",
    "/media/edward/SSD-Data/My Folder/ai-data-analyzer/olist.db"
]
thread_local = threading.local()

def get_db_connection():
    """Get a thread-safe database connection."""
    if not hasattr(thread_local, "conn") or thread_local.conn is None:
        for db_path in DB_PATHS:
            try:
                conn = duckdb.connect(database=db_path, read_only=False)
                thread_local.conn = conn
                thread_local.db_path = db_path
                print(f" ! Database connected: {db_path}")
                conn.execute("LOAD spatial;")
                conn.execute("LOAD httpfs;")
                conn.execute("LOAD fts;")
                conn.execute("LOAD icu;")
                print(f" ! Spatial, HTTP, FTS, ICU loaded in database: {db_path}")
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
    return str(table_names)

def get_column_types():
    conn = get_db_connection()
    result = conn.execute("SELECT table_name, column_name, data_type FROM information_schema.columns").fetchall()
    column_types = [(row[0], row[1], row[2]) for row in result]
    for table_name, column_name, data_type in column_types:
        print(f"- {table_name}.{column_name}: {data_type}")
    return str(column_types)

def db_schema_relationship():
    prompt = """
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
    return prompt

table_names = get_table_name()
column_types = get_column_types()
db_schema_relationship_info = db_schema_relationship()

@tool
def generate_sql(user_query: str) -> str:
    """Generate SQL query from natural language."""
    prompt = generate_sql_prompt(
        user_query=user_query,
        table_names=table_names,
        schema_info=column_types,
        db_relationship=db_schema_relationship_info,
        dialect="DuckDB"
    )

    llm = ChatOpenAI(model="gpt-4o-mini-2024-07-18", temperature=0.2)
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
def fix_sql_error(sql_query: str, error_message: str) -> str:
    """Fix SQL query based on error message using the language model."""
    prompt = fix_sql_error_prompt(sql_query=sql_query, error_message=error_message)

    llm = ChatOpenAI(model="gpt-4o-mini-2024-07-18", temperature=0.2)
    response = llm.invoke(input=prompt)
    return response.content.strip()

@tool
def result_analyzer(user_query: str, sql_query: str, result: str) -> str:
    """Analyze the SQL execution result and provide insights."""
    prompt = result_analyzer_prompt(user_query=user_query, sql_query=sql_query, result=result)

    llm = ChatOpenAI(model="gpt-4o-mini-2024-07-18", temperature=0.2)
    response = llm.invoke(input=prompt)
    return response.content.strip()

system_prompt = text_to_sql_agent_prompt(
    user_query="",
    table_names=table_names,
    schema_relationships=db_schema_relationship_info,
    column_types=column_types
)

agent = create_agent(
    llm_model, 
    tools=[generate_sql, execute_sql, fix_sql_error, result_analyzer],
    system_prompt=system_prompt
)

# Define the graph
graph = (
    StateGraph(MessagesState)
    .add_node("agent",agent)
    .add_edge(START, "agent")
    .add_edge("agent", END)
    .compile(name="Text to SQL AI Agent Graph")
)
