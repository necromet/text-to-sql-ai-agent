def generate_sql_prompt(user_query: str, table_names: str, schema_info: str, db_relationship: str, dialect: str = "DuckDB") -> str:
    """Generate prompt for SQL generation tool."""
    prompt = f"""
    User Query = {user_query}

    # System Prompt:
    You are an expert {dialect} SQL generator. Given a user query, generate the corresponding SQL query.
    Ensure the SQL is syntactically correct and optimized for {dialect}.
    
    Your role is to transform user input into a SQL query that performs only aggregation and data summarization. Every query you generate must use aggregate functions (such as COUNT(), SUM(), AVG(), WINDOW_MAX(), WINDOW_MIN(), WINDOW_AVG(), WINDOW_SUM()) and appropriate GROUP BY clauses where necessary. Do not return raw row-level data; only summarization.

    Example output will be only the SQL Query. Add necessary JOINs to get all relevant information. Use table and column names exactly as provided in the schema information. Do not make up any table or column names. Do not include any explanations, only return the SQL query.

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
    - Cut identifier strings to a maximum of 10 characters.
    - Returns only the SQL query without any additional text.
    - Today's date is 2026-02-03. The date in the database is far from the current date, so do not use current_date or similar functions.
    """
    return prompt


def fix_sql_error_prompt(sql_query: str, error_message: str) -> str:
    """Generate prompt for SQL error fixing tool."""
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
    return prompt


def result_analyzer_prompt(user_query: str, sql_query: str, result: str) -> str:
    """Generate prompt for result analysis tool."""
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
    return prompt


def text_to_sql_agent_prompt(user_query: str, table_names: str, schema_relationships: str, column_types: str) -> str:
    prompt = f"""
    # User Query:
    {user_query}

    # System Prompt: 
    You are a data analyzer expert working with Brazillian e-commerce data. Your task is to analyze user queries and give business insights by generating and executing SQL queries on the database.

    Your workflow for answering questions:
    1. Use `generate_sql` to create SQL based on the question
    2. Use `execute_sql` to run the validated query
    3. If there's an error, use `fix_sql_error` to correct it and try again (up to 3 times)
    4. Use `result_analyzer` to provide a natural language answer

    Rules when responding:
    - Use only the provided tools for SQL generation, execution, error fixing, and result analysis. 
    - Limit results to 100 rows
    - If a query fails, use the fix tool and try again (up to 3 times)
    - Provide clear, informative answers. You can also give speculative business insights based on the data.
    - Be precise with table and column names. Do not invent names or relationships.
    - Handle errors gracefully and try to fix them (up to 3 times)
    - If you fail after 3 attempts, explain what went wrong
    - Always assist the user at the end by asking the user if they need more help.

    Database Schema: 
    {table_names}

    Column Types:
    {column_types}

    Schema Relationships:
    {schema_relationships}
    """
    return prompt
