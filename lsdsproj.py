import sqlparse
import re

def try_parse_value(value):
    value = value.strip("'\"")
    try:
        return int(value)
    except ValueError:
        return value

def parse_where(where_clause):
    where_clause = where_clause.strip()
    conditions = where_clause.split('AND')
    query = {}
    for cond in conditions:
        cond = cond.strip()
        if '>=' in cond:
            field, value = cond.split('>=')
            query[field.strip()] = {"$gte": try_parse_value(value.strip())}
        elif '<=' in cond:
            field, value = cond.split('<=')
            query[field.strip()] = {"$lte": try_parse_value(value.strip())}
        elif '>' in cond:
            field, value = cond.split('>')
            query[field.strip()] = {"$gt": try_parse_value(value.strip())}
        elif '<' in cond:
            field, value = cond.split('<')
            query[field.strip()] = {"$lt": try_parse_value(value.strip())}
        elif '=' in cond:
            field, value = cond.split('=')
            query[field.strip()] = try_parse_value(value.strip())
    return query

def parse_insert(parsed):
    raw_sql = str(parsed)
    
    # Extract table name
    match_table = re.search(r"INSERT\s+INTO\s+(\w+)", raw_sql, re.IGNORECASE)
    table_name = match_table.group(1) if match_table else "unknown"

    # Extract column names
    match_columns = re.search(r"\((.*?)\)", raw_sql)
    columns = [col.strip() for col in match_columns.group(1).split(',')] if match_columns else []

    # Extract values
    match_values = re.search(r"VALUES\s*\((.*?)\)", raw_sql, re.IGNORECASE)
    values = [try_parse_value(val.strip()) for val in match_values.group(1).split(',')] if match_values else []

    # Build MongoDB document
    document = dict(zip(columns, values)) if columns and values else {}
    return f"db.{table_name}.insertOne({document})"

def parse_delete(parsed):
    tokens = parsed.tokens
    table_name = ''
    where_clause = ''
    for idx, token in enumerate(tokens):
        if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'FROM':
            table_name = str(tokens[idx + 2]).strip()
        if isinstance(token, sqlparse.sql.Where):
            where_clause = str(token).lstrip('WHERE').strip()
    query_filter = parse_where(where_clause) if where_clause else {}
    return f"db.{table_name}.deleteMany({query_filter})"

def parse_update(parsed):
    tokens = parsed.tokens
    table_name = ''
    set_clause = ''
    where_clause = ''
    for idx, token in enumerate(tokens):
        if token.ttype is sqlparse.tokens.DML and token.value.upper() == 'UPDATE':
            table_name = str(tokens[idx + 2]).strip()
        if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'SET':
            set_clause = str(tokens[idx + 2]).strip()
        if isinstance(token, sqlparse.sql.Where):
            where_clause = str(token).lstrip('WHERE').strip()
    updates = {}
    for part in set_clause.split(','):
        field, value = part.split('=')
        updates[field.strip()] = try_parse_value(value.strip())
    query_filter = parse_where(where_clause) if where_clause else {}
    return f"db.{table_name}.updateMany({query_filter}, {{'$set': {updates}}})"

def parse_create(parsed):
    raw_sql = str(parsed)

    # Extract table name
    match_table = re.search(r"CREATE\s+TABLE\s+(\w+)", raw_sql, re.IGNORECASE)
    table_name = match_table.group(1) if match_table else "unknown"

    # Extract columns and data types
    match_columns = re.search(r"\((.*?)\)", raw_sql)
    columns = match_columns.group(1).split(',') if match_columns else []
    column_definitions = {}
    for col in columns:
        col_parts = col.strip().split()
        if len(col_parts) > 1:
            column_definitions[col_parts[0]] = col_parts[1]
    
    
    return f"db.createCollection('{table_name}')"

def parse_drop(parsed):
    raw_sql = str(parsed)

    # Extract table name
    match_table = re.search(r"DROP\s+TABLE\s+(\w+)", raw_sql, re.IGNORECASE)
    table_name = match_table.group(1) if match_table else "unknown"
    
    # Drop MongoDB collection
    return f"db.{table_name}.drop()"

def parse_join(parsed):
    sql_query = str(parsed)
    
    # Extract SELECT columns (after SELECT keyword)
    select_columns = re.search(r'SELECT\s+(.*?)\s+FROM', sql_query, re.IGNORECASE)
    if select_columns:
        columns = [col.strip() for col in select_columns.group(1).split(',')]
    else:
        columns = []

    # Extract FROM table (after FROM keyword)
    from_table = re.search(r'FROM\s+(\w+)', sql_query, re.IGNORECASE)
    if from_table:
        left_table = from_table.group(1)
    else:
        left_table = ""

    # Extract JOIN table (after JOIN keyword)
    join_table = re.search(r'JOIN\s+(\w+)', sql_query, re.IGNORECASE)
    if join_table:
        right_table = join_table.group(1)
    else:
        right_table = ""

    # Extract ON condition (after ON keyword)
    on_condition = re.search(r'ON\s+(\w+\.\w+)\s*=\s*(\w+\.\w+)', sql_query, re.IGNORECASE)
    if on_condition:
        left_field, right_field = on_condition.groups()
        left_field = left_field.split('.')[-1]  
        right_field = right_field.split('.')[-1]  
    else:
        left_field = right_field = ""

    # Build the MongoDB $lookup query
    lookup_query = {
        '$lookup': {
            'from': right_table,
            'localField': left_field,
            'foreignField': right_field,
            'as': right_table
        }
    }

    # Build projection 
    projection = {}
    for col in columns:
        parts = col.split('.')
        if len(parts) == 2:
            table_alias, col_name = parts
            if table_alias.strip() == left_table:
                projection[col_name.strip()] = 1
            elif table_alias.strip() == right_table:
                projection[f'{right_table}.{col_name.strip()}'] = 1

    # Handle edge case 
    if not projection:
        projection = {col: 1 for col in columns}

    return f"""db.{left_table}.aggregate([
    {lookup_query},
    {{'$unwind': '${right_table}'}}, 
    {{'$project': {projection}}}
])"""

def sql_to_mongo(sql_query):
    sql_query = sql_query.strip()
    if sql_query.endswith(';'):
        sql_query = sql_query[:-1]
    
    parsed = sqlparse.parse(sql_query)[0]
    command = parsed.tokens[0].value.upper()

    if command == 'SELECT':
        
        if any(token.value.upper() == 'JOIN' for token in parsed.tokens if not token.is_whitespace):
            return parse_join(parsed)
        else:
            return parse_select(parsed)
    elif command == 'INSERT':
        return parse_insert(parsed)
    elif command == 'DELETE':
        return parse_delete(parsed)
    elif command == 'UPDATE':
        return parse_update(parsed)
    elif command == 'CREATE':
        return parse_create(parsed)
    elif command == 'DROP':
        return parse_drop(parsed)
    else:
        return "-- Unsupported SQL command --"

def parse_select(parsed):
    tokens = parsed.tokens
    columns = []
    table_name = ''
    where_clause = ''
    for idx, token in enumerate(tokens):
        if token.ttype is sqlparse.tokens.DML and token.value.upper() == 'SELECT':
            column_token = tokens[idx + 2]
            columns_text = str(column_token).strip()
            columns = [col.strip() for col in columns_text.split(',')]
        if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'FROM':
            table_token = tokens[idx + 2]
            table_name = str(table_token).strip()
        if isinstance(token, sqlparse.sql.Where):
            where_clause = str(token).lstrip('WHERE').strip()
    query_filter = parse_where(where_clause) if where_clause else {}
    projection = {col: 1 for col in columns}
    if 'id' not in projection:
        projection['id'] = 0
    return f"db.{table_name}.find({query_filter}, {projection})"

# Example usage
sql_queries = [
    "SELECT name, sex, id FROM user;",
    "INSERT INTO user (name, age, sex) VALUES ('Alice', 30, 'F');",
    "DELETE FROM user WHERE age >= 30;",
    "UPDATE user SET name = 'Bob', age = 25 WHERE id = 1;",
    "SELECT user.name, orders.amount FROM user JOIN orders ON user.id = orders.user_id;",
    "CREATE TABLE users (id INT, name VARCHAR(100), age INT);",
    "DROP TABLE users;"
]

for sql in sql_queries:
    print("\nSQL:", sql)
    print("MongoDB:", sql_to_mongo(sql))


while True:
    user_input = input("\nEnter your SQL query (or type 'exit' to quit): ")
    if user_input.lower() == 'exit':
        break
    else:
        print("MongoDB:", sql_to_mongo(user_input))
