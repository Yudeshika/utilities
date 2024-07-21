import pymysql

def get_db_connection(host, user, password, db):
    return pymysql.connect(
        host=host,
        user=user,
        password=password,
        db=db,
        cursorclass=pymysql.cursors.DictCursor
    )

def get_table_columns(connection, database):
    with connection.cursor() as cursor:
        cursor.execute(f"USE {database}")
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()

        schema = {}
        for table in tables:
            table_name = table[f'Tables_in_{database}']
            cursor.execute(f"SHOW COLUMNS FROM {table_name}")
            columns = cursor.fetchall()
            schema[table_name] = {col['Field']: col for col in columns}
        return schema

def compare_schemas(schema1, schema2):
    differences = {}

    tables1 = set(schema1.keys())
    tables2 = set(schema2.keys())

    all_tables = tables1.union(tables2)

    for table in all_tables:
        if table not in schema1:
            differences[table] = {"missing_in_db1": True, "missing_in_db2": False, "columns": {}}
        elif table not in schema2:
            differences[table] = {"missing_in_db1": False, "missing_in_db2": True, "columns": {}}
        else:
            columns1 = schema1[table]
            columns2 = schema2[table]
            all_columns = set(columns1.keys()).union(set(columns2.keys()))
            column_diffs = {}

            for column in all_columns:
                if column not in columns1:
                    column_diffs[column] = {"missing_in_db1": True, "missing_in_db2": False}
                elif column not in columns2:
                    column_diffs[column] = {"missing_in_db1": False, "missing_in_db2": True}
                else:
                    col1 = columns1[column]
                    col2 = columns2[column]
                    col_diff = {}
                    for key in col1:
                        if col1[key] != col2.get(key):
                            col_diff[key] = {"db1": col1[key], "db2": col2.get(key)}
                    if col_diff:
                        column_diffs[column] = col_diff

            differences[table] = {"missing_in_db1": False, "missing_in_db2": False, "columns": column_diffs}

    return differences

def print_differences(differences):
    for table, diff in differences.items():
        print(f"Table: {table}")
        if diff["missing_in_db1"]:
            print(f"  Missing in database 1")
        elif diff["missing_in_db2"]:
            print(f"  Missing in database 2")
        else:
            for column, col_diff in diff["columns"].items():
                if "missing_in_db1" in col_diff:
                    print(f"  Column '{column}' missing in database 1")
                elif "missing_in_db2" in col_diff:
                    print(f"  Column '{column}' missing in database 2")
                else:
                    print(f"  Column '{column}' differs:")
                    for key, val in col_diff.items():
                        print(f"    {key}: db1 = {val['db1']}, db2 = {val['db2']}")
        print()

# Configuration for the databases
db1_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'db': 'cea_cd'
}

db2_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'db': 'bill_management_system' 
}

# Connect to the databases
connection1 = get_db_connection(**db1_config)
connection2 = get_db_connection(**db2_config)

# Get the schemas
schema1 = get_table_columns(connection1, db1_config['db'])
schema2 = get_table_columns(connection2, db2_config['db'])

# Compare the schemas
differences = compare_schemas(schema1, schema2)

# Print the differences
print_differences(differences)

# Close the connections
connection1.close()
connection2.close()
