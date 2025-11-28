# Databricks notebook source
# Configuration
# Replace this with your actual data path
data_path = "dbfs:/tmp/tpcds_sf4" 

# Path to the SQL file. 
# If you uploaded it to DBFS, use /dbfs/...
# If you are in a Databricks Repo/Workspace, you can use the relative path or absolute path.
sql_file_path = "create_tpcds_tables_spark_parameterized.sql"

# List of tables to convert to Delta
# These names correspond to the tables defined in the SQL file (without the _csv suffix)
tables = [
    "dbgen_version", "customer_address", "customer_demographics", "date_dim", 
    "warehouse", "ship_mode", "time_dim", "reason", "income_band", "item", 
    "store", "call_center", "customer", "web_site", "store_returns", 
    "household_demographics", "web_page", "promotion", "catalog_page", 
    "inventory", "catalog_returns", "web_returns", "web_sales", 
    "catalog_sales", "store_sales"
]

# 1. Create Temporary CSV Tables
print("Creating temporary CSV tables...")
with open(sql_file_path, "r") as f:
    sql_content = f.read()

# Replace the placeholder with the actual path
sql_content = sql_content.replace("${DATA_PATH}", data_path)

# Split and execute statements
statements = sql_content.split(";")
for statement in statements:
    statement = statement.strip()
    if statement:
        try:
            spark.sql(statement)
        except Exception as e:
            print(f"Error executing DDL statement: {e}")
            # raise e # Uncomment to stop on error

print("CSV tables created.")

# 2. Convert to Delta Tables
print("Converting to Delta tables...")
for table in tables:
    csv_table = f"{table}_csv"
    delta_table = table
    
    print(f"Processing {table}...")
    
    try:
        # Check if CSV table exists
        if spark.catalog.tableExists(csv_table):
            # Create Delta table from CSV table
            # This preserves the schema defined in the CSV table DDL
            spark.sql(f"DROP TABLE IF EXISTS {delta_table}")
            spark.sql(f"CREATE TABLE {delta_table} USING DELTA AS SELECT * FROM {csv_table}")
            
            # Optimize the Delta table (optional, but recommended for performance)
            # spark.sql(f"OPTIMIZE {delta_table}")
            
            print(f"Successfully created Delta table: {delta_table}")
            
            # Clean up CSV table definition (optional)
            spark.sql(f"DROP TABLE {csv_table}")
        else:
            print(f"Warning: Source table {csv_table} not found. Skipping.")
            
    except Exception as e:
        print(f"Error converting {table}: {e}")

print("Finished converting all tables to Delta.")
