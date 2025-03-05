import dask.dataframe as dd
from sqlalchemy import create_engine

# Replace these with your Oracle database credentials
USER = 'your_username'
PASSWORD = 'your_password'
HOST = 'your_host'  # e.g., 127.0.0.1 or your-db-host.com
PORT = 1521          # Default Oracle port
SERVICE_NAME = 'your_service_name'  # e.g., ORCL or your custom service name

# Create an SQLAlchemy connection string
connection_string = f'oracle+cx_oracle://{USER}:{PASSWORD}@{HOST}:{PORT}/?service_name={SERVICE_NAME}'

# Create SQLAlchemy engine
engine = create_engine(connection_string)

# Define your table name and index column
TABLE_NAME = 'your_table_name'
INDEX_COLUMN = 'your_primary_key_column'  # Use a column that can be indexed for efficiency

# Read data into a Dask DataFrame using SQLAlchemy engine
dask_df = dd.read_sql_table(
    TABLE_NAME,
    con=engine,
    index_col=INDEX_COLUMN,
    npartitions=100  # Adjust partitions based on your machine's RAM and CPU cores
)

# Optional: Inspect the data
print(dask_df.head())

# Perform operations on the Dask DataFrame
result = dask_df.groupby('some_column').sum().compute()  # Example operation
print(result)
