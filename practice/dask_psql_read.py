import dask.bag as db
import dask.dataframe as dd
import cx_Oracle
import pandas as pd

def fetch_data_from_oracle_in_chunks(user, password, host, port, service_name, query, chunk_size=50000):
    """Fetch data from Oracle in chunks using cx_Oracle."""
    dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
    conn = cx_Oracle.connect(user=user, password=password, dsn=dsn)

    # Use a cursor to execute the query
    cursor = conn.cursor()
    cursor.execute(query)

    # Fetch data in chunks
    while True:
        chunk = cursor.fetchmany(chunk_size)
        if not chunk:
            break
        # Convert each row in chunk to a dictionary
        columns = [col[0] for col in cursor.description]
        for row in chunk:
            yield dict(zip(columns, row))

    # Cleanup
    cursor.close()
    conn.close()

def read_oracle_data_in_parallel(user, password, host, port, service_name, query, partitions=200, chunk_size=50000):
    """Read Oracle data using Dask in parallel."""
    # Create a Dask Bag from the data generator
    data_bag = db.from_sequence(
        fetch_data_from_oracle_in_chunks(user, password, host, port, service_name, query, chunk_size),
        npartitions=partitions
    )

    # Convert Dask Bag to Dask DataFrame
    df = data_bag.to_dataframe()

    # Trigger computation and inspect the first few rows
    print("Loaded data:")
    print(df.head())

    return df

# Sample usage
user = 'your_username'
password = 'your_password'
host = 'your_host'
port = 1521
service_name = 'your_service_name'
query = "SELECT * FROM employee_fact WHERE debt='cse'"

# Efficiently read data
dask_df = read_oracle_data_in_parallel(user, password, host, port, service_name, query)

# Perform some operations (example: group by department and count)
result = dask_df.groupby('department').size().compute()
print(result)
