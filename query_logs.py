import snowflake.connector
import pandas as pd

def establish_snowflake_connection():

    account = ''
    user = ''
    password = ''
    warehouse = ''
    database = ''
    schema = ''


    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
    )
    print('check-connection')

    return conn

def execute_snowflake_query(conn, query_limit):

    cursor = conn.cursor()

    total_queries = 0 
    chunk_size = 10000

    data = []

    while total_queries < query_limit:

        query = f"""
            SELECT START_TIME, QUERY_ID, QUERY_TEXT
            FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
                RESULT_LIMIT => {chunk_size},
                END_TIME_RANGE_START => DATEADD(HOUR, -160, CURRENT_TIMESTAMP()), -- Start from 1 hour ago
                END_TIME_RANGE_END => CURRENT_TIMESTAMP
            )) 
        """

        cursor.execute(query)

        rows = cursor.fetchall()

        data.extend(rows)

        total_queries += len(rows)

        if len(rows) < chunk_size or total_queries >= query_limit:

            break

    cursor.close()

    return data

def save_to_csv(data, filename):
    df = pd.DataFrame(data, columns=['START_TIME', 'QUERY_ID', 'QUERY_TEXT'])
    df.to_csv(filename, index=False)

def close_snowflake_connection(conn):

    conn.close()

def main():

    snowflake_conn = establish_snowflake_connection()

    query_limit = 1000000

    data = execute_snowflake_query(snowflake_conn, query_limit)

    save_to_csv(data, 'query_results.csv')

    close_snowflake_connection(snowflake_conn)

if __name__ == "__main__":
    main()