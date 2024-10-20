import snowflake.connector
from .configurations import get_snowflake_config

# this, of course, should be improved by adding proper auth, paging, exception handling, logging, etc.
def get_snowflake_connection():
    config = get_snowflake_config()
    return snowflake.connector.connect(**config)

def fetch_all_from_view(view_name):
    conn = get_snowflake_connection()
    cur = conn.cursor()
    
    try:
        cur.execute(f"SELECT * FROM {view_name}")
        rows = cur.fetchall()
    except Exception as e:
        raise e  # TODO: Add logging
    finally:
        cur.close()
        conn.close()

    return rows
