import psycopg2, os
from dotenv import load_dotenv
load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("REDSHIFT_HOST"),
    port=int(os.getenv("REDSHIFT_PORT", 5439)),
    dbname=os.getenv("REDSHIFT_DB"),
    user=os.getenv("REDSHIFT_USER"),
    password=os.getenv("REDSHIFT_PASSWORD"),
    sslmode="require"
)
cur = conn.cursor()
cur.execute("""
    SELECT table_schema, table_name 
    FROM information_schema.tables 
    WHERE table_type = 'BASE TABLE'
    AND table_schema NOT IN ('pg_catalog', 'information_schema')
    ORDER BY table_schema, table_name
""")
for row in cur.fetchall():
    print(f"{row[0]}.{row[1]}")
conn.close()