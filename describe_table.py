import psycopg2, os
from dotenv import load_dotenv
load_dotenv()
conn = psycopg2.connect(host=os.getenv("REDSHIFT_HOST"),port=int(os.getenv("REDSHIFT_PORT",5439)),dbname=os.getenv("REDSHIFT_DB"),user=os.getenv("REDSHIFT_USER"),password=os.getenv("REDSHIFT_PASSWORD"),sslmode="require")
cur = conn.cursor()
cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'mws' AND table_name = 'orders' ORDER BY ordinal_position")
print("=== COLUMNS ===")
for row in cur.fetchall():
    print(row[0], "-", row[1])
cur.execute("SELECT * FROM mws.orders LIMIT 3")
print("=== SAMPLE ===")
for row in cur.fetchall():
    print(row)
conn.close()