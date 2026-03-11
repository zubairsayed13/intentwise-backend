from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import psycopg2.extras
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_connection():
    return psycopg2.connect(
        host=os.getenv("REDSHIFT_HOST"),
        port=int(os.getenv("REDSHIFT_PORT", 5439)),
        dbname=os.getenv("REDSHIFT_DB"),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD"),
        sslmode="require"
    )

@app.get("/health")
def health():
    try:
        conn = get_connection()
        conn.close()
        return {"status": "ok", "db": "connected"}
    except Exception as e:
        return {"status": "error", "db": str(e)}

@app.get("/api/tables")
def get_tables():
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT
                t.table_schema,
                t.table_name,
                COUNT(c.column_name) AS column_count
            FROM information_schema.tables t
            JOIN information_schema.columns c
              ON t.table_schema = c.table_schema
             AND t.table_name   = c.table_name
            WHERE t.table_type = 'BASE TABLE'
              AND t.table_schema NOT IN ('pg_catalog','information_schema','pg_internal')
            GROUP BY t.table_schema, t.table_name
            ORDER BY t.table_schema, t.table_name
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        schemas = {}
        for r in rows:
            s = r["table_schema"]
            if s not in schemas:
                schemas[s] = []
            schemas[s].append({
                "name":         r["table_name"],
                "column_count": int(r["column_count"])
            })
        return [{"schema": s, "tables": tables} for s, tables in schemas.items()]
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/preview")
def preview_table(schema: str = Query(...), table: str = Query(...), limit: int = 50):
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema, table))
        columns = [{"name": r["column_name"], "type": r["data_type"]} for r in cur.fetchall()]
        cur.execute("SELECT COUNT(*) as count FROM \"" + schema + "\".\"" + table + "\"")
        total_rows = cur.fetchone()["count"]
        cur.execute("SELECT * FROM \"" + schema + "\".\"" + table + "\" LIMIT " + str(limit))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {
            "schema":     schema,
            "table":      table,
            "total_rows": int(total_rows),
            "columns":    columns,
            "rows":       [dict(r) for r in rows]
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/query")
def run_query(sql: str = Query(...)):
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rows = cur.fetchmany(200)
        columns = [desc[0] for desc in cur.description] if cur.description else []
        cur.close()
        conn.close()
        return {
            "columns": columns,
            "rows":    [dict(r) for r in rows],
            "count":   len(rows)
        }
    except Exception as e:
        return {"error": str(e)}