from fastapi import FastAPI
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

# ── Alerts: orders with problematic statuses ──────────────────────────────────
@app.get("/api/alerts")
def get_alerts():
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT
                id,
                amazon_order_id,
                order_status,
                item_status,
                seller_id,
                account_id,
                purchase_date,
                download_date,
                product_name,
                sku,
                asin,
                quantity,
                item_price,
                currency,
                ship_country
            FROM mws.orders
            WHERE order_status IN ('Pending', 'Canceled', 'Unshipped')
               OR item_status  IN ('Pending', 'Cancelled')
            ORDER BY purchase_date DESC
            LIMIT 100
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        result = []
        for r in rows:
            status = str(r["order_status"] or "").lower()
            severity = "critical" if status == "canceled" else "high" if status == "pending" else "medium"
            result.append({
                "id":               str(r["id"]),
                "amazon_order_id":  r["amazon_order_id"],
                "title":            f"{r['order_status']} order — {r['product_name'] or r['sku'] or r['asin']}",
                "source":           f"account-{r['account_id']}" if r["account_id"] else "mws",
                "table":            "mws.orders",
                "rule":             "ORD-STATUS",
                "severity":         severity,
                "status":           "open",
                "ts":               str(r["purchase_date"])[:16] if r["purchase_date"] else "",
                "aiSuggestion":     f"Order {r['amazon_order_id']} is {r['order_status']}. Check fulfillment pipeline for SKU {r['sku']}.",
                "canAutoFix":       False,
                "order_status":     r["order_status"],
                "item_status":      r["item_status"],
                "item_price":       float(r["item_price"]) if r["item_price"] else 0,
                "currency":         r["currency"],
                "ship_country":     r["ship_country"],
            })
        return result
    except Exception as e:
        return {"error": str(e)}

# ── KPIs: order counts and revenue summary ────────────────────────────────────
@app.get("/api/kpis")
def get_kpis():
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT
                COUNT(*)                                                AS total_orders,
                SUM(item_price)                                         AS total_revenue,
                SUM(CASE WHEN order_status = 'Shipped'   THEN 1 ELSE 0 END) AS shipped,
                SUM(CASE WHEN order_status = 'Pending'   THEN 1 ELSE 0 END) AS pending,
                SUM(CASE WHEN order_status = 'Canceled'  THEN 1 ELSE 0 END) AS canceled,
                SUM(CASE WHEN order_status = 'Unshipped' THEN 1 ELSE 0 END) AS unshipped,
                COUNT(DISTINCT account_id)                              AS accounts,
                COUNT(DISTINCT asin)                                    AS unique_asins
            FROM mws.orders
            WHERE download_date >= CURRENT_DATE - INTERVAL '30 days'
        """)
        row = cur.fetchone()
        cur.close()
        conn.close()
        return {
            "total_orders":   int(row["total_orders"]  or 0),
            "total_revenue":  float(row["total_revenue"] or 0),
            "shipped":        int(row["shipped"]   or 0),
            "pending":        int(row["pending"]   or 0),
            "canceled":       int(row["canceled"]  or 0),
            "unshipped":      int(row["unshipped"] or 0),
            "accounts":       int(row["accounts"]  or 0),
            "unique_asins":   int(row["unique_asins"] or 0),
        }
    except Exception as e:
        return {"error": str(e)}

# ── Daily trend: orders per day for last 30 days ──────────────────────────────
@app.get("/api/trend")
def get_trend():
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT
                download_date::date         AS day,
                COUNT(*)                    AS orders,
                SUM(item_price)             AS revenue,
                SUM(CASE WHEN order_status = 'Canceled' THEN 1 ELSE 0 END) AS canceled
            FROM mws.orders
            WHERE download_date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY download_date::date
            ORDER BY day ASC
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [
            {
                "day":      str(r["day"]),
                "orders":   int(r["orders"]   or 0),
                "revenue":  float(r["revenue"] or 0),
                "canceled": int(r["canceled"] or 0),
            }
            for r in rows
        ]
    except Exception as e:
        return {"error": str(e)}

# ── Top products by revenue ───────────────────────────────────────────────────
@app.get("/api/top-products")
def get_top_products():
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT
                asin,
                sku,
                product_name,
                COUNT(*)        AS order_count,
                SUM(item_price) AS revenue,
                SUM(quantity)   AS units_sold
            FROM mws.orders
            WHERE download_date >= CURRENT_DATE - INTERVAL '30 days'
              AND order_status = 'Shipped'
            GROUP BY asin, sku, product_name
            ORDER BY revenue DESC
            LIMIT 10
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [
            {
                "asin":        r["asin"],
                "sku":         r["sku"],
                "product_name": r["product_name"] or r["sku"] or r["asin"],
                "order_count": int(r["order_count"] or 0),
                "revenue":     float(r["revenue"]   or 0),
                "units_sold":  int(r["units_sold"]  or 0),
            }
            for r in rows
        ]
    except Exception as e:
        return {"error": str(e)}

# ── Datasource health check ───────────────────────────────────────────────────
@app.get("/api/datasources/health")
def datasource_health():
    import time
    start = time.time()
    try:
        conn = get_connection()
        conn.close()
        latency = int((time.time() - start) * 1000)
        return [{"name": "analytics-rs", "type": "Redshift", "status": "healthy", "latency": latency}]
    except Exception as e:
        return [{"name": "analytics-rs", "type": "Redshift", "status": "offline", "latency": None, "error": str(e)}]