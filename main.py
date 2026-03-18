from fastapi import FastAPI, Query
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import psycopg2, psycopg2.extras, os, httpx, json, asyncio
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

def get_connection():
    return psycopg2.connect(
        host=os.getenv("REDSHIFT_HOST"), port=int(os.getenv("REDSHIFT_PORT", 5439)),
        dbname=os.getenv("REDSHIFT_DB"), user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD"), sslmode="require"
    )

def q(conn, sql, params=None):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql, params or [])
    rows = cur.fetchall()
    cur.close()
    return [dict(r) for r in rows]

def account_filter(account_id):
    return "AND account_id = %s" if account_id and account_id != "all" else ""

def account_params(account_id):
    return [int(account_id)] if account_id and account_id != "all" else []

# ── Health ────────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    try:
        conn = get_connection(); conn.close()
        return {"status": "ok", "db": "connected"}
    except Exception as e:
        return {"status": "error", "db": str(e)}

# ── Accounts ──────────────────────────────────────────────────────────────────
@app.get("/api/accounts")
def get_accounts():
    try:
        conn = get_connection()
        rows = q(conn, """
            SELECT DISTINCT account_id, seller_id
            FROM mws.orders
            WHERE account_id IS NOT NULL
            ORDER BY account_id
        """)
        conn.close()
        return [{"account_id": r["account_id"], "seller_id": r["seller_id"] or str(r["account_id"])} for r in rows]
    except Exception as e:
        return {"error": str(e)}

# ── Tables ────────────────────────────────────────────────────────────────────
@app.get("/api/tables")
def get_tables():
    try:
        conn = get_connection()
        rows = q(conn, """
            SELECT t.table_schema, t.table_name, COUNT(c.column_name) AS column_count
            FROM information_schema.tables t
            JOIN information_schema.columns c ON t.table_schema=c.table_schema AND t.table_name=c.table_name
            WHERE t.table_type='BASE TABLE'
              AND t.table_schema NOT IN ('pg_catalog','information_schema','pg_internal')
            GROUP BY t.table_schema, t.table_name
            ORDER BY t.table_schema, t.table_name
        """)
        conn.close()
        return [{"table_schema": r["table_schema"], "table_name": r["table_name"], "column_count": int(r["column_count"])} for r in rows]
    except Exception as e:
        return {"error": str(e)}

# ── Preview ───────────────────────────────────────────────────────────────────
@app.get("/api/preview")
def preview_table(schema: str = Query(...), table: str = Query(...), limit: int = 50):
    try:
        conn = get_connection()
        columns = q(conn, "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position", (schema, table))
        count_row = q(conn, f'SELECT COUNT(*) as count FROM "{schema}"."{table}"')
        rows = q(conn, f'SELECT * FROM "{schema}"."{table}" LIMIT {limit}')
        conn.close()
        return {"schema": schema, "table": table, "total_rows": int(count_row[0]["count"]), "columns": columns, "rows": rows}
    except Exception as e:
        return {"error": str(e)}

# ── Custom SQL ────────────────────────────────────────────────────────────────
@app.get("/api/query")
def run_query(sql: str = Query(...)):
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rows = cur.fetchmany(200)
        columns = [desc[0] for desc in cur.description] if cur.description else []
        cur.close(); conn.close()
        return {"columns": columns, "rows": [dict(r) for r in rows], "count": len(rows)}
    except Exception as e:
        return {"error": str(e)}

# ── KPIs ──────────────────────────────────────────────────────────────────────
@app.get("/api/kpis")
def get_kpis(account_id: str = Query("all")):
    try:
        conn = get_connection()
        af = account_filter(account_id)
        ap = account_params(account_id)

        orders = q(conn, f"""
            SELECT
                COUNT(*)                                                          AS total_orders,
                SUM(item_price)                                                   AS total_revenue,
                SUM(CASE WHEN order_status='Shipped'   THEN 1 ELSE 0 END)        AS shipped,
                SUM(CASE WHEN order_status='Pending'   THEN 1 ELSE 0 END)        AS pending,
                SUM(CASE WHEN order_status='Canceled'  THEN 1 ELSE 0 END)        AS canceled,
                SUM(CASE WHEN order_status='Unshipped' THEN 1 ELSE 0 END)        AS unshipped,
                COUNT(DISTINCT account_id)                                        AS accounts,
                COUNT(DISTINCT asin)                                              AS unique_asins
            FROM mws.orders
            WHERE download_date = (SELECT MAX(download_date) FROM mws.orders) {af}
        """, ap)

        inv = q(conn, f"""
            SELECT
                COUNT(*)                                             AS total_skus,
                SUM(available)                                       AS total_available,
                SUM(CASE WHEN available <= 0 THEN 1 ELSE 0 END)     AS out_of_stock,
                SUM(CASE WHEN alert IS NOT NULL AND alert != '' THEN 1 ELSE 0 END) AS inventory_alerts
            FROM mws.inventory
            WHERE download_date = (SELECT MAX(download_date) FROM mws.inventory) {af}
        """, ap)

        sales = q(conn, f"""
            SELECT
                SUM(ordered_product_sales_amt)  AS total_sales,
                SUM(units_ordered)              AS total_units,
                SUM(sessions)                   AS total_sessions,
                AVG(buy_box_percentage)         AS avg_buy_box_pct,
                SUM(units_refunded)             AS total_refunds
            FROM mws.sales_and_traffic_by_date
            WHERE download_date = (SELECT MAX(download_date) FROM mws.sales_and_traffic_by_date) {af}
        """, ap)

        conn.close()
        o = orders[0] if orders else {}
        i = inv[0] if inv else {}
        s = sales[0] if sales else {}
        return {
            "orders":          {"total": int(o.get("total_orders") or 0), "shipped": int(o.get("shipped") or 0), "pending": int(o.get("pending") or 0), "canceled": int(o.get("canceled") or 0), "unshipped": int(o.get("unshipped") or 0), "revenue": float(o.get("total_revenue") or 0), "accounts": int(o.get("accounts") or 0), "unique_asins": int(o.get("unique_asins") or 0)},
            "inventory":       {"total_skus": int(i.get("total_skus") or 0), "available": int(i.get("total_available") or 0), "out_of_stock": int(i.get("out_of_stock") or 0), "alerts": int(i.get("inventory_alerts") or 0)},
            "sales":           {"total_sales": float(s.get("total_sales") or 0), "units": int(s.get("total_units") or 0), "sessions": int(s.get("total_sessions") or 0), "buy_box_pct": float(s.get("avg_buy_box_pct") or 0), "refunds": int(s.get("total_refunds") or 0)},
        }
    except Exception as e:
        return {"error": str(e)}

# ── Trend (last 30 days) ──────────────────────────────────────────────────────
@app.get("/api/trend")
def get_trend(account_id: str = Query("all")):
    try:
        conn = get_connection()
        af = account_filter(account_id)
        ap = account_params(account_id)
        rows = q(conn, f"""
            SELECT sale_date AS day,
                   SUM(ordered_product_sales_amt) AS revenue,
                   SUM(units_ordered)             AS units,
                   SUM(sessions)                  AS sessions
            FROM mws.sales_and_traffic_by_date
            WHERE sale_date >= CURRENT_DATE - 30 {af}
            GROUP BY sale_date ORDER BY day ASC
        """, ap)
        conn.close()
        return [{"day": str(r["day"]), "revenue": float(r["revenue"] or 0), "units": int(r["units"] or 0), "sessions": int(r["sessions"] or 0)} for r in rows]
    except Exception as e:
        return {"error": str(e)}

# ── Top ASINs ─────────────────────────────────────────────────────────────────
@app.get("/api/top-asins")
def get_top_asins(account_id: str = Query("all")):
    try:
        conn = get_connection()
        af = account_filter(account_id)
        ap = account_params(account_id)
        rows = q(conn, f"""
            SELECT child_asin AS asin,
                   SUM(units_ordered)            AS units,
                   SUM(ordered_product_sales_amt) AS revenue,
                   AVG(traffic_by_asin_buy_box_prcntg) AS buy_box_pct
            FROM mws.sales_and_traffic_by_asin
            WHERE download_date = (SELECT MAX(download_date) FROM mws.sales_and_traffic_by_asin) {af}
            GROUP BY child_asin
            ORDER BY revenue DESC NULLS LAST
            LIMIT 10
        """, ap)
        conn.close()
        return [{"asin": r["asin"], "units": int(r["units"] or 0), "revenue": float(r["revenue"] or 0), "buy_box_pct": float(r["buy_box_pct"] or 0)} for r in rows]
    except Exception as e:
        return {"error": str(e)}

# ── Inventory health ──────────────────────────────────────────────────────────
@app.get("/api/inventory")
def get_inventory(account_id: str = Query("all")):
    try:
        conn = get_connection()
        af = account_filter(account_id)
        ap = account_params(account_id)
        rows = q(conn, f"""
            SELECT asin, merchant_sku, product_name, available, total_units,
                   days_of_supply, alert, recommended_replenishment_qty, account_id
            FROM mws.inventory
            WHERE download_date = (SELECT MAX(download_date) FROM mws.inventory) {af}
            ORDER BY available ASC NULLS FIRST
            LIMIT 100
        """, ap)
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        return {"error": str(e)}

# ── Quality Rules (auto-generated from schema + data checks) ──────────────────
@app.get("/api/rules")
def get_rules(account_id: str = Query("all")):
    try:
        conn = get_connection()
        af = account_filter(account_id)
        ap = account_params(account_id)
        rules = []

        # Rule 1: NULL asin in orders
        r1 = q(conn, f"SELECT COUNT(*) as cnt FROM mws.orders WHERE asin IS NULL AND download_date=(SELECT MAX(download_date) FROM mws.orders) {af}", ap)
        cnt1 = int(r1[0]["cnt"] or 0)
        rules.append({"id":"RUL-001","name":"No NULL ASINs in orders","table":"mws.orders","column":"asin","check":"IS NOT NULL","lastResult":"pass" if cnt1==0 else "fail","failCount":cnt1,"severity":"high","source":"mws"})

        # Rule 2: Positive item_price
        r2 = q(conn, f"SELECT COUNT(*) as cnt FROM mws.orders WHERE item_price <= 0 AND order_status='Shipped' AND download_date=(SELECT MAX(download_date) FROM mws.orders) {af}", ap)
        cnt2 = int(r2[0]["cnt"] or 0)
        rules.append({"id":"RUL-002","name":"Shipped orders must have positive price","table":"mws.orders","column":"item_price","check":"> 0 where shipped","lastResult":"pass" if cnt2==0 else "fail","failCount":cnt2,"severity":"critical","source":"mws"})

        # Rule 3: No negative inventory
        r3 = q(conn, f"SELECT COUNT(*) as cnt FROM mws.inventory WHERE available < 0 AND download_date=(SELECT MAX(download_date) FROM mws.inventory) {af}", ap)
        cnt3 = int(r3[0]["cnt"] or 0)
        rules.append({"id":"RUL-003","name":"Available inventory must be >= 0","table":"mws.inventory","column":"available","check":">= 0","lastResult":"pass" if cnt3==0 else "fail","failCount":cnt3,"severity":"high","source":"mws"})

        # Rule 4: Sales data freshness
        r4 = q(conn, "SELECT MAX(sale_date) as latest FROM mws.sales_and_traffic_by_date")
        latest = r4[0]["latest"]
        from datetime import date
        stale = latest is None or (date.today() - latest).days > 3
        rules.append({"id":"RUL-004","name":"Sales data must be fresh (< 3 days old)","table":"mws.sales_and_traffic_by_date","column":"sale_date","check":"MAX(sale_date) >= today-3","lastResult":"fail" if stale else "pass","failCount":1 if stale else 0,"severity":"critical","source":"mws","detail":f"Latest: {latest}"})

        # Rule 5: Duplicate order IDs
        r5 = q(conn, f"SELECT COUNT(*) as cnt FROM (SELECT amazon_order_id FROM mws.orders WHERE download_date=(SELECT MAX(download_date) FROM mws.orders) {af} GROUP BY amazon_order_id HAVING COUNT(*)>1) x", ap)
        cnt5 = int(r5[0]["cnt"] or 0)
        rules.append({"id":"RUL-005","name":"No duplicate amazon_order_id","table":"mws.orders","column":"amazon_order_id","check":"UNIQUE","lastResult":"pass" if cnt5==0 else "fail","failCount":cnt5,"severity":"high","source":"mws"})

        # Rule 6: ASIN coverage — orders vs inventory
        r6 = q(conn, f"""
            SELECT COUNT(DISTINCT o.asin) as cnt FROM mws.orders o
            WHERE o.download_date=(SELECT MAX(download_date) FROM mws.orders)
              AND o.asin IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM mws.inventory i WHERE i.asin=o.asin {'AND i.account_id=%s' if account_id!='all' else ''})
              {af}
        """, (ap + ap) if account_id != "all" else ap)
        cnt6 = int(r6[0]["cnt"] or 0)
        rules.append({"id":"RUL-006","name":"All ordered ASINs must exist in inventory","table":"mws.orders+inventory","column":"asin","check":"EXISTS in inventory","lastResult":"pass" if cnt6==0 else "fail","failCount":cnt6,"severity":"medium","source":"mws"})

        # Rule 7: Units ordered > 0 for shipped
        r7 = q(conn, f"SELECT COUNT(*) as cnt FROM mws.orders WHERE quantity<=0 AND order_status='Shipped' AND download_date=(SELECT MAX(download_date) FROM mws.orders) {af}", ap)
        cnt7 = int(r7[0]["cnt"] or 0)
        rules.append({"id":"RUL-007","name":"Shipped orders must have quantity > 0","table":"mws.orders","column":"quantity","check":"> 0 where shipped","lastResult":"pass" if cnt7==0 else "fail","failCount":cnt7,"severity":"medium","source":"mws"})

        # Rule 8: Buy box percentage in valid range
        r8 = q(conn, f"""
            SELECT COUNT(*) as cnt FROM mws.sales_and_traffic_by_asin
            WHERE (traffic_by_asin_buy_box_prcntg < 0 OR traffic_by_asin_buy_box_prcntg > 1)
              AND download_date=(SELECT MAX(download_date) FROM mws.sales_and_traffic_by_asin) {af}
        """, ap)
        cnt8 = int(r8[0]["cnt"] or 0)
        rules.append({"id":"RUL-008","name":"Buy box % must be between 0 and 1","table":"mws.sales_and_traffic_by_asin","column":"traffic_by_asin_buy_box_prcntg","check":"BETWEEN 0 AND 1","lastResult":"pass" if cnt8==0 else "fail","failCount":cnt8,"severity":"low","source":"mws"})

        conn.close()
        return rules
    except Exception as e:
        return {"error": str(e)}

# ── Alert Detection ───────────────────────────────────────────────────────────
@app.get("/api/alerts/detect")
def detect_alerts(account_id: str = Query("all")):
    alerts = []
    try:
        conn = get_connection()
        af = account_filter(account_id)
        ap = account_params(account_id)

        # 1. Missing sales days
        missing = q(conn, f"""
            SELECT gs::date AS missing_date
            FROM generate_series(CURRENT_DATE-30, CURRENT_DATE-1, '1 day'::interval) gs
            WHERE gs::date NOT IN (
                SELECT DISTINCT sale_date FROM mws.sales_and_traffic_by_date
                WHERE sale_date >= CURRENT_DATE-30 {af}
            )
            ORDER BY missing_date DESC
        """, ap)
        if missing:
            dates = ", ".join(str(r["missing_date"]) for r in missing[:5])
            alerts.append({"id":"AGT-001","severity":"high","status":"open","title":f"Sales data missing for {len(missing)} day(s)","source":"mws.sales_and_traffic_by_date","table":"sales_and_traffic_by_date","rule":"DATA-FRESHNESS-001","ts":str(missing[0]["missing_date"]),"aiSuggestion":f"No data for: {dates}. Check if download pipeline ran.","canAutoFix":False,"details":missing[:10]})

        # 2. Duplicate order IDs
        dupes = q(conn, f"""
            SELECT amazon_order_id, COUNT(*) as cnt FROM mws.orders
            WHERE download_date>=(SELECT MAX(download_date)-7 FROM mws.orders) {af}
            GROUP BY amazon_order_id HAVING COUNT(*)>1 ORDER BY cnt DESC LIMIT 20
        """, ap)
        if dupes:
            alerts.append({"id":"AGT-002","severity":"high" if len(dupes)>10 else "medium","status":"open","title":f"{len(dupes)} duplicate amazon_order_id(s)","source":"mws.orders","table":"orders","rule":"DUPE-001","ts":"last 7 days","aiSuggestion":f"Top duplicate: {dupes[0]['amazon_order_id']} ({dupes[0]['cnt']} rows). Run dedup before analysis.","canAutoFix":False,"details":dupes})

        # 3. Negative inventory
        bad_inv = q(conn, f"""
            SELECT asin, merchant_sku, available, total_units, account_id FROM mws.inventory
            WHERE (available<0 OR total_units<0)
              AND download_date=(SELECT MAX(download_date) FROM mws.inventory) {af}
            LIMIT 50
        """, ap)
        if bad_inv:
            alerts.append({"id":"AGT-003","severity":"critical","status":"open","title":f"{len(bad_inv)} ASIN(s) with negative inventory","source":"mws.inventory","table":"inventory","rule":"INV-NEG-001","ts":"latest snapshot","aiSuggestion":f"{len(bad_inv)} ASINs have negative inventory. Likely ingestion error or unreconciled Amazon adjustment.","canAutoFix":False,"details":bad_inv})

        # 4. ASINs in orders not in inventory
        ghost = q(conn, f"""
            SELECT DISTINCT o.asin, o.account_id FROM mws.orders o
            WHERE o.download_date>=(SELECT MAX(download_date)-7 FROM mws.orders)
              AND o.asin IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM mws.inventory i WHERE i.asin=o.asin {'AND i.account_id=%s' if account_id!='all' else ''})
              {af}
            LIMIT 50
        """, (ap + ap) if account_id != "all" else ap)
        if ghost:
            alerts.append({"id":"AGT-004","severity":"medium","status":"open","title":f"{len(ghost)} ASIN(s) in orders with no inventory record","source":"mws.orders+inventory","table":"orders","rule":"ASIN-ORPHAN-001","ts":"last 7 days","aiSuggestion":f"{len(ghost)} ASINs in recent orders missing from inventory. New products or sync gap.","canAutoFix":False,"details":ghost})

        # 5. Revenue drop
        rev = q(conn, f"""
            SELECT
                AVG(CASE WHEN sale_date=CURRENT_DATE-1 THEN ordered_product_sales_amt END) AS yesterday,
                AVG(CASE WHEN sale_date BETWEEN CURRENT_DATE-8 AND CURRENT_DATE-2 THEN ordered_product_sales_amt END) AS avg7d
            FROM mws.sales_and_traffic_by_date WHERE sale_date>=CURRENT_DATE-8 {af}
        """, ap)
        if rev and rev[0]["yesterday"] and rev[0]["avg7d"]:
            y=float(rev[0]["yesterday"]); a=float(rev[0]["avg7d"])
            pct=round((y-a)/a*100,1) if a else 0
            if pct < -20:
                alerts.append({"id":"AGT-005","severity":"high" if pct<-30 else "medium","status":"open","title":f"Revenue dropped {abs(pct)}% vs 7-day average","source":"mws.sales_and_traffic_by_date","table":"sales_and_traffic_by_date","rule":"REV-DROP-001","ts":"yesterday","aiSuggestion":f"Yesterday ${y:,.0f} vs 7d avg ${a:,.0f} ({abs(pct)}% drop). Check pipeline and promotions.","canAutoFix":False,"details":[{"yesterday":y,"avg_7d":a,"pct_change":pct}]})

        conn.close()
    except Exception as e:
        alerts.append({"id":"AGT-ERR","severity":"critical","status":"open","title":"Alert detection failed","source":"backend","table":"","rule":"SYS-001","ts":"","aiSuggestion":str(e),"canAutoFix":False,"details":[]})
    return alerts

# ── AI Agent analyze ──────────────────────────────────────────────────────────
@app.post("/api/agents/analyze")
def ai_analyze(payload: dict):
    """
    Analyze a table for data quality issues.
    agent_type: "full" | "nulls" | "duplicates" | "freshness" | "range" | "schema"
    Returns { alerts: [], summary, quality_score }
    """
    table      = payload.get("table", "")
    schema     = payload.get("schema", "mws")
    account_id = payload.get("account_id")
    agent_type = payload.get("agent_type", "full")

    if not table:
        return {"error": "table is required"}

    full_table = f"{schema}.{table}"
    alerts = []

    try:
        conn = get_connection()

        # ── Check table exists ───────────────────────────────────────────────
        exists = q(conn, """
            SELECT COUNT(*) AS cnt FROM information_schema.tables
            WHERE table_schema=%s AND table_name=%s
        """, [schema, table])
        if not exists or exists[0]["cnt"] == 0:
            conn.close()
            return {"alerts": [], "summary": f"Table {full_table} not found", "quality_score": None}

        # ── Get columns ──────────────────────────────────────────────────────
        cols = q(conn, """
            SELECT column_name, data_type FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position
        """, [schema, table])
        col_names   = [c["column_name"] for c in cols]
        date_cols   = [c["column_name"] for c in cols if "date" in c["data_type"] or "timestamp" in c["data_type"]]
        num_cols    = [c["column_name"] for c in cols if c["data_type"] in ("integer","bigint","numeric","double precision","real","float")]
        id_cols     = [c for c in col_names if c.endswith("_id") or c in ("id","amazon_order_id","asin","fnsku","merchant_sku")]

        total = q(conn, f"SELECT COUNT(*) AS cnt FROM {full_table}")[0]["cnt"]
        if total == 0:
            conn.close()
            return {"alerts": [], "summary": f"{full_table} is empty", "quality_score": 100, "total_rows": 0}

        # ── NULL checks ──────────────────────────────────────────────────────
        if agent_type in ("full", "nulls"):
            for col in id_cols[:6]:
                try:
                    null_count = q(conn, f"SELECT COUNT(*) AS cnt FROM {full_table} WHERE {col} IS NULL")[0]["cnt"]
                    if null_count > 0:
                        alerts.append({
                            "id":       f"AGT-{schema[:3].upper()}-NULL-{col}",
                            "title":    f"NULL {col} in {full_table}",
                            "severity": "high" if null_count / max(total,1) > 0.05 else "medium",
                            "table":    full_table,
                            "source":   "redshift-staging",
                            "status":   "open",
                            "ts":       __import__("datetime").datetime.now().strftime("%I:%M %p"),
                            "message":  f"{null_count:,} of {total:,} rows ({null_count*100//max(total,1)}%) have NULL {col}",
                            "count":    int(null_count),
                        })
                except: pass

        # ── Duplicate checks ─────────────────────────────────────────────────
        if agent_type in ("full", "duplicates"):
            pk_candidates = [c for c in id_cols if c != "id"][:2]
            for col in pk_candidates:
                try:
                    dupe_count = q(conn, f"""
                        SELECT COUNT(*) AS cnt FROM (
                            SELECT {col} FROM {full_table}
                            GROUP BY {col} HAVING COUNT(*) > 1
                        ) d
                    """)[0]["cnt"]
                    if dupe_count > 0:
                        alerts.append({
                            "id":       f"AGT-{schema[:3].upper()}-DUPE-{col}",
                            "title":    f"Duplicate {col} in {full_table}",
                            "severity": "critical",
                            "table":    full_table,
                            "source":   "redshift-staging",
                            "status":   "open",
                            "ts":       __import__("datetime").datetime.now().strftime("%I:%M %p"),
                            "message":  f"{dupe_count:,} duplicate values of {col} found",
                            "count":    int(dupe_count),
                        })
                except: pass

        # ── Freshness checks ─────────────────────────────────────────────────
        if agent_type in ("full", "freshness") and date_cols:
            col = date_cols[0]
            try:
                latest = q(conn, f"SELECT MAX({col}) AS mx FROM {full_table}")[0]["mx"]
                if latest:
                    import datetime
                    age_days = (datetime.date.today() - (latest.date() if hasattr(latest,"date") else latest)).days
                    if age_days > 2:
                        alerts.append({
                            "id":       f"AGT-{schema[:3].upper()}-FRESH-{col}",
                            "title":    f"Stale data in {full_table}",
                            "severity": "high" if age_days > 7 else "medium",
                            "table":    full_table,
                            "source":   "redshift-staging",
                            "status":   "open",
                            "ts":       __import__("datetime").datetime.now().strftime("%I:%M %p"),
                            "message":  f"Latest {col} is {age_days} days old",
                            "count":    age_days,
                        })
            except: pass

        # ── Range checks ─────────────────────────────────────────────────────
        if agent_type in ("full", "range") and num_cols:
            for col in num_cols[:3]:
                try:
                    neg = q(conn, f"SELECT COUNT(*) AS cnt FROM {full_table} WHERE {col} < 0")[0]["cnt"]
                    if neg > 0:
                        alerts.append({
                            "id":       f"AGT-{schema[:3].upper()}-RANGE-{col}",
                            "title":    f"Negative {col} in {full_table}",
                            "severity": "medium",
                            "table":    full_table,
                            "source":   "redshift-staging",
                            "status":   "open",
                            "ts":       __import__("datetime").datetime.now().strftime("%I:%M %p"),
                            "message":  f"{neg:,} rows have negative {col}",
                            "count":    int(neg),
                        })
                except: pass

        conn.close()
        score = max(0, 100 - len(alerts) * 15)
        return {
            "alerts":        alerts,
            "total_rows":    int(total),
            "quality_score": score,
            "agent_type":    agent_type,
            "summary":       f"{len(alerts)} issue(s) found in {full_table} ({total:,} rows)",
            "columns":       col_names,
        }

    except Exception as e:
        return {"error": str(e), "alerts": []}

# ── Full scan ─────────────────────────────────────────────────────────────────
@app.post("/api/agents/full-scan")
async def full_scan(account_id: str = Query("all")):
    alerts=detect_alerts(account_id)
    if not alerts:
        return {"alerts":[],"analysis":{"summary":"No issues detected.","quality_score":98,"test_cases":[],"root_causes":[],"recommendations":["Continue monitoring"]}}
    analysis=await ai_analyze({"table":"orders + inventory + sales_and_traffic","schema":"mws","findings":alerts})
    return {"alerts":alerts,"analysis":analysis}


class ChatRequest(BaseModel):
    messages: list
    system: str = ""
    max_tokens: int = 1000

@app.post("/api/ai/chat")
async def ai_chat(req: ChatRequest):
    from fastapi.responses import JSONResponse
    api_key = os.environ.get("OPENAI_API_KEY","")
    if not api_key:
        return JSONResponse(status_code=500, content={"error": "OPENAI_API_KEY not set in Railway"})
    # Convert system prompt into OpenAI messages format
    messages = []
    if req.system:
        messages.append({"role": "system", "content": req.system})
    messages.extend(req.messages)
    async with httpx.AsyncClient(timeout=60) as client:
        payload = {
            "model": "gpt-4o",
            "max_tokens": req.max_tokens,
            "messages": messages,
        }
        r = await client.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json=payload
        )
        body = r.json()
        if r.status_code != 200:
            return JSONResponse(status_code=r.status_code, content=body)
        # Normalise to Anthropic-style response so frontend doesn't need changes
        text = body.get("choices", [{}])[0].get("message", {}).get("content", "")
        return {"content": [{"type": "text", "text": text}]}

@app.get("/api/ai/test")
async def ai_test():
    api_key = os.environ.get("OPENAI_API_KEY","")
    if not api_key:
        return {"status": "error", "reason": "OPENAI_API_KEY env var not set"}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json={"model":"gpt-4o","max_tokens":5,"messages":[{"role":"user","content":"hi"}]}
            )
        return {"status": "ok" if r.status_code==200 else "error", "http_status": r.status_code}
    except Exception as e:
        return {"status": "error", "reason": str(e)}

# ── Remediation ───────────────────────────────────────────────────────────────

class RemediationFixRequest(BaseModel):
    fix_type: str          # "delete_dupes" | "flag_nulls" | "delete_stale"
    dry_run: bool = True   # True = count only, False = execute

@app.get("/api/remediation/scan")
def remediation_scan():
    """Scan mws.orders for: NULL asin, duplicate amazon_order_id, stale download_date (>30 days)."""
    try:
        conn = get_connection()
        null_asin = q(conn, "SELECT COUNT(*) AS cnt FROM mws.orders WHERE asin IS NULL")[0]["cnt"]
        dupe_orders = q(conn, """
            SELECT COUNT(*) AS cnt FROM (
                SELECT amazon_order_id FROM mws.orders
                GROUP BY amazon_order_id HAVING COUNT(*) > 1
            ) d
        """)[0]["cnt"]
        stale_rows = q(conn, """
            SELECT COUNT(*) AS cnt FROM mws.orders
            WHERE download_date < CURRENT_DATE - INTERVAL '30 days'
        """)[0]["cnt"]
        total_rows = q(conn, "SELECT COUNT(*) AS cnt FROM mws.orders")[0]["cnt"]
        conn.close()
        return {
            "table": "mws.orders",
            "total_rows": int(total_rows),
            "issues": [
                {
                    "id": "null_asin",
                    "title": "NULL asin on orders",
                    "severity": "high",
                    "count": int(null_asin),
                    "fix_sql": "UPDATE mws.orders SET asin = \'UNKNOWN\' WHERE asin IS NULL",
                    "fix_type": "flag_nulls",
                    "description": f"{null_asin} orders have no ASIN — cannot match to product catalog"
                },
                {
                    "id": "dupe_orders",
                    "title": "Duplicate amazon_order_id",
                    "severity": "critical",
                    "count": int(dupe_orders),
                    "fix_sql": "DELETE FROM mws.orders WHERE id NOT IN (SELECT MIN(id) FROM mws.orders GROUP BY amazon_order_id)",
                    "fix_type": "delete_dupes",
                    "description": f"{dupe_orders} duplicate order IDs detected — downstream metrics will be double-counted"
                },
                {
                    "id": "stale_rows",
                    "title": "Stale rows (>30 days old)",
                    "severity": "medium",
                    "count": int(stale_rows),
                    "fix_sql": "DELETE FROM mws.orders WHERE download_date < CURRENT_DATE - INTERVAL \'30 days\'",
                    "fix_type": "delete_stale",
                    "description": f"{stale_rows} rows with download_date older than 30 days"
                }
            ]
        }
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/remediation/fix")
def remediation_fix(req: RemediationFixRequest):
    """Execute a fix on mws.orders. dry_run=True counts affected rows only."""
    try:
        conn = get_connection()
        fix_map = {
            "flag_nulls":    ("SELECT COUNT(*) AS cnt FROM mws.orders WHERE asin IS NULL",
                              "UPDATE mws.orders SET asin = \'UNKNOWN\' WHERE asin IS NULL"),
            "delete_dupes":  ("SELECT COUNT(*) AS cnt FROM mws.orders WHERE id NOT IN (SELECT MIN(id) FROM mws.orders GROUP BY amazon_order_id)",
                              "DELETE FROM mws.orders WHERE id NOT IN (SELECT MIN(id) FROM mws.orders GROUP BY amazon_order_id)"),
            "delete_stale":  ("SELECT COUNT(*) AS cnt FROM mws.orders WHERE download_date < CURRENT_DATE - INTERVAL \'30 days\'",
                              "DELETE FROM mws.orders WHERE download_date < CURRENT_DATE - INTERVAL \'30 days\'"),
        }
        if req.fix_type not in fix_map:
            return {"error": f"Unknown fix_type: {req.fix_type}"}

        count_sql, fix_sql = fix_map[req.fix_type]
        before = q(conn, "SELECT COUNT(*) AS cnt FROM mws.orders")[0]["cnt"]
        affected = q(conn, count_sql)[0]["cnt"]

        if req.dry_run:
            conn.close()
            return {"dry_run": True, "fix_type": req.fix_type, "rows_affected": int(affected), "before": int(before)}

        cur = conn.cursor()
        cur.execute(fix_sql)
        conn.commit()
        after = q(conn, "SELECT COUNT(*) AS cnt FROM mws.orders")[0]["cnt"]
        conn.close()
        return {
            "dry_run": False,
            "fix_type": req.fix_type,
            "rows_affected": int(affected),
            "before": int(before),
            "after": int(after),
            "success": True
        }
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/remediation/notify")
async def remediation_notify(payload: dict):
    """Send a Slack notification for a remediation result."""
    slack_url = os.getenv("SLACK_WEBHOOK_URL", "")
    if not slack_url:
        return {"error": "SLACK_WEBHOOK_URL not set in environment"}
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(slack_url, json={
                "text": payload.get("message", "Remediation complete"),
                "blocks": [
                    {"type": "section", "text": {"type": "mrkdwn",
                        "text": f"*🔧 Remediation Complete — mws.orders*\n{payload.get('message','')}"}},
                    {"type": "context", "elements": [
                        {"type": "mrkdwn", "text": f"Fix: `{payload.get('fix_type','')}` · Rows affected: *{payload.get('rows_affected',0)}* · Before: {payload.get('before',0)} → After: {payload.get('after',0)}"}
                    ]}
                ]
            }, timeout=10)
        return {"sent": resp.status_code == 200, "status": resp.status_code}
    except Exception as e:
        return {"error": str(e)}
# ── Flow Builder — Save / Load / Schedule ─────────────────────────────────────
# Flows stored in memory (replace with DB in production)
_SAVED_FLOWS = {}

class FlowSaveRequest(BaseModel):
    flow_id:     str
    name:        str
    description: str = ""
    steps:       list
    schedule:    dict = {}   # { enabled, cron, timezone }

@app.post("/api/flows/save")
def save_flow(req: FlowSaveRequest):
    import datetime
    _SAVED_FLOWS[req.flow_id] = {
        "flow_id":     req.flow_id,
        "name":        req.name,
        "description": req.description,
        "steps":       req.steps,
        "schedule":    req.schedule,
        "saved_at":    datetime.datetime.now().isoformat(),
        "last_run":    None,
        "run_count":   0,
    }
    return {"saved": True, "flow_id": req.flow_id}

@app.get("/api/flows")
def list_flows():
    return list(_SAVED_FLOWS.values())

@app.get("/api/flows/{flow_id}")
def get_flow(flow_id: str):
    if flow_id not in _SAVED_FLOWS:
        return {"error": "Flow not found"}
    return _SAVED_FLOWS[flow_id]

@app.post("/api/flows/{flow_id}/run")
def run_flow(flow_id: str, payload: dict = {}):
    """
    Execute a saved flow step by step.
    Returns per-step results. SELECT steps run immediately.
    FIX steps return requires_approval=True and do NOT execute unless payload.approved=True.
    """
    if flow_id not in _SAVED_FLOWS:
        return {"error": "Flow not found"}

    flow     = _SAVED_FLOWS[flow_id]
    steps    = flow["steps"]
    approved = payload.get("approved_steps", [])  # list of step ids approved by human
    results  = []

    try:
        conn = get_connection()
        for step in steps:
            sid   = step.get("id")
            stype = step.get("type")   # "select" | "fix" | "notify" | "condition"
            sql   = step.get("sql", "").strip()
            label = step.get("label", sid)

            if stype == "select":
                try:
                    rows = q(conn, sql)
                    results.append({ "step_id": sid, "label": label, "type": stype,
                        "status": "done", "rows": rows[:50], "row_count": len(rows) })
                except Exception as e:
                    results.append({ "step_id": sid, "label": label, "type": stype,
                        "status": "error", "error": str(e) })

            elif stype == "fix":
                if sid not in approved:
                    # Preview only — count affected rows
                    count_sql = step.get("count_sql", f"SELECT COUNT(*) AS cnt FROM ({sql}) x")
                    try:
                        count = q(conn, count_sql)[0].get("cnt", 0)
                    except:
                        count = "unknown"
                    results.append({ "step_id": sid, "label": label, "type": stype,
                        "status": "awaiting_approval", "requires_approval": True,
                        "preview_count": count, "sql": sql })
                else:
                    # Human approved — execute
                    try:
                        cur = conn.cursor()
                        cur.execute(sql)
                        conn.commit()
                        before = payload.get("before_counts", {}).get(sid, 0)
                        results.append({ "step_id": sid, "label": label, "type": stype,
                            "status": "done", "executed": True, "rows_affected": cur.rowcount })
                    except Exception as e:
                        results.append({ "step_id": sid, "label": label, "type": stype,
                            "status": "error", "error": str(e) })

            elif stype == "condition":
                # Evaluate a SELECT that returns a single boolean-ish value
                try:
                    rows = q(conn, sql)
                    val  = list(rows[0].values())[0] if rows else 0
                    passed = bool(val) if not isinstance(val, (int,float)) else val > 0
                    results.append({ "step_id": sid, "label": label, "type": stype,
                        "status": "done", "passed": passed, "value": str(val) })
                except Exception as e:
                    results.append({ "step_id": sid, "label": label, "type": stype,
                        "status": "error", "error": str(e) })

            elif stype == "notify":
                results.append({ "step_id": sid, "label": label, "type": stype,
                    "status": "done", "message": step.get("message", "Flow step completed") })

        conn.close()
        import datetime
        _SAVED_FLOWS[flow_id]["last_run"]  = datetime.datetime.now().isoformat()
        _SAVED_FLOWS[flow_id]["run_count"] += 1
        return { "flow_id": flow_id, "status": "complete", "steps": results }

    except Exception as e:
        return {"error": str(e), "steps": results}


# ── Report Status Auto-Triage ─────────────────────────────────────────────────
# Schema: mws.report
# Columns: request_id, report_id, report_type, period_start_date, period_end_date,
#          requested_date, download_date, report_processing_time, tries, status,
#          account, period_start_time, period_end_time, precheck_status, copy_status
# status:      pending | processed | failed
# copy_status: REPLICATED | NOT_REPLICATED | NULL

@app.get("/api/report/triage")
def report_triage(account: str = None):
    """
    Scan mws.report for 3 issues:
      RPT-001 — Failed/pending downloads   (status != 'processed')
      RPT-002 — Not replicated             (copy_status != 'REPLICATED' or NULL)
      RPT-003 — Stuck reports              (processed but not replicated for >2h)
    """
    try:
        conn = get_connection()

        # account filter — column is called 'account' not 'account_id'
        af = "AND account = %s" if account and account != "all" else ""
        ap = [account] if account and account != "all" else []

        base_where = f"WHERE 1=1 {af}"

        total = q(conn, f"SELECT COUNT(*) AS cnt FROM mws.report {base_where}", ap)[0]["cnt"]
        issues = []

        # ── RPT-001: Failed / still-pending downloads ─────────────────────────
        failed_rows = q(conn, f"""
            SELECT status, COUNT(*) AS cnt
            FROM mws.report {base_where}
            AND status != 'processed'
            GROUP BY status ORDER BY cnt DESC
        """, ap)
        failed_count = sum(r["cnt"] for r in failed_rows)
        if failed_count:
            samples = q(conn, f"""
                SELECT request_id, report_type, status, copy_status,
                       download_date, tries, account
                FROM mws.report {base_where}
                AND status != 'processed'
                ORDER BY requested_date DESC LIMIT 5
            """, ap)
            issues.append({
                "id":          "RPT-001",
                "title":       "Failed / pending downloads",
                "severity":    "critical" if failed_count > 20 else "high",
                "count":       int(failed_count),
                "breakdown":   [{"status": r["status"], "count": int(r["cnt"])} for r in failed_rows],
                "samples":     [dict(r) for r in samples],
                "fix_action":  "redrive",
                "description": f"{failed_count} reports with status != 'processed'",
            })

        # ── RPT-002: Not replicated to Redshift ──────────────────────────────
        not_rep = q(conn, f"""
            SELECT COUNT(*) AS cnt FROM mws.report {base_where}
            AND (copy_status != 'REPLICATED' OR copy_status IS NULL)
        """, ap)[0]["cnt"]
        if not_rep:
            samples = q(conn, f"""
                SELECT request_id, report_type, status, copy_status,
                       download_date, tries, account
                FROM mws.report {base_where}
                AND (copy_status != 'REPLICATED' OR copy_status IS NULL)
                ORDER BY download_date DESC LIMIT 5
            """, ap)
            breakdown_rows = q(conn, f"""
                SELECT COALESCE(copy_status, 'NULL') AS copy_status, COUNT(*) AS cnt
                FROM mws.report {base_where}
                AND (copy_status != 'REPLICATED' OR copy_status IS NULL)
                GROUP BY 1 ORDER BY cnt DESC
            """, ap)
            issues.append({
                "id":          "RPT-002",
                "title":       "Reports not replicated to Redshift",
                "severity":    "high",
                "count":       int(not_rep),
                "breakdown":   [{"status": r["copy_status"], "count": int(r["cnt"])} for r in breakdown_rows],
                "samples":     [dict(r) for r in samples],
                "fix_action":  "recopy",
                "description": f"{not_rep} reports with copy_status != 'REPLICATED' (or NULL)",
            })

        # ── RPT-003: Stuck — processed but not replicated for >2h ────────────
        stuck = q(conn, f"""
            SELECT COUNT(*) AS cnt FROM mws.report {base_where}
            AND status = 'processed'
            AND (copy_status IS NULL OR copy_status = 'NOT_REPLICATED')
            AND download_date < CURRENT_TIMESTAMP - INTERVAL '2 hours'
        """, ap)[0]["cnt"]
        if stuck:
            samples = q(conn, f"""
                SELECT request_id, report_type, status, copy_status,
                       download_date, tries, account
                FROM mws.report {base_where}
                AND status = 'processed'
                AND (copy_status IS NULL OR copy_status = 'NOT_REPLICATED')
                AND download_date < CURRENT_TIMESTAMP - INTERVAL '2 hours'
                ORDER BY download_date ASC LIMIT 5
            """, ap)
            issues.append({
                "id":          "RPT-003",
                "title":       "Stuck — processed but not replicated >2h",
                "severity":    "high",
                "count":       int(stuck),
                "breakdown":   [],
                "samples":     [dict(r) for r in samples],
                "fix_action":  "redrive_copy",
                "description": f"{stuck} reports downloaded but copy stuck >2h",
            })

        conn.close()
        import datetime
        return {
            "table":       "mws.report",
            "total_rows":  int(total),
            "scanned_at":  datetime.datetime.now().isoformat(),
            "issues":      issues,
            "clean":       len(issues) == 0,
        }
    except Exception as e:
        return {"error": str(e), "issues": []}


@app.post("/api/report/fix")
def report_fix(payload: dict):
    """
    Execute a fix on mws.report.
    fix_action: redrive | recopy | redrive_copy
    dry_run: bool (default True)
    account: optional account filter
    """
    fix_action = payload.get("fix_action", "")
    dry_run    = payload.get("dry_run", True)
    account    = payload.get("account")
    af         = "AND account = %s" if account and account != "all" else ""
    ap         = [account] if account and account != "all" else []
    base_where = f"WHERE 1=1 {af}"

    FIX_MAP = {
        "redrive": (
            f"SELECT COUNT(*) AS cnt FROM mws.report {base_where} AND status != 'processed'",
            f"UPDATE mws.report SET status = 'pending', tries = 0 {base_where} AND status = 'failed'"
        ),
        "recopy": (
            f"SELECT COUNT(*) AS cnt FROM mws.report {base_where} AND (copy_status != 'REPLICATED' OR copy_status IS NULL)",
            f"UPDATE mws.report SET copy_status = 'NOT_REPLICATED' {base_where} AND status = 'processed' AND (copy_status IS NULL)"
        ),
        "redrive_copy": (
            f"SELECT COUNT(*) AS cnt FROM mws.report {base_where} AND status='processed' AND (copy_status IS NULL OR copy_status='NOT_REPLICATED') AND download_date < CURRENT_TIMESTAMP - INTERVAL '2 hours'",
            f"UPDATE mws.report SET copy_status = 'NOT_REPLICATED' {base_where} AND status='processed' AND copy_status IS NULL AND download_date < CURRENT_TIMESTAMP - INTERVAL '2 hours'"
        ),
    }

    if fix_action not in FIX_MAP:
        return {"error": f"Unknown fix_action: {fix_action}"}

    try:
        conn = get_connection()
        count_sql, fix_sql = FIX_MAP[fix_action]
        before   = q(conn, f"SELECT COUNT(*) AS cnt FROM mws.report {base_where}", ap)[0]["cnt"]
        affected = q(conn, count_sql, ap)[0]["cnt"]

        if dry_run:
            conn.close()
            return {"dry_run": True, "fix_action": fix_action,
                    "rows_affected": int(affected), "before": int(before)}

        cur = conn.cursor()
        cur.execute(fix_sql, ap)
        conn.commit()
        after = q(conn, f"SELECT COUNT(*) AS cnt FROM mws.report {base_where}", ap)[0]["cnt"]
        conn.close()
        return {"dry_run": False, "fix_action": fix_action,
                "rows_affected": int(affected), "before": int(before),
                "after": int(after), "success": True}
    except Exception as e:
        return {"error": str(e)}



# ═══════════════════════════════════════════════════════════════════════════════
# WiziAgent — Autonomous Data Quality Agent (powered by LangGraph)
# ═══════════════════════════════════════════════════════════════════════════════
#
# Graph:
#   scan → classify → [high_risk? → request_approval → awaiting] OR auto_fix
#        → verify_loop (up to 3 attempts, re-fixes on each attempt)
#        → notify → END
#
# Approval flow:
#   High-risk fixes (count >= threshold) post a Slack message and pause.
#   Call POST /api/wizi-agent/approve { token, decision:"approve"|"reject" }
#   to resume. Frontend polls /api/wizi-agent/status/{token}.
#
# Railway env vars required:
#   OPENAI_API_KEY     — already set
#   SLACK_WEBHOOK_URL  — optional, for approval requests + notifications

try:
    from typing import TypedDict, Annotated, List, Optional
    import operator, datetime, uuid, threading

    from langgraph.graph import StateGraph, END as LG_END
    from langchain_openai import ChatOpenAI
    from langchain_core.messages import HumanMessage, SystemMessage

    LANGGRAPH_AVAILABLE = True
except ImportError:
    LANGGRAPH_AVAILABLE = False

# ── In-memory approval store (survives for duration of Railway process) ────────
# { token: { "decision": None|"approve"|"reject", "event": threading.Event } }
_approval_store: dict = {}

if LANGGRAPH_AVAILABLE:

    # ── State ──────────────────────────────────────────────────────────────────
    class ReportTriageState(TypedDict):
        account:          Optional[str]
        # scan
        total_rows:       int
        issues:           List[dict]
        # classify
        classification:   Optional[dict]
        # approval gate
        needs_approval:   bool
        approval_token:   Optional[str]
        approval_status:  str           # "pending"|"approved"|"rejected"|"skipped"
        threshold:        int
        # fix
        fix_results:      Annotated[List[dict], operator.add]
        # verify loop
        verify_attempts:  int
        verify_issues:    List[dict]
        # notify
        notified:         bool
        # trace
        trace:            Annotated[List[dict], operator.add]
        # final
        status:           str
        error:            Optional[str]

    def _t(node, msg, level="info"):
        return {"node": node,
                "ts":   datetime.datetime.now().strftime("%H:%M:%S"),
                "msg":  msg, "level": level}

    def _scan_report(account=None):
        conn = get_connection()
        af   = "AND account = %s" if account and account != "all" else ""
        ap   = [account] if account and account != "all" else []
        bw   = f"WHERE 1=1 {af}"

        total = q(conn, f"SELECT COUNT(*) AS cnt FROM mws.report {bw}", ap)[0]["cnt"]
        issues = []

        # RPT-001 failed/pending
        rows = q(conn, f"""SELECT status, COUNT(*) AS cnt FROM mws.report {bw}
            AND status != 'processed' GROUP BY status""", ap)
        cnt = sum(r["cnt"] for r in rows)
        if cnt:
            issues.append({"id":"RPT-001","title":"Failed/pending downloads",
                "count":int(cnt),"fix_action":"redrive","severity":"critical" if cnt>20 else "high",
                "breakdown":[{"status":r["status"],"count":int(r["cnt"])} for r in rows]})

        # RPT-002 not replicated
        cnt2 = q(conn, f"""SELECT COUNT(*) AS cnt FROM mws.report {bw}
            AND (copy_status != 'REPLICATED' OR copy_status IS NULL)""", ap)[0]["cnt"]
        if cnt2:
            breakdown = q(conn, f"""SELECT COALESCE(copy_status,'NULL') AS cs, COUNT(*) AS cnt
                FROM mws.report {bw} AND (copy_status != 'REPLICATED' OR copy_status IS NULL)
                GROUP BY 1""", ap)
            issues.append({"id":"RPT-002","title":"Not replicated to Redshift",
                "count":int(cnt2),"fix_action":"recopy","severity":"high",
                "breakdown":[{"status":r["cs"],"count":int(r["cnt"])} for r in breakdown]})

        # RPT-003 stuck >2h
        cnt3 = q(conn, f"""SELECT COUNT(*) AS cnt FROM mws.report {bw}
            AND status='processed'
            AND (copy_status IS NULL OR copy_status='NOT_REPLICATED')
            AND download_date < CURRENT_TIMESTAMP - INTERVAL '2 hours'""", ap)[0]["cnt"]
        if cnt3:
            issues.append({"id":"RPT-003","title":"Stuck >2h (processed, not copied)",
                "count":int(cnt3),"fix_action":"redrive_copy","severity":"high","breakdown":[]})

        conn.close()
        return int(total), issues

    # ── Nodes ──────────────────────────────────────────────────────────────────

    def node_scan(state: ReportTriageState) -> dict:
        try:
            total, issues = _scan_report(state.get("account"))
            msg = f"Scanned mws.report — {total:,} rows, {len(issues)} issue(s)"
            return {"total_rows": total, "issues": issues,
                    "trace": [_t("scan", msg, "warning" if issues else "success")]}
        except Exception as e:
            return {"status":"error","error":str(e),
                    "trace":[_t("scan",f"Scan failed: {e}","error")]}

    def node_classify(state: ReportTriageState) -> dict:
        issues    = state.get("issues",[])
        threshold = state.get("threshold", 50)

        if not issues:
            return {"classification":{"strategy":"clean","fix_actions":[],
                "needs_approval":False},"status":"clean",
                "trace":[_t("classify","No issues — mws.report is clean","success")]}

        llm = ChatOpenAI(model="gpt-4o", temperature=0,
                         openai_api_key=os.getenv("OPENAI_API_KEY",""))
        system = f"""You are WiziAgent, an autonomous data quality agent for Intentwise.
Analyse issues in mws.report and decide the fix strategy.
Approval threshold: {threshold} rows. Fixes affecting >= {threshold} rows are HIGH RISK and need human approval.
For each issue:
- RPT-001 (failed downloads, count < {threshold}): auto_fix via redrive
- RPT-001 (count >= {threshold}): needs_approval=true (too many failures — upstream issue likely)
- RPT-002 (not replicated): auto_fix via recopy (always safe)
- RPT-003 (stuck >2h): auto_fix via redrive_copy (always safe)
Respond ONLY with JSON (no markdown):
{{"strategy":"auto_fix|needs_approval|escalate","fix_actions":["redrive","recopy"],"needs_approval":true|false,"escalate_reason":"","summary":"one sentence","risk_reason":"why approval needed or empty"}}"""
        try:
            resp = llm.invoke([SystemMessage(content=system),
                               HumanMessage(content=json.dumps(issues, indent=2))])
            text = resp.content.strip().lstrip("```json").rstrip("```").strip()
            cl   = json.loads(text)
            needs = cl.get("needs_approval", False)
            return {"classification": cl, "needs_approval": bool(needs),
                    "trace":[_t("classify",
                        f"Strategy: {cl.get('strategy')} — {cl.get('summary','')}",
                        "warning" if needs else "info")]}
        except Exception as e:
            # safe fallback
            total_affected = sum(i["count"] for i in issues)
            needs = total_affected >= threshold
            cl = {"strategy":"needs_approval" if needs else "auto_fix",
                  "fix_actions":[i["fix_action"] for i in issues],
                  "needs_approval": needs,
                  "escalate_reason":"LLM unavailable",
                  "summary":"Fallback: auto-classify by threshold",
                  "risk_reason":f"Total affected rows {total_affected} >= threshold {threshold}" if needs else ""}
            return {"classification":cl,"needs_approval":needs,
                    "trace":[_t("classify",f"LLM failed ({e}), fallback used","warning")]}

    def node_request_approval(state: ReportTriageState) -> dict:
        """Post to Slack and store approval token. Returns immediately — does NOT block."""
        token     = str(uuid.uuid4())[:8]
        issues    = state.get("issues",[])
        cl        = state.get("classification",{})
        slack_url = os.getenv("SLACK_WEBHOOK_URL","")
        base_url  = os.getenv("BACKEND_URL",
                    "https://intentwise-backend-production.up.railway.app")

        # Register token
        evt = threading.Event()
        _approval_store[token] = {"decision": None, "event": evt}

        approve_url = f"{base_url}/api/wizi-agent/approve"
        lines = [
            f"*⚠️ WiziAgent — Approval Required*",
            f"Table: `mws.report` | Risk: {cl.get('risk_reason','')}",
            "",
        ]
        for i in issues:
            lines.append(f"  • {i['title']}: *{i['count']:,} rows* affected ({i['fix_action']})")
        lines += [
            "",
            f"To approve:  `POST {approve_url}` `{{\"token\":\"{token}\",\"decision\":\"approve\"}}`",
            f"To reject:   `POST {approve_url}` `{{\"token\":\"{token}\",\"decision\":\"reject\"}}`",
            f"Or use the UI: Automation → Report Triage → Pending Approvals",
        ]
        msg = "\n".join(lines)

        if slack_url:
            try:
                import urllib.request as ur
                data = json.dumps({"text": msg}).encode()
                req  = ur.Request(slack_url, data=data,
                        headers={"Content-Type":"application/json"})
                ur.urlopen(req, timeout=5)
            except: pass

        return {
            "approval_token":  token,
            "approval_status": "pending",
            "status":          "awaiting_approval",
            "trace": [_t("request_approval",
                f"Approval requested (token: {token}) — awaiting human decision",
                "warning")],
        }

    def node_await_approval(state: ReportTriageState) -> dict:
        """Block (up to 10 min) waiting for the approval decision."""
        token = state.get("approval_token","")
        entry = _approval_store.get(token)
        if not entry:
            return {"approval_status":"rejected",
                    "trace":[_t("await_approval","Token not found — rejecting","error")]}

        # Wait up to 10 minutes
        granted = entry["event"].wait(timeout=600)
        decision = entry.get("decision","rejected")

        if not granted or decision == "rejected":
            return {"approval_status":"rejected","status":"escalated",
                    "trace":[_t("await_approval",
                        "Approval rejected or timed out — no changes made","warning")]}

        return {"approval_status":"approved",
                "trace":[_t("await_approval","✅ Approved — proceeding with fix","success")]}

    def node_auto_fix(state: ReportTriageState) -> dict:
        cl       = state.get("classification",{})
        issues   = state.get("issues",[])
        account  = state.get("account")
        af       = "AND account = %s" if account and account != "all" else ""
        ap       = [account] if account and account != "all" else []
        bw       = f"WHERE 1=1 {af}"

        FIX_SQL = {
            "redrive":      f"UPDATE mws.report SET status='pending', tries=0 {bw} AND status='failed'",
            "recopy":       f"UPDATE mws.report SET copy_status='NOT_REPLICATED' {bw} AND status='processed' AND copy_status IS NULL",
            "redrive_copy": f"UPDATE mws.report SET copy_status='NOT_REPLICATED' {bw} AND status='processed' AND copy_status IS NULL AND download_date < CURRENT_TIMESTAMP - INTERVAL '2 hours'",
        }
        COUNT_SQL = {
            "redrive":      f"SELECT COUNT(*) AS cnt FROM mws.report {bw} AND status='failed'",
            "recopy":       f"SELECT COUNT(*) AS cnt FROM mws.report {bw} AND status='processed' AND copy_status IS NULL",
            "redrive_copy": f"SELECT COUNT(*) AS cnt FROM mws.report {bw} AND status='processed' AND copy_status IS NULL AND download_date < CURRENT_TIMESTAMP - INTERVAL '2 hours'",
        }

        fix_actions = cl.get("fix_actions",[])
        results = []; trace = []
        try:
            conn = get_connection()
            for action in fix_actions:
                if action not in FIX_SQL: continue
                before   = q(conn, f"SELECT COUNT(*) AS cnt FROM mws.report {bw}", ap)[0]["cnt"]
                affected = q(conn, COUNT_SQL[action], ap)[0]["cnt"]
                cur = conn.cursor()
                cur.execute(FIX_SQL[action], ap)
                conn.commit()
                after = q(conn, f"SELECT COUNT(*) AS cnt FROM mws.report {bw}", ap)[0]["cnt"]
                results.append({"action":action,"rows_affected":int(affected),
                                 "before":int(before),"after":int(after)})
                trace.append(_t("auto_fix",
                    f"✓ {action}: {affected} rows fixed (before:{before} → after:{after})","success"))
            conn.close()
        except Exception as e:
            trace.append(_t("auto_fix", f"Fix failed: {e}", "error"))

        return {"fix_results":results,"trace":trace,
                "status":"fixed" if results else "escalated",
                "verify_attempts": 0}

    def node_verify(state: ReportTriageState) -> dict:
        """
        Re-scan. If issues remain and attempts < 3, re-run targeted fixes.
        On 3rd failed attempt, escalate.
        """
        attempt  = state.get("verify_attempts", 0) + 1
        account  = state.get("account")
        af       = "AND account = %s" if account and account != "all" else ""
        ap       = [account] if account and account != "all" else []
        bw       = f"WHERE 1=1 {af}"

        try:
            _, remaining = _scan_report(account)
            if not remaining:
                return {"verify_issues":[],"verify_attempts":attempt,"status":"fixed",
                        "trace":[_t("verify",
                            f"✅ Attempt {attempt}: all issues resolved — mws.report is clean",
                            "success")]}

            ids = ", ".join(i["id"] for i in remaining)
            trace = [_t("verify",
                f"Attempt {attempt}/3: {len(remaining)} issue(s) remain ({ids})",
                "warning")]

            if attempt >= 3:
                return {"verify_issues":remaining,"verify_attempts":attempt,"status":"escalated",
                        "trace": trace + [_t("verify",
                            "Max retries reached — escalating for manual review","error")]}

            # Re-run targeted fix on remaining issues
            FIX_SQL = {
                "redrive":      f"UPDATE mws.report SET status='pending', tries=0 {bw} AND status='failed'",
                "recopy":       f"UPDATE mws.report SET copy_status='NOT_REPLICATED' {bw} AND status='processed' AND copy_status IS NULL",
                "redrive_copy": f"UPDATE mws.report SET copy_status='NOT_REPLICATED' {bw} AND status='processed' AND copy_status IS NULL AND download_date < CURRENT_TIMESTAMP - INTERVAL '2 hours'",
            }
            retry_results = []
            conn = get_connection()
            for issue in remaining:
                action = issue.get("fix_action")
                if action and action in FIX_SQL:
                    cur = conn.cursor()
                    cur.execute(FIX_SQL[action], ap)
                    conn.commit()
                    retry_results.append(action)
                    trace.append(_t("verify",
                        f"  ↺ Re-applied {action} for {issue['id']}","info"))
            conn.close()

            return {"verify_issues":remaining,"verify_attempts":attempt,
                    "trace": trace + [_t("verify",
                        f"Re-fixed {len(retry_results)} action(s) — will re-scan","info")]}

        except Exception as e:
            return {"verify_issues":[],"verify_attempts":attempt,
                    "trace":[_t("verify",f"Verify error: {e}","error")]}

    def node_escalate(state: ReportTriageState) -> dict:
        reason = state.get("classification",{}).get("escalate_reason","")
        issues = state.get("issues",[])
        msg    = f"{len(issues)} issue(s) escalated for human review"
        if reason: msg += f": {reason}"
        return {"status":"escalated",
                "trace":[_t("escalate", msg, "warning")]}

    def node_notify(state: ReportTriageState) -> dict:
        slack_url = os.getenv("SLACK_WEBHOOK_URL","")
        st        = state.get("status","unknown")
        fixes     = state.get("fix_results",[])
        remaining = state.get("verify_issues",[])
        attempts  = state.get("verify_attempts",0)

        emoji = "✅" if st=="fixed" else "⚠️" if st=="escalated" else "ℹ️"
        lines = [f"*{emoji} WiziAgent Report Triage — {st.upper()}*",
                 f"Table: `mws.report`"]
        if fixes:
            lines.append(f"Fixes applied ({attempts} verify attempt(s)):")
            for r in fixes:
                lines.append(f"  ✓ `{r['action']}`: {r['rows_affected']} rows ({r['before']}→{r['after']})")
        if remaining:
            lines.append(f"Still open: {', '.join(i['id'] for i in remaining)}")
        if state.get("approval_status") == "rejected":
            lines.append("No changes made — approval was rejected or timed out")

        msg = "\n".join(lines)
        notified = False
        if slack_url:
            try:
                import urllib.request as ur
                data = json.dumps({"text": msg}).encode()
                req  = ur.Request(slack_url, data=data,
                        headers={"Content-Type":"application/json"})
                ur.urlopen(req, timeout=5)
                notified = True
            except: pass

        return {"notified": notified,
                "trace":[_t("notify",
                    "Slack notification sent" if notified else "Logged (no SLACK_WEBHOOK_URL)",
                    "success" if notified else "info")]}

    # ── Routing ────────────────────────────────────────────────────────────────

    def route_after_classify(state: ReportTriageState) -> str:
        if state.get("status") == "clean":      return "notify"
        if state.get("needs_approval"):          return "request_approval"
        return "auto_fix"

    def route_after_approval(state: ReportTriageState) -> str:
        if state.get("approval_status") == "approved": return "auto_fix"
        return "escalate"

    def route_after_verify(state: ReportTriageState) -> str:
        remaining = state.get("verify_issues",[])
        attempts  = state.get("verify_attempts",0)
        if not remaining:                        return "notify"
        if attempts >= 3:                        return "notify"
        return "verify"   # loop back

    # ── Build graph ────────────────────────────────────────────────────────────

    def build_triage_graph():
        g = StateGraph(ReportTriageState)
        g.add_node("scan",             node_scan)
        g.add_node("classify",         node_classify)
        g.add_node("request_approval", node_request_approval)
        g.add_node("await_approval",   node_await_approval)
        g.add_node("auto_fix",         node_auto_fix)
        g.add_node("escalate",         node_escalate)
        g.add_node("verify",           node_verify)
        g.add_node("notify",           node_notify)

        g.set_entry_point("scan")
        g.add_edge("scan",             "classify")
        g.add_conditional_edges("classify", route_after_classify, {
            "request_approval": "request_approval",
            "auto_fix":         "auto_fix",
            "notify":           "notify",
        })
        g.add_edge("request_approval", "await_approval")
        g.add_conditional_edges("await_approval", route_after_approval, {
            "auto_fix":  "auto_fix",
            "escalate":  "escalate",
        })
        g.add_edge("auto_fix",  "verify")
        g.add_edge("escalate",  "notify")
        g.add_conditional_edges("verify", route_after_verify, {
            "verify":  "verify",
            "notify":  "notify",
        })
        g.add_edge("notify", LG_END)
        return g.compile()

    _triage_graph = None
    def get_triage_graph():
        global _triage_graph
        if _triage_graph is None:
            _triage_graph = build_triage_graph()
        return _triage_graph


# ── WiziAgent — Generic Table Agent ──────────────────────────────────────────
# Runs on ANY mws table: scan → classify → fix → verify → log
# Supports approval threshold: auto-fix if affected < threshold, else escalate

if LANGGRAPH_AVAILABLE:

    class TableAgentState(TypedDict):
        schema:      str
        table:       str
        account:     Optional[str]
        threshold:   int               # row count above which to escalate
        # scan
        total_rows:  int
        alerts:      List[dict]        # from /api/agents/analyze
        # classify
        classification: Optional[dict]
        # fix
        fix_results: Annotated[List[dict], operator.add]
        # verify
        verify_alerts: List[dict]
        # meta
        notified:    bool
        trace:       Annotated[List[dict], operator.add]
        status:      str
        error:       Optional[str]


    def _t(node, msg, level="info"):
        return {"node":node,"ts":datetime.datetime.now().strftime("%H:%M:%S"),
                "msg":msg,"level":level}


    def table_node_scan(state: TableAgentState) -> dict:
        """Run real SQL checks on the target table via existing analyze logic."""
        schema = state["schema"]; table = state["table"]
        account = state.get("account")
        try:
            # Reuse the analyze endpoint logic directly
            payload = {"schema":schema,"table":table,"account_id":account,"agent_type":"full"}
            conn = get_connection()
            cols = q(conn,"""SELECT column_name,data_type FROM information_schema.columns
                WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position""",
                [schema,table])
            col_names = [c["column_name"] for c in cols]
            date_cols = [c["column_name"] for c in cols if "date" in c["data_type"] or "timestamp" in c["data_type"]]
            num_cols  = [c["column_name"] for c in cols if c["data_type"] in
                ("integer","bigint","numeric","double precision","real","float")]
            id_cols   = [c for c in col_names if c.endswith("_id") or c in
                ("id","amazon_order_id","asin","fnsku","request_id","report_id")]
            total = q(conn,f"SELECT COUNT(*) AS cnt FROM {schema}.{table}")[0]["cnt"]
            alerts = []
            now = datetime.datetime.now().strftime("%I:%M %p")

            for col in id_cols[:5]:
                try:
                    nc = q(conn,f"SELECT COUNT(*) AS cnt FROM {schema}.{table} WHERE {col} IS NULL")[0]["cnt"]
                    if nc:
                        pct = nc*100//max(total,1)
                        alerts.append({"id":f"WA-NULL-{col}","title":f"NULL {col}",
                            "severity":"high" if pct>5 else "medium","count":int(nc),
                            "fix_type":"flag_nulls","fix_sql":f"UPDATE {schema}.{table} SET {col}='UNKNOWN' WHERE {col} IS NULL",
                            "table":f"{schema}.{table}","ts":now})
                except: pass

            for col in [c for c in id_cols if c != "id"][:2]:
                try:
                    dc = q(conn,f"""SELECT COUNT(*) AS cnt FROM (
                        SELECT {col} FROM {schema}.{table} GROUP BY {col} HAVING COUNT(*)>1) d""")[0]["cnt"]
                    if dc:
                        alerts.append({"id":f"WA-DUPE-{col}","title":f"Duplicate {col}",
                            "severity":"critical","count":int(dc),
                            "fix_type":"delete_dupes",
                            "fix_sql":f"DELETE FROM {schema}.{table} WHERE id NOT IN (SELECT MIN(id) FROM {schema}.{table} GROUP BY {col})",
                            "table":f"{schema}.{table}","ts":now})
                except: pass

            if date_cols:
                col = date_cols[0]
                try:
                    latest = q(conn,f"SELECT MAX({col}) AS mx FROM {schema}.{table}")[0]["mx"]
                    if latest:
                        age = (datetime.date.today() - (latest.date() if hasattr(latest,"date") else latest)).days
                        if age > 2:
                            alerts.append({"id":f"WA-FRESH-{col}","title":f"Stale data ({age}d old)",
                                "severity":"high" if age>7 else "medium","count":age,
                                "fix_type":None,
                                "fix_sql":None,
                                "table":f"{schema}.{table}","ts":now})
                except: pass

            conn.close()
            msg = f"Scanned {schema}.{table} — {total:,} rows, {len(alerts)} issue(s)"
            return {"total_rows":int(total),"alerts":alerts,
                    "trace":[_t("scan",msg,"warning" if alerts else "success")]}
        except Exception as e:
            return {"status":"error","error":str(e),
                    "trace":[_t("scan",f"Scan failed: {e}","error")]}


    def table_node_classify(state: TableAgentState) -> dict:
        alerts    = state.get("alerts",[])
        threshold = state.get("threshold",50)
        if not alerts:
            return {"classification":{"strategy":"clean","fix_actions":[]},"status":"clean",
                    "trace":[_t("classify","No issues — table is clean","success")]}

        llm = ChatOpenAI(model="gpt-4o",temperature=0,
                         openai_api_key=os.getenv("OPENAI_API_KEY",""))
        system = f"""You are WiziAgent, an autonomous data quality agent for Intentwise.
Analyse these issues on {state['schema']}.{state['table']} and decide the fix strategy.
Threshold for auto-fix: {threshold} rows. Above threshold → escalate for human review.
Rules:
- NULL values on non-critical columns: auto_fix if count < threshold
- Duplicate primary keys: escalate if count >= threshold (risky delete), else auto_fix
- Stale data (no fix_sql): escalate with explanation
- Any fix_sql=null: escalate
Respond ONLY with JSON (no markdown):
{{"strategy":"auto_fix|escalate|mixed","auto_fix_ids":["WA-NULL-x"],"escalate_ids":["WA-DUPE-y"],"escalate_reason":"","summary":"one sentence"}}"""

        try:
            resp = llm.invoke([SystemMessage(content=system),
                               HumanMessage(content=json.dumps(alerts,indent=2))])
            text = resp.content.strip().lstrip("```json").rstrip("```").strip()
            cl   = json.loads(text)
            return {"classification":cl,
                    "trace":[_t("classify",f"Strategy: {cl.get('strategy')} — {cl.get('summary','')}","info")]}
        except Exception as e:
            # fallback: auto-fix fixable, escalate the rest
            auto = [a["id"] for a in alerts if a.get("fix_sql")]
            esc  = [a["id"] for a in alerts if not a.get("fix_sql")]
            cl   = {"strategy":"mixed","auto_fix_ids":auto,"escalate_ids":esc,
                    "escalate_reason":"LLM unavailable","summary":"Fallback classification"}
            return {"classification":cl,
                    "trace":[_t("classify",f"LLM failed ({e}), fallback used","warning")]}


    def table_node_fix(state: TableAgentState) -> dict:
        alerts = state.get("alerts",[])
        cl     = state.get("classification",{})
        auto_ids = cl.get("auto_fix_ids",[])
        to_fix = [a for a in alerts if a["id"] in auto_ids and a.get("fix_sql")]
        if not to_fix:
            return {"fix_results":[],"trace":[_t("fix","No auto-fixable issues","info")]}

        results = []; trace = []
        try:
            conn = get_connection()
            schema = state["schema"]; table = state["table"]
            for alert in to_fix:
                before = q(conn,f"SELECT COUNT(*) AS cnt FROM {schema}.{table}")[0]["cnt"]
                cur = conn.cursor()
                cur.execute(alert["fix_sql"])
                conn.commit()
                after = q(conn,f"SELECT COUNT(*) AS cnt FROM {schema}.{table}")[0]["cnt"]
                results.append({"alert_id":alert["id"],"title":alert["title"],
                    "fix_type":alert["fix_type"],"rows_affected":int(before-after),
                    "before":int(before),"after":int(after)})
                trace.append(_t("fix",
                    f"✓ {alert['title']}: {before-after} rows fixed ({before}→{after})","success"))
            conn.close()
        except Exception as e:
            trace.append(_t("fix",f"Fix failed: {e}","error"))

        return {"fix_results":results,"trace":trace,"status":"fixed" if results else "escalated"}


    def table_node_escalate(state: TableAgentState) -> dict:
        cl     = state.get("classification",{})
        reason = cl.get("escalate_reason","")
        ids    = cl.get("escalate_ids",[])
        msg    = f"{len(ids)} issue(s) require human review" + (f": {reason}" if reason else "")
        return {"status":"escalated","trace":[_t("escalate",msg,"warning")]}


    def table_node_verify(state: TableAgentState) -> dict:
        """Re-scan the table after fixes to confirm issues are resolved."""
        schema = state["schema"]; table = state["table"]
        try:
            conn = get_connection()
            # Re-check each originally-fixed alert
            remaining = []
            for res in state.get("fix_results",[]):
                alert_id = res["alert_id"]
                orig = next((a for a in state.get("alerts",[]) if a["id"]==alert_id),None)
                if not orig: continue
                # Re-run the count for that check type
                if "NULL" in alert_id:
                    col = alert_id.replace("WA-NULL-","")
                    cnt = q(conn,f"SELECT COUNT(*) AS cnt FROM {schema}.{table} WHERE {col} IS NULL")[0]["cnt"]
                    if cnt: remaining.append({**orig,"count":int(cnt)})
                elif "DUPE" in alert_id:
                    col = alert_id.replace("WA-DUPE-","")
                    cnt = q(conn,f"""SELECT COUNT(*) AS cnt FROM (
                        SELECT {col} FROM {schema}.{table} GROUP BY {col} HAVING COUNT(*)>1) d""")[0]["cnt"]
                    if cnt: remaining.append({**orig,"count":int(cnt)})
            conn.close()
            if not remaining:
                return {"verify_alerts":[],"status":"fixed",
                        "trace":[_t("verify","✅ All fixes verified — table is clean","success")]}
            return {"verify_alerts":remaining,
                    "trace":[_t("verify",f"⚠ {len(remaining)} issue(s) remain after fix","warning")]}
        except Exception as e:
            return {"verify_alerts":[],"trace":[_t("verify",f"Verify error: {e}","error")]}


    def table_node_notify(state: TableAgentState) -> dict:
        slack_url = os.getenv("SLACK_WEBHOOK_URL","")
        tbl   = f"{state['schema']}.{state['table']}"
        st    = state.get("status","unknown")
        fixes = state.get("fix_results",[])
        remaining = state.get("verify_alerts",[])
        lines = [f"*✨ WiziAgent — {tbl} — {st.upper()}*"]
        for r in fixes:
            lines.append(f"  ✓ {r['title']}: {r['rows_affected']} rows fixed ({r['before']}→{r['after']})")
        if remaining:
            lines.append(f"  ⚠ {len(remaining)} issue(s) still open — escalation required")
        if state.get("classification",{}).get("escalate_reason"):
            lines.append(f"  Escalation: {state['classification']['escalate_reason']}")
        msg = "\n".join(lines)
        notified = False
        if slack_url:
            try:
                import urllib.request
                data = json.dumps({"text":msg}).encode()
                req  = urllib.request.Request(slack_url,data=data,
                        headers={"Content-Type":"application/json"})
                urllib.request.urlopen(req,timeout=5)
                notified = True
            except: pass
        return {"notified":notified,"trace":[_t("notify",
            "Slack notification sent" if notified else "Logged (no SLACK_WEBHOOK_URL)",
            "success" if notified else "info")]}


    def table_route_classify(state: TableAgentState) -> str:
        if state.get("status") == "clean":   return "notify"
        st = state.get("classification",{}).get("strategy","auto_fix")
        if st == "escalate":                  return "escalate"
        return "auto_fix"   # auto_fix or mixed


    def build_table_agent_graph():
        g = StateGraph(TableAgentState)
        g.add_node("scan",     table_node_scan)
        g.add_node("classify", table_node_classify)
        g.add_node("auto_fix", table_node_fix)
        g.add_node("escalate", table_node_escalate)
        g.add_node("verify",   table_node_verify)
        g.add_node("notify",   table_node_notify)
        g.set_entry_point("scan")
        g.add_edge("scan",     "classify")
        g.add_conditional_edges("classify", table_route_classify,
            {"auto_fix":"auto_fix","escalate":"escalate","notify":"notify"})
        g.add_edge("auto_fix", "verify")
        g.add_edge("escalate", "notify")
        g.add_edge("verify",   "notify")
        g.add_edge("notify",   LG_END)
        return g.compile()

    _table_agent_graph = None
    def get_table_agent_graph():
        global _table_agent_graph
        if _table_agent_graph is None:
            _table_agent_graph = build_table_agent_graph()
        return _table_agent_graph


@app.post("/api/wizi-agent/run-table")
async def wizi_agent_run_table(payload: dict = {}):
    """
    Run WiziAgent on any mws table.
    Body: { "schema": "mws", "table": "orders", "account": "...", "threshold": 50, "dry_run": false }
    Returns: { status, trace, alerts, fix_results, verify_alerts, notified, total_rows }
    """
    if not LANGGRAPH_AVAILABLE:
        return {"error":"LangGraph not installed. pip install langgraph langchain-openai"}

    schema    = payload.get("schema","mws")
    table     = payload.get("table","")
    account   = payload.get("account")
    threshold = int(payload.get("threshold",50))
    dry_run   = payload.get("dry_run",False)

    if not table:
        return {"error":"table is required"}

    init: TableAgentState = {
        "schema":schema,"table":table,"account":account,
        "threshold":threshold,"total_rows":0,"alerts":[],
        "classification":None,"fix_results":[],"verify_alerts":[],
        "notified":False,"trace":[],"status":"running","error":None,
    }

    if dry_run:
        init["classification"] = {
            "strategy":"escalate","auto_fix_ids":[],"escalate_ids":[],
            "escalate_reason":"dry_run=true","summary":"Dry run — scan only"}

    try:
        graph  = get_table_agent_graph()
        result = await asyncio.to_thread(graph.invoke, init)
        return {
            "status":         result.get("status"),
            "schema":         schema, "table": table,
            "total_rows":     result.get("total_rows",0),
            "alerts":         result.get("alerts",[]),
            "classification": result.get("classification"),
            "fix_results":    result.get("fix_results",[]),
            "verify_alerts":  result.get("verify_alerts",[]),
            "notified":       result.get("notified",False),
            "trace":          result.get("trace",[]),
            "error":          result.get("error"),
        }
    except Exception as e:
        return {"error":str(e),"status":"error",
                "trace":[{"node":"agent","ts":"","msg":str(e),"level":"error"}]}

@app.post("/api/wizi-agent/run")
async def wizi_agent_run(payload: dict = {}):
    """
    Run WiziAgent on mws.report.
    Body: {
      "account":   optional account filter,
      "dry_run":   true = scan+classify only (no fixes),
      "threshold": row count above which approval is required (default 50)
    }
    Returns: { status, trace, issues, fix_results, verify_issues,
               approval_token (if awaiting_approval), notified, total_rows }
    """
    if not LANGGRAPH_AVAILABLE:
        return {"error": "LangGraph not installed. pip install langgraph langchain-openai"}

    account   = payload.get("account")
    dry_run   = payload.get("dry_run", False)
    threshold = int(payload.get("threshold", 50))

    initial: ReportTriageState = {
        "account":         account,
        "total_rows":      0,
        "issues":          [],
        "classification":  None,
        "needs_approval":  False,
        "approval_token":  None,
        "approval_status": "skipped",
        "threshold":       threshold,
        "fix_results":     [],
        "verify_attempts": 0,
        "verify_issues":   [],
        "notified":        False,
        "trace":           [],
        "status":          "running",
        "error":           None,
    }

    if dry_run:
        initial["classification"] = {
            "strategy":"escalate","fix_actions":[],"needs_approval":False,
            "escalate_reason":"dry_run=true","summary":"Dry run: scan + classify only",
            "risk_reason":""
        }

    try:
        graph  = get_triage_graph()
        result = await asyncio.to_thread(graph.invoke, initial)
        return {
            "status":          result.get("status","unknown"),
            "total_rows":      result.get("total_rows",0),
            "issues":          result.get("issues",[]),
            "classification":  result.get("classification"),
            "needs_approval":  result.get("needs_approval",False),
            "approval_token":  result.get("approval_token"),
            "approval_status": result.get("approval_status","skipped"),
            "fix_results":     result.get("fix_results",[]),
            "verify_attempts": result.get("verify_attempts",0),
            "verify_issues":   result.get("verify_issues",[]),
            "notified":        result.get("notified",False),
            "trace":           result.get("trace",[]),
            "error":           result.get("error"),
        }
    except Exception as e:
        return {"error":str(e),"status":"error",
                "trace":[{"node":"agent","ts":"","msg":str(e),"level":"error"}]}


@app.post("/api/wizi-agent/approve")
def wizi_agent_approve(payload: dict = {}):
    """
    Submit an approval decision for a pending WiziAgent run.
    Body: { "token": "abc12345", "decision": "approve" | "reject" }
    """
    token    = payload.get("token","")
    decision = payload.get("decision","reject")
    entry    = _approval_store.get(token)
    if not entry:
        return {"error": f"Token '{token}' not found or already resolved"}
    entry["decision"] = decision
    entry["event"].set()   # unblock node_await_approval
    return {"token": token, "decision": decision, "accepted": True}


@app.get("/api/wizi-agent/status/{token}")
def wizi_agent_status(token: str):
    """
    Poll approval status for a pending WiziAgent run.
    Returns: { token, decision, resolved }
    """
    entry = _approval_store.get(token)
    if not entry:
        return {"token": token, "decision": None, "resolved": False,
                "error": "Token not found"}
    return {"token":    token,
            "decision": entry.get("decision"),
            "resolved": entry.get("decision") is not None}
