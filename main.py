from fastapi import FastAPI, Query
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import psycopg2, psycopg2.extras, os, httpx, json
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

