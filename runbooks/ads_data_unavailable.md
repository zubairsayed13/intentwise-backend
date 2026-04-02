# Runbook: Amazon Ads Data Not Available

## Trigger
Check "Amazon Ads Data Available" fails — no processed rows found in mws.report for today.

## Severity
Critical — ads campaigns cannot be optimized without fresh data.

## Step 1: Confirm scope
```sql
SELECT download_date, status, COUNT(*) as cnt
FROM mws.report
WHERE download_date >= CURRENT_DATE - 2
GROUP BY 1, 2
ORDER BY 1 DESC;
```
Expected: rows with status='processed' for today. If missing, data pipeline has not completed.

## Step 2: Check for stuck pending downloads
```sql
SELECT report_type, status, created_at, updated_at
FROM mws.report
WHERE status IN ('pending', 'in_progress')
  AND download_date = CURRENT_DATE
ORDER BY created_at;
```
If rows exist here, the download job is running but not completing.

## Step 3: Check for failed downloads
```sql
SELECT report_type, error_message, COUNT(*) as cnt
FROM mws.report
WHERE status = 'failed'
  AND download_date = CURRENT_DATE
GROUP BY 1, 2;
```
Note the error_message — this tells you if it's an API rate limit, auth issue, or timeout.

## Step 4: Resolution paths

### If status = 'failed' with auth error
- Rotate Amazon MWS/SP-API credentials in your secrets manager
- Re-trigger the download job manually

### If status = 'pending' for > 2 hours
- Check if the download worker process is running on the VM
- Restart the worker: `sudo systemctl restart mws-downloader`
- Monitor: re-run Step 1 after 10 minutes

### If no rows at all (not even pending)
- The scheduler did not trigger — check cron logs
- Manually trigger: `POST /api/workflow/ads-sop`

## Step 5: Verify resolution
Re-run the "Amazon Ads Data Available" check. Expect rows > 0 with status='processed'.

## Escalation
If unresolved after 1 hour, escalate to data engineering. Ads campaigns will be running on stale data.
