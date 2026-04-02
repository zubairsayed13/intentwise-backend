# Runbook: GDS Copy Job Failures

## Trigger
Check "Stuck Copies" fails — processed rows exist but copy_status != 'REPLICATED'.

## Severity
High — Google Data Studio / downstream BI reports will show stale data.

## Step 1: Identify stuck records
```sql
SELECT report_type, copy_status, copy_started_at,
       DATEDIFF('minute', copy_started_at, GETDATE()) as stuck_minutes,
       COUNT(*) as cnt
FROM mws.report
WHERE status = 'processed'
  AND copy_status != 'REPLICATED'
  AND download_date >= CURRENT_DATE - 1
GROUP BY 1, 2, 3
ORDER BY stuck_minutes DESC;
```
If stuck_minutes > 30, the copy job has stalled.

## Step 2: Check copy error details
```sql
SELECT report_type, copy_status, copy_error, copy_started_at
FROM mws.report
WHERE copy_status IN ('FAILED', 'ERROR', 'TIMEOUT')
  AND download_date >= CURRENT_DATE - 1
LIMIT 20;
```

## Step 3: Resolution paths

### copy_status = 'FAILED' with permission error
- Verify GDS service account has write access to the target dataset
- Check if the BigQuery/GDS credentials have expired and rotate them

### copy_status = 'IN_PROGRESS' stuck > 30 min
- The copy worker is hung — restart it:
  `sudo systemctl restart gds-copy-worker`
- Reset stuck records so they retry:
```sql
UPDATE mws.report
SET copy_status = 'PENDING', copy_started_at = NULL
WHERE copy_status = 'IN_PROGRESS'
  AND copy_started_at < GETDATE() - INTERVAL '45 minutes';
```

### copy_status = 'TIMEOUT'
- Network issue between Redshift and GDS destination
- Check VPC/firewall rules — GDS destination must be reachable from your VM
- Retry manually: `POST /api/workflow/ads-sop/retry-copy`

## Step 4: Force re-copy for today's data
```sql
UPDATE mws.report
SET copy_status = 'PENDING'
WHERE status = 'processed'
  AND copy_status != 'REPLICATED'
  AND download_date = CURRENT_DATE;
```
Then trigger the copy job to pick up the reset rows.

## Step 5: Verify
```sql
SELECT copy_status, COUNT(*)
FROM mws.report
WHERE download_date = CURRENT_DATE
GROUP BY 1;
```
All rows should show copy_status = 'REPLICATED'.

## Escalation
If copy failures persist after retry, check GDS destination quota limits and contact the BI team.
