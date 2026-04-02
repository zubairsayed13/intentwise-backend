# Runbook: Custom Check Failures

## Trigger
Any user-defined check in a custom workflow fails its pass condition.

## Severity
Varies — determined by the check's configured severity level (critical / high / medium / low).

## Step 1: Understand what the check is testing
Review the check's SQL and pass condition in the dashboard (Workflows → Edit → check detail).
Key questions:
- Is it a row count check (rows = 0, rows > 0)?
- Is it a value threshold (value < X)?
- Is it a freshness check (max date = today)?

## Step 2: Run the check SQL manually in Query tab
Copy the check SQL and run it directly to see the raw output.
Compare the result against the pass condition to confirm the failure is real (not a transient error).

## Step 3: Common failure patterns

### "rows > 0" fails (expected data missing)
- Source table may not have loaded yet — check upstream pipeline status
- Filter in the SQL may be too restrictive — verify date filters use correct timezone
- Partition pruning issue on Redshift — run ANALYZE on the table:
  `ANALYZE schema.table_name;`

### "rows = 0" fails (unexpected rows found)
- Data quality issue in source — duplicate records, nulls, or out-of-range values
- Run the check SQL and inspect the offending rows
- Determine if this is a one-time data issue or a recurring pipeline problem

### "value < threshold" fails
- Metric has genuinely crossed the threshold — verify with the business team
- Or threshold is too tight — adjust the pass condition in the workflow editor

### Check SQL errors out (not a logic failure)
- Table or column renamed — update the SQL in the workflow editor
- Permissions issue — verify the Redshift user has SELECT on the target table
- Syntax error from a recent edit — validate SQL in the Query tab before saving

## Step 4: Triage the issue
Use the Triage tab — failed checks are automatically scored and surfaced there.
The AI explainer (⚡ Explain) gives a one-line root cause based on the SQL and row count.

## Step 5: Mark as investigated
Add a note to the check result in the Results tab so the team knows it's been looked at.

## Step 6: Fix and re-run
- If it's a data issue: fix upstream, then re-run the workflow manually (▶ Run Now)
- If it's a check definition issue: edit the SQL/pass condition, save, then re-run
- If it's a known false positive: adjust severity or add a filter to the SQL

## Escalation
Critical checks unresolved after 30 minutes should be escalated with the check name, SQL, and row count attached.
