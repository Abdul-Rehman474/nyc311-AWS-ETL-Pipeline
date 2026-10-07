-- How long closed complaints took to close, per agency and complaint type.
-- Rows where closed_date precedes created_date are bad source data and excluded.
SELECT
    agency,
    agency_name,
    complaint_type,
    COUNT(*)                                                                   AS closed_complaints,
    ROUND(AVG(date_diff('minute', created_date, closed_date)) / 60.0, 2)       AS avg_hours_to_close,
    ROUND(approx_percentile(date_diff('minute', created_date, closed_date), 0.5) / 60.0, 2) AS median_hours_to_close
FROM nyc311_db.nyc311_curated
WHERE created_date IS NOT NULL
  AND closed_date IS NOT NULL
  AND closed_date >= created_date
GROUP BY agency, agency_name, complaint_type
