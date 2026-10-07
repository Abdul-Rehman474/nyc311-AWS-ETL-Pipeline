-- Complaint volume per type per month, with each type's rank within its month.
SELECT
    created_year,
    created_month,
    complaint_type,
    complaints,
    RANK() OVER (PARTITION BY created_year, created_month ORDER BY complaints DESC) AS rank_in_month
FROM (
    SELECT
        year(created_date)  AS created_year,
        month(created_date) AS created_month,
        complaint_type,
        COUNT(*)            AS complaints
    FROM nyc311_db.nyc311_curated
    WHERE created_date IS NOT NULL
    GROUP BY year(created_date), month(created_date), complaint_type
)
