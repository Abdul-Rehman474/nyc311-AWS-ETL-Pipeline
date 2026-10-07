-- Complaints per borough per month.
SELECT
    year(created_date)  AS created_year,
    month(created_date) AS created_month,
    borough,
    COUNT(*)            AS complaints
FROM nyc311_db.nyc311_curated
WHERE created_date IS NOT NULL
GROUP BY year(created_date), month(created_date), borough
