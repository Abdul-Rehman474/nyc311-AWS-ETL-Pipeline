-- Open vs. closed backlog: complaint counts per status per borough, as of the data freeze point.
SELECT
    borough,
    status,
    COUNT(*) AS complaints
FROM nyc311_db.nyc311_curated
GROUP BY borough, status
