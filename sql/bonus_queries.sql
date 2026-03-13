-- a) Of the two most frequent regions, what is the latest source?
WITH top_regions AS (
  SELECT region, COUNT(*) AS trips
  FROM trips
  GROUP BY region
  ORDER BY trips DESC
  LIMIT 2
), ranked AS (
  SELECT
    t.region,
    t.datasource,
    t.datetime,
    ROW_NUMBER() OVER (PARTITION BY t.region ORDER BY t.datetime DESC) AS rn
  FROM trips t
  JOIN top_regions tr ON tr.region = t.region
)
SELECT region, datasource, datetime
FROM ranked
WHERE rn = 1;

-- b) In which regions did the source "cheap_mobile" appear?
SELECT DISTINCT region
FROM trips
WHERE datasource = 'cheap_mobile'
ORDER BY region;
