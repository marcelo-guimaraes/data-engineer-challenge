WITH RegionCounts AS (
    SELECT
        region,
        COUNT(*) AS region_count
    FROM `marcelo-jobsity-challenge.raw.raw_trips`
    GROUP BY region
    ORDER BY region_count DESC
    LIMIT 2
)

select 
  T.region,
  T.datasource AS latest_datasource
from `marcelo-jobsity-challenge.raw.raw_trips` T
join RegionCounts RC ON T.region = RC.region
qualify 1 = row_number() over(partition by T.region order by T.datetime desc);