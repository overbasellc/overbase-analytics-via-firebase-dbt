WITH counts AS (
SELECT COUNT(DISTINCT user_pseudo_id) AS dist_cnt,count(1) AS cnt
FROM {{ref('fb_analytics_installs_raw')}}
WHERE {{ overbase_firebase.analyticsTestDateFilter('event_date',extend = 2) }}
AND event_date >='2024-08-22' --date when incrementel model was fixed
)

SELECT * FROM counts WHERE dist_cnt < cnt
