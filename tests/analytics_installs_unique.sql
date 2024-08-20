WITH counts AS (
SELECT COUNT(DISTINCT user_pseudo_id) AS dist_cnt,count(1) AS cnt
FROM {{ref('fb_analytics_events_raw')}}
WHERE {{ overbase_firebase.analyticsDateFilterFor('event_date',extend = 2) }}
)

SELECT * FROM counts WHERE dist_cnt < cnt
