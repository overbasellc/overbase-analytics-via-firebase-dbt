{{ config(
    severity =  'error'
) }}

WITH stg AS (
SELECT event_date, COUNT(*) AS cnt FROM {{ ref('fb_analytics_events_raw') }} 
WHERE {{ overbase_firebase.analyticsDateFilterFor('event_date',extend=2) }}
GROUP BY 1
)
, src AS (

SELECT event_date,COUNT(*) AS cnt
FROM {{ source("firebase_analytics", "events") }}
WHERE {{ overbase_firebase.analyticsTableSuffixFilter(extend =3) }}
AND {{ overbase_firebase.analyticsDateFilterFor('event_date',extend=2) }}
GROUP BY 1
)
select * from 
stg left join src on stg.event_date = src.event_date
where stg.cnt <> src.cnt
