
SELECT app_info.version, COUNT(1) as cnt, COUNT(DISTINCT(user_pseudo_id)) as users
FROM {{ source("firebase_analytics", "events") }}  as events
WHERE True 
AND _TABLE_SUFFIX = '20231010'
GROUP BY 1
ORDER BY 2 DESC