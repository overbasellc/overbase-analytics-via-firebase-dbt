{%- macro analyticsDateFilterFor(dateField) -%}
	{%- set startEndTSTuple = overbase_firebase.analyticsStartEndTimestampsTuple() -%}
	{{ dateField }} BETWEEN DATE({{ startEndTSTuple[0] }}) AND DATE({{ startEndTSTuple[1] }})
{%- endmacro -%}

{%- macro crashlyticsDateFilterFor(dateField) -%}
	{%- set startEndTSTuple = overbase_firebase.crashlyticsStartEndTimestampsTuple() -%}
	{{ dateField }} BETWEEN DATE({{ startEndTSTuple[0] }}) AND DATE({{ startEndTSTuple[1] }})
{%- endmacro -%}

{%- macro analyticsTSFilterFor(tsField) -%}
	{%- set startEndTSTuple = overbase_firebase.analyticsStartEndTimestampsTuple() -%}
	{{ tsField }} BETWEEN {{ startEndTSTuple[0] }} AND {{ startEndTSTuple[1] }}	
{%- endmacro -%}

{%- macro crashlyticsTSFilterFor(tsField) -%}
	{%- set startEndTSTuple = overbase_firebase.crashlyticsStartEndTimestampsTuple() -%}
	{{ tsField }} BETWEEN {{ startEndTSTuple[0] }} AND {{ startEndTSTuple[1] }}	
{%- endmacro -%}

{%- macro analyticsTableSuffixFilter() -%}
	{%- set startEndTSTuple = overbase_firebase.analyticsStartEndTimestampsTuple() -%}
  REPLACE(_TABLE_SUFFIX, 'intraday_', '') BETWEEN FORMAT_DATE('%Y%m%d', {{ startEndTSTuple[0] }}) AND FORMAT_DATE('%Y%m%d', {{ startEndTSTuple[1] }})
{%- endmacro -%}

{%- macro analyticsStartEndTimestampsTuple() -%}
	{%- if is_incremental() -%}
		{%- set tsStart = "TIMESTAMP_SUB(TIMESTAMP(CURRENT_DATE()), INTERVAL " ~ var('OVERBASE:FIREBASE_ANALYTICS_DEFAULT_INCREMENTAL_DAYS', "5")  ~ " DAY)" -%}
		{%- set tsEnd = "TIMESTAMP(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY))" -%}
		{{ return((tsStart, tsEnd)) }}
	{%- else -%}
		{%- set tsStart = "TIMESTAMP('" ~ var('OVERBASE:FIREBASE_ANALYTICS_FULL_REFRESH_START_DATE', '2018-01-01') ~ "')" -%}
		{%- if var('OVERBASE:FIREBASE_ANALYTICS_FULL_REFRESH_END_DATE', "nada") == 'nada' -%}
			{%- set tsEnd = "TIMESTAMP(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY))" -%}
		{%- else -%}
			{%- set tsEnd = "TIMESTAMP('" ~ var('OVERBASE:FIREBASE_ANALYTICS_FULL_REFRESH_END_DATE', "")  ~ "')" -%}
		{%- endif -%}
		{{ return((tsStart, tsEnd)) }}
	{%- endif -%}
{%- endmacro -%}


{%- macro crashlyticsStartEndTimestampsTuple() -%}
	{%- if is_incremental() -%}
		{%- set tsStart = "TIMESTAMP_SUB(TIMESTAMP(CURRENT_DATE()), INTERVAL " ~ var('OVERBASE:FIREBASE_CRASHLYTICS_DEFAULT_INCREMENTAL_DAYS', "5")  ~ " DAY)" -%}
		{%- set tsEnd = "TIMESTAMP(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY))" -%}
		{{ return((tsStart, tsEnd)) }}
	{%- else -%}
		{%- set tsStart = "TIMESTAMP('" ~ var('OVERBASE:FIREBASE_CRASHLYTICS_FULL_REFRESH_START_DATE', '2018-01-01') ~ "')" -%}
		{%- if var('OVERBASE:FIREBASE_CRASHLYTICS_FULL_REFRESH_END_DATE', "nada") == 'nada' -%}
			{%- set tsEnd = "TIMESTAMP(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY))" -%}
		{%- else -%}
			{%- set tsEnd = "TIMESTAMP('" ~ var('OVERBASE:FIREBASE_CRASHLYTICS_FULL_REFRESH_END_DATE', "")  ~ "')" -%}
		{%- endif -%}
		{{ return((tsStart, tsEnd)) }}
	{%- endif -%}
{%- endmacro -%}

