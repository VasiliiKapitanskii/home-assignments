{{ config(
  materialized = 'table',
) }}

WITH

leads AS (
  SELECT state,
  	     entry_month,
  	     COUNT(*) AS total_leads,
  	     SUM(CASE WHEN appt_datetime IS NOT NULL THEN 1 ELSE 0 END) AS total_appointments,
  	     SUM(CASE WHEN demo = 1 THEN 1 ELSE 0 END) AS total_demos
  FROM {{ ref('raw_leads') }}
  WHERE state IS NOT NULL
  GROUP BY entry_month, state
)

SELECT state,
       entry_month,
	     total_leads,
	     total_appointments,
	     total_demos,
	     COALESCE(total_appointments / NULLIF(CAST(total_leads AS DECIMAL), 0), 0) AS appointments_percent,
	     COALESCE(total_demos / NULLIF(CAST(total_leads AS DECIMAL), 0), 0) AS demos_percent
FROM leads
ORDER BY state, entry_month
  