WITH
inactivity AS (
	SELECT [session_id],
		   [player_id],
		   [country],
		   [start_time],
		   [end_time],
		   DATEFROMPARTS(YEAR([start_time]), MONTH([start_time]), DAY([start_time])) AS [day_start_time],
		   CAST(DATEDIFF(SECOND, start_time, end_time) AS DECIMAL(10, 2)) AS [duration],
		   DATEDIFF(SECOND, LAG([end_time]) OVER (PARTITION BY [player_id] ORDER BY [start_time]), [start_time]) AS [inactivity]
	FROM [dbo].[game_sessions]
),
sessioned AS (
	SELECT [session_id],
           [player_id],
           [country],
           [start_time],
           [end_time],
		   [day_start_time],
		   [duration],
		   IIF([inactivity] < 300, [inactivity], 0) AS [inactivity_adjusted],
		   SUM(IIF([inactivity] < 300, 0, 1)) OVER (ORDER BY [player_id], [end_time]) AS [session_id_adjusted]
	FROM inactivity
),
collapsed AS (
	SELECT [player_id],
		   [country],
		   [day_start_time],
		   [session_id_adjusted],
		   SUM([duration] + [inactivity_adjusted]) AS [duration]
	FROM sessioned
	GROUP BY [player_id], [country], [session_id_adjusted], [day_start_time]
),
analysis AS (
	SELECT [player_id],
		   [country],
		   -- assuming session data will be strictly in daily windows
		   -- for days-overlapping sessions additional processing is required to cut sessions at midnight
		   SUM([duration]) / 60 AS [duration_of_all_sessions],
		   MIN([duration]) / 60 AS [shortest_session],
		   MAX([duration]) / 60 AS [longest_session]
	FROM collapsed
	GROUP BY [player_id], [country], [day_start_time]
)

SELECT [player_id],
       [country],
       [duration_of_all_sessions],
	   [shortest_session],
	   [longest_session],
	   ROW_NUMBER() OVER(PARTITION BY [country] ORDER BY [duration_of_all_sessions] DESC) AS [rank_of_the_user]
FROM analysis