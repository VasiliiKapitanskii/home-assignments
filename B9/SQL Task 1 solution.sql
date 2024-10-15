-- Option 1: Use recursion (this)
-- Option 2: Create Dates dimension and reuse weeks from it
-- Option 3: Use table-valued UDF to generate weeks using a loop
-- Option 4: SQL Server 2022 Preview - Use GENERATE_SERIES ( start, stop [, step ] )
WITH
weeksRange AS (
	SELECT MIN([check_in_date]) AS min_check_in,
	       MAX([check_out_date]) AS max_check_out
	FROM [dbo].[reservations]
),
weeks AS (
    SELECT [week] = CONVERT(DATETIME, min_check_in)
	FROM weeksRange

    UNION ALL

	SELECT [week] = DATEADD(WEEK, 1, [week])
    FROM weeks, weeksRange
    WHERE [week] < max_check_out
),
counted AS (
	SELECT [week], COUNT(*) AS number_of_rooms
	FROM weeks w
	INNER JOIN [dbo].[reservations] r
		ON r.[check_in_date] >= [Week] AND r.[check_out_date] < DATEADD(DAY, 7, [week])
	GROUP BY [week]
)

-- formatting
SELECT CONCAT(YEAR([week]), '-', DATEPART(WEEK, [week])) AS [week],
       number_of_rooms
FROM counted