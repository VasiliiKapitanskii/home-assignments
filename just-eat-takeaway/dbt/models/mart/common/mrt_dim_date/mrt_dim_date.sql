{{
    config(
        engine='MergeTree',
        incremental_strategy='delete+insert',
    )
}}


WITH border_dates AS (
    SELECT
        toDate('2000-01-01') AS start_date,
        addYears(today(), 2) AS end_date
)

, dates AS (
    SELECT
        number AS i,
        addDays(b.start_date, number) AS date_input
    FROM border_dates AS b
    -- Use generate_series-like functionality in ClickHouse with `arrayJoin` and `range`
    ARRAY JOIN range(20000) AS number  -- Generates 20000 dates to cover until 2070
    WHERE number < toUInt32(b.end_date - b.start_date)
)

, base AS (
    SELECT
        date_input AS date_id,
        toYear(date_input) AS year_actual,
        toQuarter(date_input) AS quarter_actual,
        toStartOfQuarter(date_input) AS quarter_start,
        toMonth(date_input) AS month_actual,
        formatDateTime(date_input, '%M') AS month_name,
        formatDateTime(date_input, '%b') AS month_name_abbr,
        toStartOfMonth(date_input) AS month_start,
        toWeek(date_input) AS week_of_year,
        toStartOfWeek(date_input, 1) AS week_start,  -- ISO week starts on Monday
        toDayOfMonth(date_input) AS day_of_month,
        CASE toDayOfWeek(date_input) -- ClickHouse `toDayOfWeek` returns 1 for Monday
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
            WHEN 7 THEN 'Sunday'
        END AS day_name,
        toDayOfWeek(date_input) AS day_of_week,
        toDayOfYear(date_input) AS day_of_year
    FROM dates
    {% if is_incremental() %}
        WHERE date_input > (SELECT MAX(date_id) FROM {{ this }})
    {% endif %}
)

{% if is_incremental() %}
    , diff AS (
        SELECT * FROM base
        EXCEPT
        SELECT * EXCEPT (last_updated_at) FROM {{ this }}
    )
{% endif %}

SELECT
    b.*,
    now() AS last_updated_at
FROM base AS b
{% if is_incremental() %}
    WHERE b.date_id IN (
        SELECT d.date_id
        FROM diff AS d
    )
{% endif %}
