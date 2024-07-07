--LAST VERSION
  --table with filtered and formatted columns that will be needed for calculating marketing compaigns performance
WITH
  raw_events_filtered AS (
  SELECT
    DISTINCT PARSE_DATE('%Y%m%d', event_date) AS event_date,
    TIMESTAMP(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S UTC', TIMESTAMP_MICROS(event_timestamp))) AS event_datetime,
    event_name,
    user_pseudo_id AS user_id,
    category,
    country,
    purchase_revenue_in_usd AS revenue_usd,
    total_item_quantity,
    campaign
  FROM
    `turing_data_analytics.raw_events`),
  --
  --adding a column to get the time of previous event for every event
  sorted_events AS (
  SELECT
    *,
    LAG(event_datetime,1) OVER (PARTITION BY user_id ORDER BY event_datetime) AS prev_event_time
  FROM
    raw_events_filtered),
  --
  --add columns with boolean value if an event is a new session
  sessions AS (
  SELECT
    *,
  IF
    (TIMESTAMP_DIFF(event_datetime, prev_event_time, MINUTE) > 30
      OR prev_event_time IS NULL, 1, 0) AS is_new_session
  FROM
    sorted_events),
  ---
  --counting sessions ids for every user
  session_ids AS (
  SELECT
    *,
    SUM(is_new_session) OVER (PARTITION BY user_id ORDER BY event_datetime) AS session_id_user,
    SUM(is_new_session) OVER (ORDER BY user_id, event_datetime) AS session_id_global
  FROM
    sessions),
  --
  --calculating session start and end points
  session_start_end AS (
  SELECT
    *,
    MIN(event_datetime) OVER (PARTITION BY user_id, session_id_user) AS session_start,
    MAX(event_datetime) OVER (PARTITION BY user_id, session_id_user) AS session_end
  FROM
    session_ids),
  --
  --table with all info about sessions (events, campaigns, country, device)
  session_data AS (
  SELECT
    session_id_global,
    user_id,
    session_id_user,
    MAX(event_date) AS session_date,
    country,
    MAX(category) AS category,
    ROUND(TIMESTAMP_DIFF(session_end, session_start, SECOND) / 60, 2) AS session_duration_minute,
    TIMESTAMP_DIFF(session_end, session_start, SECOND) AS session_duration_seconds,
    SUM(revenue_usd) AS revenue,
    SUM(CASE
        WHEN event_name = 'purchase' THEN total_item_quantity
        ELSE 0
    END
      ) AS quantity,
    COUNTIF(event_name IN ('page_view',
        'user_engagement',
        'scroll')) AS c_user_activity,
    COUNTIF(event_name = 'view_item') AS c_view_item,
    COUNTIF(event_name = 'view_promotion') AS c_view_promotion,
    COUNTIF(event_name = 'select_promotion') AS c_select_promotion,
    COUNTIF(event_name = 'select_item') AS c_select_item,
    COUNTIF(event_name = 'add_to_cart') AS c_add_to_cart,
    COUNTIF(event_name = 'begin_checkout') AS c_begin_checkout,
    COUNTIF(event_name = 'add_shipping_info') AS c_add_shipping,
    COUNTIF(event_name = 'add_payment_info') AS c_payment_info,
    COUNTIF(event_name = 'purchase') AS count_purchase,
    COUNT(DISTINCT campaign) AS count_campaign,
    MAX(CASE
        WHEN campaign = '(referral)' THEN 1
        ELSE 0
    END
      ) AS referral,
    MAX(CASE
        WHEN campaign = '<Other>' THEN 1
        ELSE 0
    END
      ) AS other,
    MAX(CASE
        WHEN campaign = '(organic)' THEN 1
        ELSE 0
    END
      ) AS organic,
    MAX(CASE
        WHEN campaign = '(direct)' THEN 1
        ELSE 0
    END
      ) AS direct,
    MAX(CASE
        WHEN campaign = 'Data Share Promo' THEN 1
        ELSE 0
    END
      ) AS data_share,
    MAX(CASE
        WHEN campaign IN ('NewYear_V1', 'NewYear_V2') THEN 1
        ELSE 0
    END
      ) AS NewYear,
    MAX(CASE
        WHEN campaign IN ('BlackFriday_V1', 'BlackFriday_V2') THEN 1
        ELSE 0
    END
      ) AS BlackFriday,
    MAX(CASE
        WHEN campaign IN ('Holiday_V1', 'Holiday_V2') THEN 1
        ELSE 0
    END
      ) AS Holiday,
    MAX(CASE
        WHEN campaign = '(data deleted)' THEN 1
        ELSE 0
    END
      ) AS marketing_all,
    CASE
      WHEN SUM(CASE
        WHEN campaign IN ('Holiday_V2',
        'Holiday_V1',
        'Data Share Promo',
        'NewYear_V1',
        'NewYear_V2',
        'BlackFriday_V1',
        'BlackFriday_V2',
        '(data deleted)') THEN 1
        ELSE 0
    END
      ) >= 1 THEN 1
      ELSE 0
  END
    AS marketing_campaign,
    CASE
      WHEN SUM(CASE
        WHEN campaign IN ('(referral)',
        '(organic)',
        '(direct)',
        '<Other>') THEN 1
        ELSE 0
    END
      ) >= 1 THEN 1
      ELSE 0
  END
    AS non_marketing
  FROM
    session_start_end
  GROUP BY
    ALL)
  --
  --addd column with session status (bounced or not) and whether it is associated with a marketing campaign
SELECT
  CASE
    WHEN (session_duration_seconds <= 10 AND revenue IS NULL) THEN 'bounced'
    ELSE 'engaged'
END
  AS session_status,
  CASE
    WHEN (marketing_campaign=1 AND non_marketing=0) THEN 'marketing session'
    WHEN (marketing_campaign=1
    AND non_marketing=1) THEN 'mixed session'
    ELSE 'no campaign session'
END
  AS campaign_column,
  *
FROM
  session_data
