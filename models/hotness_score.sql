{{
  config(
    materialized='view'
  )
}}


SELECT
    mentions.song,
    mentions.artist,
    SUM(
        EXP(-DATETIME_DIFF(CURRENT_DATE(), mentions.played_time, DAY)
        )
    ) AS hotness_score
    FROM
    {{ref('stg_songs')}} as mentions
GROUP BY
    mentions.song,
    mentions.artist
ORDER BY
    hotness_score DESC
