{{
  config(
    materialized='view'
  )
}}


SELECT DISTINCT 
    song,
    artist,
    played_time
FROM
    triplej-398802.triplejsongs.songs
