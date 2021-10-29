CREATE OR replace TABLE staging.food_entries_weighted_agg AS
SELECT   food_id,
         Sum(food_entry_count)     AS food_entry,
         Dense_rank() OVER(ORDER BY Sum(weighted_food_entry_count) DESC) AS weight
FROM    ( SELECT           food_id,
                           entry_date,
                           Substr(timestamp,1,10) AS log_dt,
                           CASE
                                    WHEN (Count(user_id)-Datediff(day,Substr(timestamp,1,10),'2017-09-02')*(.01*Count(user_id))) < 0 THEN 0
                                    ELSE (Count(user_id)-Datediff(day,Substr(timestamp,1,10),'2017-09-02')*(.01*Count(user_id)))
                           END               AS weighted_food_entry_count,
                           Count(entry_meal) AS food_entry_count
                  FROM     staging.food_entries
                  WHERE    entry_date >='2017-03-01'
                  AND      entry_date <='2017-09-01'
                  AND      Datediff(day,Substr(timestamp,1,10),'2017-09-03')>0
                  GROUP BY food_id,
                           entry_date,
                           Substr(timestamp,1,10) )
GROUP BY 1
