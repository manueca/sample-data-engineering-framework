CREATE TABLE staging.food_entries_non_weighted_agg AS
  SELECT food_id,
         Count(user_id) AS food_entry
  FROM   staging.food_entries
  WHERE  entry_date >= '2017-03-01'
         AND entry_date <= '2017-09-01'
  GROUP  BY food_id
