
CREATE OR REPLACE EXTERNAL TABLE staging.food_entries
(

    timestamp varchar AS (value: c1 :: varchar),
    entry_date varchar AS (value: c2 :: varchar),
    entry_meal varchar AS (value: c3 :: varchar),
    user_id varchar AS (value: c4 :: varchar),
    food_id varchar AS (value: c5 :: varchar)

)

LOCATION = @jerry_dev_stg/rawdata/food_entries/
auto_refresh = TRUE

file_format = (TYPE=csv skip_header = 1)

;
