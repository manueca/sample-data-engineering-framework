

# Sample Data Engineering project 

**A lightweight pyspark  project  which can demonstrate how the code can be organised in a scalable and maintainable way.
---
## Table of Contents


* [Official components for some of the most used frameworks](#Official-components-for-some-of-the-most-used-frameworks)
  * [pyspark](#http://spark.apache.org/docs/latest/api/python/)

### **_Usage_**

Code is driven from main scripts available in ETL folder. 


```python
"""
Purpose: Building aggregate based on the food entry data set.
Author : Jerry Cheruvathoor.

"""
from job_context import JobContext
import data
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import food_entries_transform
import sys
import json
import subprocess
import utils


def main(args_dict):
    """
    :param args_dict: ETL args
    :return: None
    """
    print(params_dict['environment'])
    print(params_dict['dag_exec_dt'])
    job_context = JobContext("food_entry_analysis",
                             params_dict['environment'], params_dict['dag_exec_dt'])
    start_dt = params_dict.get('start_dt')
    end_dt = params_dict.get('end_dt')
    hdfs_target_food_entries_agg = '/data/food_entries_agg/'
    hdfs_target_food_entries_weighted_agg = '/data/food_entries_weighted_agg/'
    s3_target_food_entries_agg = job_context.s3_aggregated_prefix + "food_entries_agg/"
    s3_target_food_entries_weighted_agg = job_context.s3_aggregated_prefix + "food_entries_weighted_agg/"

    print('start_dt  is : ', start_dt)
    print('end_dt is : ', end_dt)
    print('s3_target_food_entries_weighted_agg is : ', s3_target_food_entries_weighted_agg)
    print('s3_target_food_entries_agg is : ', s3_target_food_entries_agg)
    food_entry_df = data.read_food_entries(job_context)
    food_id_entry_filter = (food_entry_df.filter(col('entry_date') >= start_dt)
                            .filter(col('entry_date') <= end_dt))
    # Caching food_id_entry_filter dataframe as this dataframe has been used more than once.
    food_id_entry_filter.cache()
    food_id_entry_filter.show()
    """
    Functions food_entry_agg_func and food_entry_weighted_agg_func are called
    to do the aggregation  with both weighted and non weighted counts.
    """
    food_entries_agg_df = food_entries_transform.food_entry_agg_func(
        job_context, food_id_entry_filter)
    food_entries_weighted_agg_df = food_entries_transform.food_entry_weighted_agg_func(
        job_context, end_dt, food_id_entry_filter)

    """
    Writting data to hdfs to tap to the IO capacity of HDFS as we are bounded on
    IOs with S3. This will help to achieve more parallelism and also helps to avoid
    s3 consistency issue.
    S3 distcp method is recommended by AWS  . If we run through airflow,
    we can perform s3 distcp to copy the data  to s3.
    """
    utils.hdfs_write(job_context, food_entries_weighted_agg_df.coalesce(1),
                     hdfs_target_food_entries_weighted_agg, s3_target_food_entries_weighted_agg)
    utils.hdfs_write(job_context, food_entries_agg_df.coalesce(1),
                     hdfs_target_food_entries_agg, s3_target_food_entries_agg)

    job_context.spark.stop()
    print("s3_target_food_entries_agg is ", s3_target_food_entries_agg)
    utils.run_bash('hadoop distcp -D mapreduce.map.memory.mb=50480 -m 80 {0} {1}'
                   .format(hdfs_target_food_entries_weighted_agg + "*", s3_target_food_entries_weighted_agg))

    utils.run_bash('hadoop distcp -D mapreduce.map.memory.mb=50480 -m 80 {0} {1}'
                   .format(hdfs_target_food_entries_agg + "*", s3_target_food_entries_agg))


if __name__ == "__main__":
    params = ' '.join(sys.argv[1:])
    print(params)

    argv = {"environment": "dev",
            "dag_exec_dt": "2021-01-22",
            "start_dt": "2017-03-01",
            "end_dt": "2017-09-01"
            }
    params = json.dumps(argv)

    try:
        params_dict = json.loads(params)
    except ValueError:
        raise Exception("Input parameter is NOT a valid JSON string: " + params)
    print(params_dict)

    main(params_dict)

```

