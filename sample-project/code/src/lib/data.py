"""
Methods that connect to specific data sources
Minimal transforms should be performed here because:
a) the majority of transformations should be reusable and located in transforms.py
b) some ETL jobs may want the transformation applied and some may not, so transformations applied in data.py are ones
that should be universally applied
"""
from dateutil.relativedelta import *
import pyspark
from job_context import JobContext
from pyspark.sql.functions import expr, col, when, concat, lit
import dateutil.relativedelta
from datetime import date, datetime, timedelta
import subprocess
import sys


def read_food_entries(job_context: JobContext) -> pyspark.sql.DataFrame:
    """
    :param job_context: JobContext accommodates use of many objects/properties frequently accessed for ETL
    (SparkSession, logger, creds, variable)
    :return:
    """
    df1 = job_context.spark.read.csv(job_context.s3_read_prefix + "food_entries/", header=True)
    return df1
