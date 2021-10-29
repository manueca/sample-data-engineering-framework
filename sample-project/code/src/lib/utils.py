"""
Methods that can be resused across jobs are coded in utils.py
"""


import argparse
from datetime import datetime, date
from logging import Logger
import pyspark
import job_context as JobContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import subprocess


def csv_load(job_context, filename):
    """
    This function is to read the CSV files with header enabled

    :param: job_context
    :param: filename
    """
    df = job_context.spark.read.csv(filename, header=True)

    return df


def hdfs_write(job_context, df: pyspark.sql.DataFrame,

               hdfs_target: str, s3_target: str):
    """
    This function is to  Write to HDFS and delete the s3 target location subsequently

    :param: job_context
    :param: hdfs_target
    :param: s3_target
    """

    job_context.logger.info("Starting write to HDFS")
    hdfs_target = hdfs_target if hdfs_target[-1:] == '/' else hdfs_target + '/'
    df.write.format("parquet").save(hdfs_target, mode='overwrite')
    job_context.logger.info("Removing s3 path")

    run_bash('aws s3 rm ' + s3_target + ' --recursive --page-size 800')
    # job_context.logger.info('aws s3 rm ' + s3_target + path +
    #                        '/' + ' --recursive --page-size 800')


def run_bash(bash_command):
    """
    This function is to execute a bash command

    :param: bash_command
    """
    try:
        sp = subprocess.Popen(bash_command, shell=True,
                              stdout=subprocess.PIPE, cwd=None).stdout.read().decode("utf-8").strip('\n').split('\n')
    except Exception as e:
        raise e
        exit(1)
    return sp

def trim_string_values(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Trim string columns
    :param df: pyspark.sql.DataFrame
    :return: pyspark.sql.DataFrame
    """
    for col in df.dtypes:
        if col[1].startswith("string"):
            df = df.withColumn("{}".format(col[0]), trim(df[col[0]]))

    return df

def createColumnWithoutMapping(df1: pyspark.sql.DataFrame, df2: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Creates a column if not present in df2 with reference to df1 when two dataframes have the same column names.
    :param df1: dataframe which is a reference
    :param df2: dataframe in which column is to be created
    :return: new dataframe with added columns.
    """

    df1list = []
    df1list = list(set(df1.columns) - set(df2.columns))
    df1Dict = dict()
    df1Dict = getNameTypeDict(df1.dtypes)
    for col in df1list:
        df2 = df2.withColumn(col, lit(None).cast(df1Dict[col]))
    return df2


def getNameTypeDict(schemalist):
    nameType = dict()
    for x in schemalist:
        nameType[x[0]] = x[1]
    return nameType

def convert_date_to_string(df, date_list: list = None):
    """
    :param : Converts date types to string either for all dates or for given list of
             date columns and also converts the columns to lowercase by default
    :return: Dataframe with dates to string
    """
    df = column_names_to_lower(df)
    if date_list is None:
        for col in df.dtypes:
            if (col[1].startswith("date")):
                df = df.withColumn("{}".format(col[0]), df[col[0]].cast("string"))
    else:
        date_list = [x.lower() for x in date_list]
        for col in df.dtypes:
            if (col[1].startswith("date")) and (col[0] in date_list):
                df = df.withColumn("{}".format(col[0]), df[col[0]].cast("string"))
    return df

def convert_decimal_to_double(df, decimal_list: list = None):
    """
    :param : Converts decimal types to double either for all decimals or for given list of
             decimal columns and also converts the columns to lowercase by default
    :return: Dataframe with decimal to double
    """
    df = column_names_to_lower(df)
    if decimal_list is None:
        for col in df.dtypes:
            if (col[1].startswith("decimal")):
                df = df.withColumn("{}".format(col[0]), df[col[0]].cast("double"))
    else:
        decimal_list = [x.lower() for x in decimal_list]
        for col in df.dtypes:
            if (col[1].startswith("decimal")) and (col[0] in decimal_list):
                df = df.withColumn("{}".format(col[0]), df[col[0]].cast("double"))
    return df

def select_relevant_snapshot(df: pyspark.sql.DataFrame, snapshot_col_name: str, etl_snapshot_date: date) -> date:
    """
    Select the snapshot to use for ETL based on the etl_snapshot_date arg
    :param df: DataFrame to find relevant snapshot within
    :param snapshot_col_name: The Snapshot column for df
    :param etl_snapshot_date: The as-of date that the ETL is being run for
    :return:
    """

    min_snapshot_date = df.select(min(snapshot_col_name)).collect()[0][0]

    # if the ETL snapshot date is before the earliest snapshot, use the earliest snapshot
    if min_snapshot_date > etl_snapshot_date:
        datasource_snapshot_date = min_snapshot_date
    # otherwise use the max snapshot that is less than or equal to the ETL snapshot date
    else:
        datasource_snapshot_date = (df
                                    .select(col(snapshot_col_name))
                                    .distinct()
                                    .filter(col(snapshot_col_name) <= lit(etl_snapshot_date))
                                    .select(max(snapshot_col_name))
                                    ).collect()[0][0]

    return datasource_snapshot_date

