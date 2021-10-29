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
