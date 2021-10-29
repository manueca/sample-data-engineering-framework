"""
JobContext
Creating spark session with common packages and seettings added which can be used 
across multiple jobs .
"""

import argparse
from datetime import datetime, date
from logging import Logger
from time import sleep
import boto3
from urllib.parse import urlparse

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import subprocess


class JobContext(object):
    """
    A JobContext provides a convenient way to use many objects/properties that need to be accessed frequently in an
    ETL workflow, e.g. SparkSession, logger, credentials, variables
    I am not using snowflake for this usecase, but added snowflake jars in spark context to demonstrate
    how to configure a generic package that can be used across.
    """

    def get_spark(self, app_name: str) -> pyspark.sql.SparkSession:
        """Spark session with minimal config applied
        legacy parquet format: https://mapr.com/support/s/article/Issue-when-reading-Parquet-data-in-Spark
        :param app_name:
        :return:
        """
        spark = (SparkSession
                 .builder
                 .appName(app_name)
                 .config("spark.jars.packages",
                         "net.snowflake:snowflake-jdbc:3.6.5,net.snowflake:spark-snowflake_2.11:2.3.2")
                 .enableHiveSupport()
                 .getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        return spark

    def get_logger(self, spark: SparkSession) -> Logger:
        """
        Create  logger and set to level ALL
        Spark log level will already have been set, so setting level for this logger to ALL overrides
        for just this logger only
        :param spark:
        :return: log4j logger
        """
        sc = spark.sparkContext
        # noinspection PyProtectedMember
        logger = sc._jvm.org.apache.log4j.LogManager.getLogger("UA")
        # noinspection PyProtectedMember
        logger.setLevel(sc._jvm.org.apache.log4j.Level.ALL)
        logger.info("UA Logger started.")
        return logger

    def __init__(self, app_name, environment, etl_snapshot_date):
        self.app_name = app_name
        self.environment = environment
        self.spark = self.get_spark("{}_{}_{}".format(app_name, environment, etl_snapshot_date))
        self.logger = self.get_logger(self.spark)
        self.etl_snapshot_date = etl_snapshot_date
        input_bucket = "s3://zz-testing/jcher2"
        if environment == "dev":
            self.s3_read_prefix = "{}-dev/rawdata/".format(input_bucket)
            self.s3_aggregated_prefix = "{}-dev/aggregated/".format(input_bucket)
            self.hive_db = "dev_test"
        elif environment == "qa":
            self.s3_read_prefix = "{}-qa/rawdata/".format(input_bucket)
            self.s3_aggregated_prefix = "{}-qa/aggregated/".format(input_bucket)
            self.hive_db = "qa_test"
        elif environment == "prod":
            self.s3_read_prefix = "{}/rawdata/".format(input_bucket)
            self.s3_aggregated_prefix = "{}/aggregated/".format(input_bucket)
            self.hive_db = "test"
        else:
            raise NotImplementedError("Invalid environment specified!")

    def __enter__(self):
        self.logger.info(
            "Executing {} {} for snapshot date {}...".format(self.environment, self.app_name, self.etl_snapshot_date))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.spark.stop()
            self.logger.info(
                "Terminating run {} {} for snapshot date {} with ERROR {}: {}".format(self.environment, self.app_name,
                                                                                      self.etl_snapshot_date,
                                                                                      exc_type.__name__, exc_val))
        else:
            self.spark.stop()
            self.logger.info(
                "Finished run {} {} for snapshot date {}".format(self.environment, self.app_name,
                                                                 self.etl_snapshot_date))
