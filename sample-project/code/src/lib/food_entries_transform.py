"""
Data Test PySpark transformation functions
"""
import pyspark
import utils
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from functools import reduce
from dateutil.relativedelta import relativedelta
from datetime import date, datetime, timedelta
from pyspark.ml.feature import VectorAssembler, OneHotEncoder, StringIndexer, OneHotEncoderEstimator
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.functions import col


def food_entry_agg_func(job_context, df1: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Aggregate the food entries at food id and food entry level based on the
    start date and end date arguments passed and display in the console

    :param: food_entries_df
    :param: start_dt
    :param: end_dt
    """

    food_id_entry_agg = (df1.groupBy("food_id")
                         .agg(expr("count(user_id) as  food_entry")))
    return food_id_entry_agg


def food_entry_weighted_agg_func(job_context,
                                 end_dt,
                                 food_id_entry_filter: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Aggregate the food entries at food id and food entry level based on the
    start date and end date arguments passed and also the weight of different
    food ids based on the number of entries
    and display in the console
    Here decay factor is considered as .1 % for each day passes.

    :param: food_entries_df
    :param: end_dt
    """
    end_dt_plus1 = (datetime.strptime(end_dt, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    decay_factor = .01
    food_id_entry_filter.createOrReplaceTempView('food_id_entry_filter')
    food_id_entry_weighted_agg = job_context.spark.sql("""

                            select  food_id,
                                    sum(food_entry_count) as food_entry,
                                    dense_rank() over(order by sum(weighted_food_entry_count) desc) as weight
                            from(
                                    select food_id,entry_date,substr(timestamp,1,10) as log_dt,
                                    case when (count(user_id)-(DATEDIFF('{0}',substr(timestamp,1,10))*{1}*count(user_id))) <0
                                                then 0
                                        else (count(user_id)-(DATEDIFF('{0}',substr(timestamp,1,10))*{1}*count(user_id))) end
                                                as weighted_food_entry_count,
                                    count(user_id) as food_entry_count
                                    from food_id_entry_filter
                                    group by food_id,entry_date,substr(timestamp,1,10)
                                )
                            group by 1
                          """.format(end_dt_plus1, decay_factor))
    return food_id_entry_weighted_agg


def food_entry_prediction(job_context,
                          log_dt,
                          entry_date,
                          food_id,
                          food_id_entry_filter: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Function to predict the number of food entries for a food id for specific date .
    In this implementation Logistic regression model has been used along with
    string indexer and OneHotEncoderEstimator to do indexing and assembling

    :param: food_entries_df
    :param: end_dt
    """

    food_id_entry_filter.createOrReplaceTempView('food_id_entry_filter')
    train = job_context.spark.sql("""
          select substring(timestamp,1,10) as log_dt,
                 entry_date,
                 food_id,
                 count(*) as food_entry
          from food_id_entry_filter
          group by 1,2,3
    """)
    #train, test = df2.randomSplit([0.7, 0.3], seed=7)
    #  below values can  be used for Testing
    log_dt = "2017-08-11"
    entry_date = "2017-08-11"
    food_id = "49831"

    num_cols = ["food_entry"]
    cat_cols = ["log_dt", "entry_date", "food_id"]
    """
     One hot encoder is a transformer which will transform the string columns
     to  index feature.
    """

    string_indexer = [
        StringIndexer(inputCol=x, outputCol=x+"_StringIndexer", handleInvalid="skip")
        for x in cat_cols
    ]
    one_hot_encoder = [
        OneHotEncoderEstimator(
            inputCols=[f"{x}_StringIndexer" for x in cat_cols],
            outputCols=[f"{x}_OneHotEncoder" for x in cat_cols],
        )
    ]
    # Assembler is used to assemble the hot encoded indexed vectors.
    #assembler_input = [x for x in num_cols]
    assembler_input = [f"{x}_OneHotEncoder" for x in cat_cols]
    assembler1 = VectorAssembler(inputCols=assembler_input, outputCol="vector_assember_features")
    # stages are defined for a pipeline.
    stages = []
    stages += string_indexer
    stages += one_hot_encoder
    stages += [assembler1]

    pipeline = Pipeline(stages=stages)

    ppln = pipeline.fit(train)
    pp_df = ppln.transform(train)
    pp_df.createOrReplaceTempView('pp_df')

    data = pp_df.select(col("vector_assember_features").alias(
        "features"), col("food_id"), col("food_entry").alias("label"))
    # defining logistic regression model and fitting using training or known data.
    lr = LogisticRegression(maxIter=10, regParam=0.001)
    model = lr.fit(data)

    test = job_context.spark.createDataFrame([
        (log_dt, entry_date, food_id, 100)
    ], ["log_dt", "entry_date", "food_id", "food_entry"])

    test_pp_df = ppln.transform(test)
    test_data = test_pp_df.select(col("vector_assember_features").alias(
        "features"), col("food_id"), col("food_entry").alias("label"))

    # Predicting using unknown data

    prediction_df = model.transform(test_data)
    prediction_df.select("prediction", "label", "features", "food_id").show(5)
    return prediction_df
