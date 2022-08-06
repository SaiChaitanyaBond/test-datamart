from pyspark.sql import SparkSession
import yaml
import os.path
from com.utility import *

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "mysql:mysql-connector-java:8.0.15" pyspark-shell'
    )

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise applications") \
        .config("spark.mongodb.input.uri", app_secret["mongodb_config"]["uri"]) \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    staging_loc = app_conf['staging_loc']

    src_list = app_conf['src_list']

    for src in src_list:
        src_conf = app_conf[src]

        if src == 'SB':
            reciept_df = read_from_mysql(spark, src_conf['mysql_conf'], app_secret['mysql_conf'])
            reciept_df = reciept_df.withColumn('ins_date', current_date())

            reciept_df.show()

            reciept_df \
                .write \
                .partitionBy("ins_date") \
                .mode("overwrite") \
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + '/' + staging_loc + "/" + src)

        elif src == 'OL':
            reciept_df = read_from_sftp(spark, src_conf['sftp_conf'], app_secret['sftp_conf'])
            reciept_df = reciept_df.withColumn('ins_date', current_date())

            reciept_df.show()

            reciept_df \
                .write \
                .partitionBy("ins_date") \
                .mode("overwrite") \
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + '/' + staging_loc + "/" + src)

        elif src == 'ADDR':
            address_df = read_from_mongodb(spark, src_conf["mongodb_config"])
            address_df = address_df.withColumn('ins_date', current_date())

            address_df.show()

            address_df \
                .write \
                .partitionBy("ins_date") \
                .mode("overwrite") \
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + '/' + staging_loc + "/" + src)

        elif src == 'CP':
            cp_df = spark.read \
                .option("header", "true") \
                .option("delimiter", "|") \
                .format("csv") \
                .load("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/KC_Extract_1_20171009.csv")
            cp_df = cp_df.withColumn('ins_date', current_date())

            cp_df \
                .write \
                .partitionBy("ins_date") \
                .mode("overwrite") \
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + '/' + staging_loc + "/" + src)

# spark-submit --packages "mysql:mysql-connector-java:8.0.15" dataframe/ingestion/others/systems/mysql_df.py
