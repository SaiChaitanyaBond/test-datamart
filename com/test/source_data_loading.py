from pyspark.sql import SparkSession
import yaml
import os.path
from com.utility import *

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "mysql:mysql-connector-java:8.0.15" pyspark-shell'
    )

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise applications") \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

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

        elif src == 'CP':
            print()

        elif src == 'OL':
            print()

# spark-submit --packages "mysql:mysql-connector-java:8.0.15" dataframe/ingestion/others/systems/mysql_df.py
