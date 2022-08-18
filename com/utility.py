import os.path


def read_from_mysql(spark, conf, secrets):
    jdbc_params = {"url": get_mysql_jdbc_url(secrets),
                   "lowerBound": "1",
                   "upperBound": "100",
                   "dbtable": conf["dbtable"],
                   "numPartitions": "2",
                   "partitionColumn": conf["partition_column"],
                   "user": secrets["username"],
                   "password": secrets["password"]
                   }

    df = spark \
        .read.format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .options(**jdbc_params) \
        .load()

    return df

def read_from_sftp(spark, conf, secrets):
    current_dir = os.path.abspath(os.path.dirname(__file__))

    df = spark.read \
        .format("com.springml.spark.sftp") \
        .option("host", secrets["hostname"]) \
        .option("port", secrets["port"]) \
        .option("username", secrets["username"]) \
        .option("pem", os.path.abspath(current_dir + "/../../" + secrets["pem"])) \
        .option("fileType", "csv") \
        .option("delimiter", "|") \
        .load(conf["directory"] + '/receipts_delta_GBR_14_10_2017.csv')

    return df

def read_from_mongodb(spark, conf):
    address_df = spark \
        .read \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .option("database", conf["database"]) \
        .option("collection", conf["collection"]) \
        .load()
    return address_df

def get_mysql_jdbc_url(mysql_config):
    host = mysql_config["hostname"]
    port = mysql_config["port"]
    database = mysql_config["database"]
    return "jdbc:mysql://{}:{}/{}?autoReconnect=true&useSSL=false".format(host, port, database)
