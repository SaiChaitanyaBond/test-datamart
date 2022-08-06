import os.path


def read_from_mysql(spark, conf, secrets):
    jdbc_params = {"url": get_mysql_jdbc_url(secrets),
                   "lowerBound": "1",
                   "upperBound": "100",
                   "dbtable": conf["dbtable"],
                   "numPartitions": "2",
                   "partitionColumn": ["partition_column"],
                   "user": secrets["mysql_conf"]["username"],
                   "password": secrets["mysql_conf"]["password"]
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
        .option("pem", os.path.abspath(current_dir + "/../" + secrets["sftp_conf"]["pem"])) \
        .option("fileType", "csv") \
        .option("delimiter", "|") \
        .load(conf["directory"] + "/receipts_delta_GBR_14_10_2017.csv")

    return df

def read_from_mongodb(spark, conf):
    address_df = spark \
        .read \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .option("database", conf["mongodb_config"]["database"]) \
        .option("collection", conf["mongodb_config"]["collection"]) \
        .load()
    return address_df

def get_mysql_jdbc_url(mysql_config: dict):
    host = mysql_config["mysql_conf"]["hostname"]
    port = mysql_config["mysql_conf"]["port"]
    database = mysql_config["mysql_conf"]["database"]
    return "jdbc:mysql://{}:{}/{}?autoReconnect=true&useSSL=false".format(host, port, database)
