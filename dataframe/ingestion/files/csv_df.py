from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, BooleanType,DoubleType
import os.path
import yaml

if __name__ == '__main__':
    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read Files") \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.4') \
        .enableHiveSupport()\
        .getOrCreate()
        # .master('local[*]') \
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    print("\nCreating dataframe ingestion CSV file using 'SparkSession.read.format()'")

    fin_schema = StructType() \
        .add("id", IntegerType(), True) \
        .add("has_debt", BooleanType(), True) \
        .add("has_financial_dependents", BooleanType(), True) \
        .add("has_student_loans", BooleanType(), True) \
        .add("income", DoubleType(), True)

    fin_df = spark.read \
        .option("header", "false") \
        .option("delimiter", ",") \
        .format("csv") \
        .schema(fin_schema) \
        .load("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/finances.csv")

    fin_df.printSchema()
    fin_df.show()

    print("Creating dataframe ingestion CSV file using 'SparkSession.read.csv()',")

    finance_df = spark.read \
        .option("mode", "DROPMALFORMED") \
        .option("header", "false") \
        .option("delimiter", ",") \
        .option("inferSchema", "true") \
        .csv("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/finances.csv") \
        .toDF("id", "has_debt", "has_financial_dependents", "has_student_loans", "income")

    print("Number of partitions = " + str(fin_df.rdd.getNumPartitions()))
    finance_df.printSchema()
    finance_df.show()

    finance_df \
        .repartition(2) \
        .write \
        .partitionBy("id") \
        .mode("overwrite") \
        .option("header", "true") \
        .option("delimiter", "~") \
        .csv("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/fin")

    finance_df.createOrReplaceTempView("myTempTable")

   # spark.sql("CREATE TABLE IF NOT EXISTS src (id INT, income STRING) USING hive")
    print("Hive results")
    spark.sql("CREATE TABLE IF NOT EXISTS mytable AS SELECT * FROM myTempTable ")
    #print(type(myTempTable))
    spark.sql("SELECT * FROM mytable").show()
    spark.sql("SELECT COUNT(*) FROM mytable").show()
    spark.sql("SELECT MAX(income) FROM mytable").show()
    set_dynamic_mode = "SET hive.exec.dynamic.partition.mode = nonstrict"
    spark.sql(set_dynamic_mode)
    create_partitioned_sql = "CREATE TABLE IF NOT EXISTS abc_part (income INT) PARTITIONED BY (id INT)"
    spark.sql(create_partitioned_sql)
    insert_sql = "INSERT INTO abc_part PARTITION (id) SELECT id,income FROM mytable"
    spark.sql(insert_sql)
    spark.sql("show partitions abc_part").show()
    spark.sql("CREATE TABLE  IF NOT EXISTS abc_par (id INT, income INT) STORED AS PARQUET")
    spark.sql("INSERT INTO abc_par  SELECT id,income FROM mytable")
    spark.sql("SELECT COUNT(*) FROM abc_par").show()
    spark.sql("SELECT * FROM abc_par").show()
    spark.sql("DESCRIBE TABLE abc_part").show()
    spark.sql("DESCRIBE TABLE abc_par").show()

    spark.sql("SELECT * FROM students").show()
    print("student info")
    # student_df = spark.sql("SELECT * FROM student_info")
    # student_df.show()
    spark.stop()

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" dataframe/ingestion/files/csv_df.py
