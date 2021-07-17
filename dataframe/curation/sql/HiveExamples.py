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

    student_df = spark.sql("SELECT * FROM student_info")
    student_df.show()
    student_df.printSchema()

    student_df.createOrReplaceTempView("studentsView")

    agg_students_df = spark.sql("""
            select
                fname,
                sum(age) as TotalAge,
                count(age) as Count,
                max(age) as MaxAge,
                min(age) as MinAge
            from
                studentsView
            group by
                fname
            """)

    agg_students_df.show()
    spark.stop()

    # spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" dataframe/curation/sql/HiveExamples.py
