from pyspark.sql import SparkSession
from pyspark.sql.functions import first,trim,lower,ltrim,initcap,format_string,coalesce,lit,col
from model.Person import Person

if __name__ == '__main__':
    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("DSL examples") \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    people_df = spark.createDataFrame([
        Person("Sidhartha", "Ray", 32, None, "Programmer"),
        Person("Pratik", "Solanki", 22, 176.7, None),
        Person("Ashok ", "Pradhan", 62, None, None),
        Person(" ashok", "Pradhan", 42, 125.3, "Chemical Engineer"),
        Person("Pratik", "Solanki", 22, 222.2, "Teacher")
    ])

    people_df.show()
    people_df.createOrReplaceTempView("people_view")
    # spark.sql("SELECT * FROM people_view").show()



    spark.sql("SELECT firstName,WeightInLbs from "+
              " (SELECT *, row_number() OVER (PARTITION BY firstName ORDER BY weightInLbs) as rowNum " +
              " FROM people_view) tmp where rowNum =1").show()

    spark.sql("SELECT firstName,WeightInLbs from " +
              " (SELECT *, row_number() OVER (PARTITION BY trim(lower(firstName)) ORDER BY weightInLbs) as rowNum " +
              " FROM people_view) tmp where rowNum =1").show()

    # spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" dataframe/curation/sql/more_functions.py