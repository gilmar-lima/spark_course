from pyspark.sql import SparkSession, Row


def main():
    spark = SparkSession.builder.appName("SparkSQL").getOrCreate()    
    spark.sparkContext.setLogLevel("ERROR")

    schema = "id int, name string, age int, numFriends int"

    firendsDF = (spark.read
                            .option("header", "false")
                            .option("inferSchema", "true")
                            .csv("../spark-data/fakefriends.csv", 
                                    schema=schema))

    firendsDF.printSchema()

    firendsDF.createOrReplaceTempView("friends")

    # group using functions
    firendsDF.groupBy("age").count().orderBy("age").show()

    # group usins select statement
    friendsByAge = spark.sql("SELECT age, COUNT(Id) \
                              FROM friends GROUP BY age \
                              ORDER BY age")
    friendsByAge.show()

    spark.stop()


if __name__ == "__main__":
    main()