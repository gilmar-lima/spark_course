from pyspark.sql import SparkSession, Row


def mapper(line):
    fields = line.split(',')
    return Row(Id=int(fields[0]), name=str(fields[1].encode("utf-8")), \
               age=int(fields[2]), numFriends=int(fields[3]))


def main():
    spark = SparkSession.builder.appName("SparkSQL").getOrCreate()    
    spark.sparkContext.setLogLevel("ERROR")

    friendsDataRaw = spark.sparkContext.textFile("../spark-data/fakefriends.csv")

    friendsDataMapped = friendsDataRaw.map(mapper)

    schemaFriends = spark.createDataFrame(friendsDataMapped).cache()
    schemaFriends.createOrReplaceTempView("friends")


    schemaFriends.groupBy("age").count().orderBy("age").show()

    friendsByAge = spark.sql("SELECT age, COUNT(Id) \
                              FROM friends GROUP BY age \
                              ORDER BY age")
    friendsByAge.show()

    spark.stop()


if __name__ == "__main__":
    main()