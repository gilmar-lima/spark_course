from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField 
from pyspark.sql.types import IntegerType, StringType

                              
FILE_PATH_NAMES = '../spark-data/Marvel-Names.txt'
FILE_PATH_GRAPH = '../spark-data/Marvel-Graph.txt'

def createNames(session : SparkSession) -> None:

    schema = StructType([StructField("id", IntegerType(), True), \
                         StructField("name", StringType(), True)])

    names = session.read.csv(path=FILE_PATH_NAMES, sep=" ", schema=schema)
    names.createTempView("names")
    
    return None


def createConnections(session : SparkSession) -> None:

    lines : DataFrame = session.read.text(paths=FILE_PATH_GRAPH) 

    connectionsDF = (lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0])
                        .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1)
                        .groupBy("id")
                        .agg(func.sum("connections").alias("connections")))

    connectionsDF.createTempView("connections")

    return None


def main() -> None:


    session = SparkSession.builder.appName("LessPopularSuperHero").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")

    createNames(session=session)
    createConnections(session=session)

    result = session.sql('SELECT c.id, c.connections, n.name \
                          FROM connections c\
                          LEFT JOIN names n \
                          ON c.id = n.id \
                          ORDER BY c.connections ASC \
                          LIMIT 10')
    result.show()
    
    session.stop()

if __name__ == '__main__':
    main()
