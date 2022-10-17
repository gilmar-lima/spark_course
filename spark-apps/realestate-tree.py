from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import DecisionTreeRegressor


FILE_PATH = '../spark-data/realestate.csv'

def loadDataset(session : SparkSession) -> DataFrame :

    df = session.read.csv(path=FILE_PATH, inferSchema=True, header=True)

    return df

def transformDataset(df : DataFrame) -> DataFrame :

    assembler = VectorAssembler(inputCols=['TransactionDate','HouseAge',
                                            'DistanceToMRT','NumberConvenienceStores',
                                            'Latitude','Longitude'],
                                outputCol='features')
    return assembler.transform(df).select('PriceOfUnitArea', 'features')


def main() -> None :
    
    spark = SparkSession.builder.appName("RealEstate").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = loadDataset(session=spark)
    dfSplitted = df.randomSplit([0.8, 0.2])

    train = dfSplitted[0]
    test = dfSplitted[1]

    trainTransformed = transformDataset(train)
    trainTransformed.show()

    model = DecisionTreeRegressor(labelCol='PriceOfUnitArea')

    return None



if __name__ == '__main__' :
    main()



