from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml import Transformer


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
    testTransformed = transformDataset(test)

    decisionTree = DecisionTreeRegressor(labelCol='PriceOfUnitArea')
    model : Transformer = decisionTree.fit(trainTransformed)

    predictions = model.transform(testTransformed)
    predictions.show()

    spark.stop()

    return None



if __name__ == '__main__' :
    main()



