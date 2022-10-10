from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import IntegerType, FloatType
                              
FILE_PATH = '../spark-data/customer-orders.csv'

def main() -> None:


    session = SparkSession.builder.appName("MinTemperatures").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")

    schema = StructType([StructField('customer_id', IntegerType(), True),\
                         StructField('order_id', IntegerType(), True),\
                         StructField('amount', FloatType(), True)])

    data = session.read.csv(path=FILE_PATH, schema=schema)

    data.createOrReplaceTempView("customer_orders")

    orders_aggregate = session.sql('SELECT customer_id, \
                                    ROUND(SUM(amount),2) as total_orders \
                                    FROM customer_orders \
                                    GROUP BY customer_id \
                                    ORDER BY customer_id')
    
    orders_aggregate.show()
    
    session.stop()


if __name__ == '__main__':
    main()
