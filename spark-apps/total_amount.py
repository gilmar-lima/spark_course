from pyspark import SparkConf, SparkContext, RDD
from operator import add

FILE_PATH = '/home/gilmar/Documents/repos/spark_course/spark-data/customer-orders.csv'

def parse_line(line: str) -> tuple[int, float]:

    fields = line.split(',')
    id = int(fields[0])
    amount = float(fields[2])
    return (id, amount)


def printer(data: RDD) -> None:

    for record in data.collect():
        print(f'Id : {record[0]} total: {record[1]}')

    return None


def main() -> None:

    conf = SparkConf().setMaster("local").setAppName("total_amount")

    sc = SparkContext(conf = conf)
    sc.setLogLevel('ERROR')
    
    data = sc.textFile(FILE_PATH)
    total_data = (data
                    .map(parse_line)
                    .reduceByKey(add)
                    .sortBy(lambda x : x[1], ascending=False))
    printer(total_data)
    
    return None


if __name__ == "__main__":
    main()
