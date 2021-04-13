from pyspark.sql.session import SparkSession
from pyspark.sql.types import Row

if __name__ == '__main__':
    spark = SparkSession.builder.appName("test[*]").getOrCreate()
    print("Spark Init:", spark.version)

    data = [Row(name="abc", id=1)]
    df = spark.createDataFrame(data)
    df.show()

    print(f"count: {df.count()}")
