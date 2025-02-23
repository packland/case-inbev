from pyspark.sql import SparkSession
import random

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Spark Pi").getOrCreate()

    def inside(p):
        x, y = random.random(), random.random()
        return x*x + y*y < 1

    partitions = 2
    n = 100000 * partitions
    count = spark.sparkContext.parallelize(range(1, n + 1), partitions).filter(inside).count()
    print("Pi is roughly %f" % (4.0 * count / n))

    spark.stop()