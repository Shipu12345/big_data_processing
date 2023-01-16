from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


class SparkCSVConcater:
    def __init__(self) -> None:
        conf = SparkConf().setAppName("csv_concatenation")
        sc = SparkContext(conf=conf)
        self.spark = SparkSession(sc)

    def concat_multiple_csvs(self, files: list):
        dataframes = []
        for file in files:
            df = self.spark.read.csv(file, header=True, inferSchema=True)
            dataframes.append(df)

        result = dataframes[0]
        for df in dataframes[1:]:
            result = result.union(df)
        result.write.csv("data/sample_large_dataset.csv", header=True)
