from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pprint import pprint
import os


class SparkCSVConcater:
    def __init__(self) -> None:
        conf = SparkConf().setAppName("csv_concatenation")
        sc = SparkContext(conf=conf)
        self.spark = SparkSession(sc)
        self.target_path = "data/sample_large_dataset"

    def concat_multiple_csvs(self, files: list) -> None:
        dataframes = []
        for file in files:
            df = self.spark.read.csv(file, header=True, inferSchema=True)
            dataframes.append(df)

        result = dataframes[0]
        for df in dataframes[1:]:
            result = result.union(df)
        
        result.write.csv(self.target_path, header=True)
        return result

    def load_data(self, files: list) -> None:

        if not os.path.exists(self.target_path):
            self.result = self.concat_multiple_csvs(files)
        else:
            self.result = self.spark.read.csv(self.target_path)

    def show_head_of_res(self, num: int):
        pprint(self.result.head(num))
