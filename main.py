import pandas as pd
import numpy as np
import os
from pprint import pprint
from spark_handler import SparkCSVConcater


def get_file_paths() -> list:
    paths = []
    for dirname, _, filenames in os.walk("data/"):
        for filename in filenames:
            paths.append(os.path.join(dirname, filename))
    return paths


def get_dataframe_list(paths: list) -> list:
    df_list = [pd.read_csv(path) for path in paths]
    return df_list


if __name__ == "__main__":
    file_paths = get_file_paths()
    spark_handler = SparkCSVConcater()
    spark_handler.load_data(file_paths)
    spark_handler.show_head_of_res(5)
    print("file Saved")
