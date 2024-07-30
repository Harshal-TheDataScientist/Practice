from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
        .master("local[*]") \
        .appName("check") \
        .config("spark.pyspark.python", r"D:/pyspark_setup/pycharm/idk/pythonProject/.venv/Scripts/First_project/.venv/Scripts/python.exe") \
        .config("spark.python.worker.memory", "2g") \
        .config("spark.python.worker.reuse", "true") \
        .getOrCreate()

data = [("Rachna","Manager"),("Supriya","Intern"),("Harshal","Intern")]

df = spark.createDataFrame(data,("NAME","DESIGNATION"))

df.show()