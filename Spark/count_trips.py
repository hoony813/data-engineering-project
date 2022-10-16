from pyspark import SparkConf, SparkContext
import pandas as pd

# Spark 설정
conf = SparkConf().setMaster("local").setAppName("uber-date-trips").set("spark.driver.bindAddress","127.0.0.1")
sc = SparkContext(conf=conf)

# 데이터
directory = "/Users/imchanghun/data-engineering/data-engineering-project"
filename = "fhvhv_tripdata_2020-03.csv"

# 데이터 파싱
lines = sc.textFile(f"file:///{directory}/{filename}")
header = lines.first()
filtered_lines = lines.filter(lambda row:row != header)

# 필요한 부분만 골라내서 세는 부분
# countByValue로 같은 날짜 등장하는 부분을 샌다
dates = filtered_lines.map(lambda x: x.split(",")[2].split(" ")[0])
result = dates.countByValue()

# CSV로 결과값 저장
pd.Series(result,name="trips").to_csv("trips_date.csv")

