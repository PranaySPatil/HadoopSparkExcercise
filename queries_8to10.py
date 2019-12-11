from pyspark.sql import SparkSession
import datetime
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

spark = SparkSession.builder.master("yarn").appName("my app").enableHiveSupport().getOrCreate()

# Q8
flight_data_denorm = spark.table("flights.flight_data_denorm")


@f.udf(returnType=BooleanType())
def dateFilter(date):
    start_date = '12/20/2016'
    formatted_start_date = datetime.strptime(start_date, '%m/%d/%Y')
    return date > formatted_start_date


df = flight_data_denorm.filter(dateFilter(flight_data_denorm.FL_DATE))
df = df.groupBy(flight_data_denorm.AIRPORT_NAMES.DESTINATION).count()
df = df.withColumnRenamed("AIRPORT_NAMES['DESTINATION']", "AIRPORT_NAME").withColumnRenamed("count", "FLIGHT_COUNT")
df = df.sort(df.FLIGHT_COUNT.desc()).limit(3)
df.show()


# Q9
@f.udf(returnType=BooleanType())
def myFilter(dest):
    return dest.split(':')[0].split(',')[1] == ' MT'


df = flight_data_denorm.filter(myFilter(flight_data_denorm.AIRPORT_NAMES.DESTINATION))
partition_def = Window.partitionBy(df.AIRPORT_NAMES.DESTINATION)
df = df.withColumn("FLIGHT_COUNT", f.count("*").over(partition_def))
df = df.sort(df.FLIGHT_COUNT.desc()).limit(1)
df = df.select(df.DEST, df.FLIGHT_COUNT)
df.show()

# Q10
df = spark.sql("SELECT COUNT(*) FROM flights.flight_data_denorm")
df.show()