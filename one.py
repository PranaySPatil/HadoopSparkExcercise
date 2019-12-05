from pyspark.sql import SparkSession
spark = SparkSession.builder.master("yarn").appName("my app").enableHiveSupport().getOrCreate()

flight_data_orc2 = spark.table("flights.flight_data_orc2")
flightDF = flight_data_orc2.groupBy(flight_data_orc2.origin).agg({"dep_delay":"avg"}).withColumnRenamed("avg(dep_delay)", "avg_dep_delay")
airport_codes = spark.table("flights.airport_codes")
airport = flightDF.sort(flightDF.avg_dep_delay.desc()).collect()[0][0]
airport_codes.filter(airport_codes.Code==airport).show()