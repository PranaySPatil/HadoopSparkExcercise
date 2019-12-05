#Q1
flight_data_orc2 = spark.table("flights.flight_data_orc2")
flightDF = flight_data_orc2.groupBy(flight_data_orc2.origin).agg({"dep_delay":"avg"}).withColumnRenamed("avg(dep_delay)", "avg_dep_delay")
airport_codes = spark.table("flights.airport_codes")
airport = flightDF.sort(flightDF.avg_dep_delay.desc()).collect()[0][0]
airport_codes.filter(airport_codes.Code==airport).show()

#Q2
flight_data_denorm = spark.table("flights.flight_data_denorm")
df=flight_data_denorm.groupBy(flight_data_denorm.AIRPORT_NAMES.SOURCE).agg({"dep_delay":"avg"})
df=df.withColumnRenamed("AIRPORT_NAMES['SOURCE']","AIRPORT_NAME").withColumnRenamed("avg(dep_delay)","AVG_DEP_DELAY")
df.sort(df.AVG_DEP_DELAY.desc()).collect()[0][0]

#Q3
flight_data_date = spark.table("flights.flight_data_date")
df = flight_data_date.groupBy(flight_data_date.carrier).agg({"arr_delay":"sum"})
df=df.withColumnRenamed("sum(arr_delay)","arr_delay")
carrier=df.sort(df.arr_delay.desc()).collect()[0][0]
carrier_codes = spark.table("flights.carrier_codes")
carrier_codes.filter(carrier_codes.Code==carrier).show()

#Q4
flight_data_denorm = spark.table("flights.flight_data_denorm")
df=flight_data_denorm.groupBy(flight_data_denorm.CARRIER_NAME).agg({"ARR_DELAY":"sum"})
df=df.withColumnRenamed("sum(ARR_DELAY)","ARR_DELAY")
df.sort(df.ARR_DELAY.desc()).collect()[0][0]
