#Q5
flight_data_denorm = spark.table("flights.flight_data_denorm")
flight_data_denorm.filter(flight_data_denorm. AIRPORT_NAMES.SOURCE.contains("Beaumont")).agg({"DEP_DELAY":"sum"}).show()

#Q6
flight_data_denorm = spark.table("flights.flight_data_denorm")
flight_data_denorm.filter(flight_data_denorm. AIRPORT_NAMES.DESTINATION.contains("TX")).filter(flight_data_denorm. CARRIER_NAME.contains("Virgin")).count() 

#Q7
flight_data_denorm = spark.table("flights.flight_data_denorm")
flight_data_denorm.select(flight_data_denorm.CARRIER_NAME).distinct().count()
