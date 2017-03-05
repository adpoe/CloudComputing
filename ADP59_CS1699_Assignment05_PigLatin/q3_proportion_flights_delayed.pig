-- LOCAL
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();

-- Non-local: 'your/path/flights_directory'
flights = LOAD '/Users/tony/Documents/_LEARNINGS/CLOUD/pig/flights' using CSVExcelStorage() AS
  	(year:int,
     month:int,
     dayOfMonth:int,
     dayOfWeek:int,
     depTime:int,
     crsDepTime:int,
     arrTime:int,
     crsArrTime:int,
     uniqueCarrier:chararray,
     flightNum:int,
     tailNum:chararray,
     actualElapsedTime:int,
     crsElapsedTime:int,
     airTime:int,
     arrDelay:int,
     depDelay:int,
     origin:chararray,
     dest:chararray,
     distance:int,
     taxiIn:int,
     taxiOut:int,
     cancelled:int,
     cancellationCode:int,
     diverted:int,
     carrierDelay:int,
     weatherDelay:int,
     nasDelay:int,
     securityDelay:int,
     lateAircraftDelay:int);

-- use arrival delay to determine delays
-- and only take keys we are going to use, to save space + improve readability
df = FOREACH flights
     GENERATE uniqueCarrier AS car,
              arrDelay AS delay;

-- group by carrier
grp_car = GROUP df
          BY car;

-- iterate through each grouping
delay_counts = FOREACH grp_car {
     	only_delays = FILTER df
                    BY (delay >= 15);
     	GENERATE group,
               COUNT(df) AS total_records,
               COUNT(only_delays) AS num_delays,
               (float)COUNT(only_delays)/(float)COUNT(df) AS percentage; -- need a float, since we are doing division
}

-- pull out only the data we need
only_pct= FOREACH delay_counts
          GENERATE group, percentage;

order_pct = ORDER only_pct
            BY percentage ASC;

-- dump for end user
dump order_pct
