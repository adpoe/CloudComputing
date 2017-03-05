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

-- project, to get rid of unused fields
A = FOREACH flights GENERATE uniqueCarrier AS car,(int)(arrDelay) AS delay; -- use arrival delay to determine delays

-- group by carier
B = GROUP A BY car;

COUNT_TOTAL = FOREACH B {
     	C = FILTER A BY (delay >= 15); -- only keep tuples with a delay >= than 15 minutes
     	GENERATE group, COUNT(A) AS tot, COUNT(C) AS del, (float) COUNT(C)/COUNT(A) AS frac;
}

only_fractions = FOREACH COUNT_TOTAL GENERATE group, frac;
order_fractions = ORDER only_fractions BY frac ASC;
