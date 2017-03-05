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


------ Re-use Code from Q3 // Arrival Delays -------
-- project, to get rid of unused fields
A = FOREACH flights GENERATE uniqueCarrier AS car, (int)(arrDelay) AS delay; -- use arrival delay to determine delays

-- group by carier
B = GROUP A BY car;

COUNT_TOTAL = FOREACH B {
     	C = FILTER A BY (delay >= 15); -- only keep tuples with a delay >= than 15 minutes
     	GENERATE group, COUNT(A) AS tot, COUNT(C) AS del, (float) COUNT(C)/COUNT(A) AS arr_frac;
}

-- sort for output
only_fractions = FOREACH COUNT_TOTAL GENERATE group, arr_frac;
order_fractions_arrival_delay = ORDER only_fractions BY arr_frac ASC;
---------------- END ARRIVAL DELAYS ------------------
/* results -->
(UniqueCarrier,0.0)
(HA,0.065928735)
(AQ,0.0711805)
(HP,0.17399281)
(WN,0.1855077)
(9E,0.18889607)
(OO,0.19264872)
(DH,0.19775517)
(F9,0.19790597)
(TZ,0.21132928)
(DL,0.21484193)
(OH,0.22701155)
(US,0.22867478)
(YV,0.23143096)
(XE,0.23232298)
(FL,0.23658705)
(MQ,0.23693244)
(CO,0.24154568)
(NW,0.2419228)
(UA,0.24376364)
(AA,0.24692008)
(AS,0.2477898)
(B6,0.26485085)
(EV,0.2776003)
*/


------- New Code // Carrier Delays ---------------------
-- instead of calculating arrDelay itself, find the number caused carrier delays
A = FOREACH flights GENERATE uniqueCarrier AS carrier,(int)(carrierDelay) AS delay;

-- group by carrier
B = GROUP A BY carrier;
COUNT_TOTAL = FOREACH B {
	C = FILTER A BY (delay >= 15); -- only keep tuples with a delay >= than 15 minutes
	GENERATE group, COUNT(A) AS tot, COUNT(C) AS del, (float) COUNT(C)/COUNT(A) AS cr_frac;
}

-- sort for output
only_fractions = FOREACH COUNT_TOTAL GENERATE group, cr_frac;
order_fractions_carrier_delay = ORDER only_fractions BY cr_frac ASC;
---------------- END CARRIER DELAYS -----------------------


----- Get the Ratio of arrival_delay / carrierDelay -----
arrival_and_carrier_delays = JOIN
                            ord_fractions_arrival_delay BY group,
                            order_fractions_carrier_delay BY group;
