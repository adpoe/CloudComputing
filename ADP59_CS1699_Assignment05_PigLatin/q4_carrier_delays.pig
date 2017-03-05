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
-- use arrival delay to determine delays
-- and only take keys we are going to use, to save space + improve readability
df = FOREACH flights
     GENERATE uniqueCarrier AS car,
              arrDelay AS delay;

-- group by carrier
grp_car = GROUP df BY car;

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

order_pct_arrivals = ORDER only_pct
                     BY percentage ASC;

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
-- use arrival delay to determine delays
-- and only take keys we are going to use, to save space + improve readability
df = FOREACH flights
     GENERATE uniqueCarrier AS car,
              carrierDelay AS delay;

-- group by carrier
grp_car = GROUP df BY car;

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

order_pct_carriers = ORDER only_pct
                     BY percentage ASC;
---------------- END CARRIER DELAYS -----------------------



----- Get the Ratio of carrierDelay / arrival_delay -----
arrival_and_carrier_delays = JOIN
                            order_pct_arrivals BY group,
                            order_pct_carriers BY group;
----------------------------------------------------------
/* results
(9E,0.18889607,9E,0.05788404)
(AA,0.24692008,AA,0.063971005)
(AQ,0.0711805,AQ,0.035612583)
(AS,0.2477898,AS,0.0790517)
(B6,0.26485085,B6,0.04886883)
(CO,0.24154568,CO,0.04786357)
(DH,0.19775517,DH,0.035665095)
(DL,0.21484193,DL,0.05313382)
(EV,0.2776003,EV,0.11914216)
(F9,0.19790597,F9,0.04765579)
(FL,0.23658705,FL,0.041693114)
(HA,0.065928735,HA,0.043094564)
(HP,0.17399281,HP,0.052766446)
(MQ,0.23693244,MQ,0.060964167)
(NW,0.2419228,NW,0.08073263)
(OH,0.22701155,OH,0.08766261)
(OO,0.19264872,OO,0.09314518)
(TZ,0.21132928,TZ,0.03875312)
(UA,0.24376364,UA,0.06319585)
(US,0.22867478,US,0.06151146)
(WN,0.1855077,WN,0.03652036)
(XE,0.23232298,XE,0.04340171)
(YV,0.23143096,YV,0.10662182)
(UniqueCarrier,0.0,UniqueCarrier,0.0)
*/


--------- FINAL OUTPUT ---------
pcts = FOREACH arrival_and_carrier_delays
       GENERATE $0 as label,
                $3/$1 as delay_pct;

pct_ordered = ORDER pcts
              BY delay_pct ASC;


dump pct_ordered

/* results
(9E,0.30643326)
(AA,0.25907576)
(AQ,0.50031376)
(AS,0.31902727)
(B6,0.18451454)
(CO,0.19815536)
(DH,0.18034974)
(DL,0.24731587)
(EV,0.42918602)
(F9,0.24080017)
(FL,0.17622738)
(HA,0.65365374)
(HP,0.30326796)
(MQ,0.25730613)
(NW,0.33371237)
(OH,0.38615924)
(OO,0.4834975)
(TZ,0.18337789)
(UA,0.2592505)
(US,0.268991)
(WN,0.19686708)
(XE,0.18681626)
(YV,0.4607068)
(UniqueCarrier,)
*/
