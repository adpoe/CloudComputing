-- Local
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();

-- Non-local: 'flights/2005.csv'
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

-- group by airport code
origin_grps = GROUP flights
              BY origin;

dst_grps = GROUP flights
           BY dest;

-- get total counts
grp_origin_counts = FOREACH origin_grps
                    GENERATE group,
                             COUNT(flights) as count_flights;

grp_dst_counts = FOREACH dst_grps
                 GENERATE group,
                          COUNT(flights) as count_flights;

total_counts =  UNION grp_origin_counts, grp_dst_counts;

-- perform a join and sum the groups in the result
total_join = JOIN grp_origin_counts by group,
                  grp_dst_counts by group;

ts = FOREACH total_join
     GENERATE grp_origin_counts::group,
              (grp_origin_counts::count_flights + grp_dst_counts::count_flights) as total_counts;

-- dump total counts for user
order_desc = ORDER ts
             BY total_counts DESC;

TOP_20 = LIMIT order_desc 20;
DUMP TOP_20
