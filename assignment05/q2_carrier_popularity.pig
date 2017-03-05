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


-- pull out the data we're interested in: year and carrier
-- we can get number of flights by counting instances of each uniqueCarrier
yearly_volume_by_carrier = FOREACH flights
                           GENERATE year AS year,
                                    uniqueCarrier AS carrier,
                                    year AS emit;

-- group as a pivot-table like structure with data we need as indices
-- and values being the counts of values we emitted --> year, which we know must be present
-- and can therefore be used to count instances
carrier_and_instances = GROUP yearly_volume_by_carrier
                        BY (year, carrier);

carrier_and_counts = FOREACH carrier_and_instances
                     GENERATE FLATTEN(group) as (yr, car),
                              COUNT(yearly_volume_by_carrier) as counts;


-- now, find YoY growth
-- grab our data out of the bag
data = FOREACH carrier_and_counts
       GENERATE $0, $1, $2;

base_cols = foreach data
            generate
                  $1 as label,
                  ($0==2005 ? $2 : 0) as yr_2005_vol, -- get volumnes
                  ($0==2006 ? $2 : 0) as yr_2006_vol,
                  ($0==2007 ? $2 : 0) as yr_2007_vol,
                  ($0==2008 ? $2 : 0) as yr_2008_vol;
/* yields
      ...
(US,425609,0,0,0)
(WN,1036034,0,0,0)
(AQ,0,35387,0,0)
(CO,0,309389,0,0)
(EV,0,273143,0,0)
      ...
*/
yr_grps = group base_cols by label;
/* yields
      ...
(AS,{(AS,0,159404,0,0),(AS,0,0,0,151102),(AS,0,0,160185,0),(AS,158100,0,0,0)})
(B6,{(B6,0,155732,0,0),(B6,0,0,191450,0),(B6,111050,0,0,0),(B6,0,0,0,196091)})
(CO,{(CO,296661,0,0,0),(CO,0,0,323151,0),(CO,0,309389,0,0),(CO,0,0,0,298455)})
      ...
yr_grps: {group: chararray,base_cols:
{(label: chararray,yr_2005_vol: long,yr_2006_vol: long,yr_2007_vol: long,yr_2008_vol: long)}}
*/

yr_05_flat = foreach yr_grps
             generate group as label,
             FLATTEN(base_cols.yr_2005_vol) as yr_05; -- can sum and then join all these

yr_06_flat = foreach yr_grps
             generate group as label,
             FLATTEN(base_cols.yr_2006_vol) as yr_06; -- can sum and then join all these

yr_07_flat = foreach yr_grps
             generate group as label,
             FLATTEN(base_cols.yr_2007_vol) as yr_07; -- can sum and then join all these

yr_08_flat = foreach yr_grps
             generate group as label,
             FLATTEN(base_cols.yr_2008_vol) as yr_08; -- can sum and then join all these

/*
(AA,673569)
(AA,0)
(AA,0)
(AA,0)
(AQ,0)
(AQ,0)
(AQ,0)
(AS,0)
(AS,0)
(AS,0)
(AS,158100)
*/
yr_05_value = FILTER yr_05_flat by $1 > 0;
yr_06_value = FILTER yr_06_flat by $1 > 0;
yr_07_value = FILTER yr_07_flat by $1 > 0;
yr_08_value = FILTER yr_08_flat by $1 > 0;

df = JOIN yr_05_value by label,
          yr_06_value by label,
          yr_07_value by label,
          yr_08_value by label;

totals = FOREACH df
         GENERATE $0 as label,
                  ((float)$1/(float)$3) + ((float)$3/(float)$5) + ((float)$5/(float)$7) as total_growth;

-- and sort totals for display
order_desc = ORDER totals BY $1 DESC;
DUMP order_desc
