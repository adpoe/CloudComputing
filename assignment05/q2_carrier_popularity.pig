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
yearly_volume_by_carrier = FOREACH flights GENERATE year AS year, uniqueCarrier AS carrier, year AS emit;

-- group as a pivot-table like structure with data we need as indices
-- and values being the counts of values we emitted --> year, which we know must be present
-- and can therefore be used to count instances
carrier_and_instances = GROUP yearly_volume_by_carrier BY (year, carrier);
carrier_and_counts = FOREACH carrier_and_instances GENERATE FLATTEN(group) as (yr, car), COUNT(yearly_volume_by_carrier) as counts;


-- now, find YoY growth
-- grab our data out of the bag
data = FOREACH carrier_and_counts GENERATE $0, $1, $2;
group_car = GROUP data BY ($1); -- groupby carrier
----------- All Speculation Below this Point ---------------
-- ~~~~~~~~ Here lay dragons!!!!!!!! ~~~~~~~~~ yayyy
-- use a foreach to go through the bags and find what we need
A = FOREACH group_car
      FLATTEN(DATA) as yr, car, counts;

A = GROUP data BY $1 ;
B = FOREACH A {
            -- Project out the first column of A.
            projected = FOREACH A GENERATE $0, $2 ;
            -- Now you can order the projection.
            ordered = ORDER projected BY $0 ;
      GENERATE DIFF($2) as diff;
}

years = FOREACH group_car GENERATE group data.yr as yr;
counts
/* group car
(9E,{(2007,9E,258851),(2008,9E,262208)})
(AA,{(2005,AA,673569),(2006,AA,643597),(2007,AA,633857),(2008,AA,604885)})
(AQ,{(2007,AQ,46360),(2006,AQ,35387),(2008,AQ,7800)})
(AS,{(2006,AS,159404),(2008,AS,151102),(2007,AS,160185),(2005,AS,158100)})
(B6,{(2006,B6,155732),(2007,B6,191450),(2005,B6,111050),(2008,B6,196091)})
(CO,{(2005,CO,296661),(2007,CO,323151),(2006,CO,309389),(2008,CO,298455)})
(DH,{(2005,DH,136492)})
(DL,{(2007,DL,475889),(2006,DL,506086),(2005,DL,658302),(2008,DL,451931)})
(EV,{(2007,EV,286234),(2005,EV,304647),(2006,EV,273143),(2008,EV,280575)})
(F9,{(2005,F9,53255),(2007,F9,97760),(2008,F9,95762),(2006,F9,90181)})
(FL,{(2008,FL,261684),(2006,FL,237645),(2007,FL,263159),(2005,FL,195392)})
(HA,{(2006,HA,52173),(2007,HA,56175),(2008,HA,61826),(2005,HA,48183)})
(HP,{(2005,HP,197853)})
(MQ,{(2006,MQ,550088),(2005,MQ,532032),(2007,MQ,540494),(2008,MQ,490693)})
(NW,{(2006,NW,432880),(2007,NW,414526),(2005,NW,480832),(2008,NW,347652)})
(OH,{(2007,OH,233787),(2005,OH,381337),(2008,OH,197607),(2006,OH,278099)})
(OO,{(2006,OO,548109),(2008,OO,567159),(2005,OO,517454),(2007,OO,597882)})
(TZ,{(2006,TZ,19602),(2005,TZ,44109)})
(UA,{(2005,UA,485918),(2007,UA,490002),(2006,UA,500008),(2008,UA,449515)})
(US,{(2008,US,453589),(2006,US,504844),(2005,US,425609),(2007,US,485447)})
(WN,{(2006,WN,1099321),(2007,WN,1168871),(2008,WN,1201754),(2005,WN,1036034)})
(XE,{(2008,XE,374510),(2007,XE,434773),(2005,XE,403767),(2006,XE,441470)})
(YV,{(2007,YV,294362),(2008,YV,254930),(2006,YV,304764)})
(UniqueCarrier,{(,UniqueCarrier,0)})
grunt> dump group_car
*/
-- type group_car: {group: chararray,data: {(yr: int,car: chararray,counts: long)}}


rows = FOREACH group_car GENERATE group, (data.yr, data.counts) as data;
/* yields
(9E,({(2007),(2008)},{(258851),(262208)}))
(AA,({(2005),(2006),(2007),(2008)},{(673569),(643597),(633857),(604885)}))
(AQ,({(2007),(2006),(2008)},{(46360),(35387),(7800)}))
(AS,({(2006),(2008),(2007),(2005)},{(159404),(151102),(160185),(158100)}))
(B6,({(2006),(2007),(2005),(2008)},{(155732),(191450),(111050),(196091)}))
(CO,({(2005),(2007),(2006),(2008)},{(296661),(323151),(309389),(298455)}))
(DH,({(2005)},{(136492)}))
(DL,({(2007),(2006),(2005),(2008)},{(475889),(506086),(658302),(451931)}))
(EV,({(2007),(2005),(2006),(2008)},{(286234),(304647),(273143),(280575)}))
(F9,({(2005),(2007),(2008),(2006)},{(53255),(97760),(95762),(90181)}))
(FL,({(2008),(2006),(2007),(2005)},{(261684),(237645),(263159),(195392)}))
(HA,({(2006),(2007),(2008),(2005)},{(52173),(56175),(61826),(48183)}))
(HP,({(2005)},{(197853)}))
(MQ,({(2006),(2005),(2007),(2008)},{(550088),(532032),(540494),(490693)}))
(NW,({(2006),(2007),(2005),(2008)},{(432880),(414526),(480832),(347652)}))
(OH,({(2007),(2005),(2008),(2006)},{(233787),(381337),(197607),(278099)}))
(OO,({(2006),(2008),(2005),(2007)},{(548109),(567159),(517454),(597882)}))
(TZ,({(2006),(2005)},{(19602),(44109)}))
(UA,({(2005),(2007),(2006),(2008)},{(485918),(490002),(500008),(449515)}))
(US,({(2008),(2006),(2005),(2007)},{(453589),(504844),(425609),(485447)}))
(WN,({(2006),(2007),(2008),(2005)},{(1099321),(1168871),(1201754),(1036034)}))
(XE,({(2008),(2007),(2005),(2006)},{(374510),(434773),(403767),(441470)}))
(YV,({(2007),(2008),(2006)},{(294362),(254930),(304764)}))

grunt> describe rows
rows: {group: chararray,org.apache.pig.builtin.totuple_533: ({(yr: int)},{(counts: long)})}*/


ordered = FOREACH group_car {
  rows = GENERATE (data.yr as yr, data.counts);
  sorted = ORDER rows BY yr;

}


ordered = FOREACH data {
    sorted = ORDER data BY $1 DESC;
    volumes =  GENERATE sorted.$2 as srt; -- get counts
    GENERATE volumes.srt.$1
   }

ordered = ORDER data by $1;
/* ordered
(2007,9E,258851)
(2008,9E,262208)
(2005,AA,673569)
(2006,AA,643597)
(2007,AA,633857)
(2008,AA,604885)
(2007,AQ,46360)
(2006,AQ,35387)
(2008,AQ,7800)
(2006,AS,159404)
(2008,AS,151102)
(2007,AS,160185)
(2005,AS,158100)
(2006,B6,155732)
(2007,B6,191450)
*/

growth = FOREACH data {
    ordered = ORDER data by $1;
    growth = GENERATE
}
/* data
(2005,B6,111050)
(2005,DL,658302)
(2005,NW,480832)
(2005,TZ,44109)
(2005,UA,485918)
(2005,US,425609)
(2005,WN,1036034)
(2006,AQ,35387)
(2006,CO,309389)
(2006,EV,273143)
(2006,FL,237645)
(2006,HA,52173)
(2006,MQ,550088)
(2006,OO,548109)
(2007,AA,633857)
(2007,AS,160185)
(2007,F9,97760)
(2007,OH,233787)
(2007,XE,434773)
(2007,YV,294362)
(2008,9E,262208)
(2008,B6,196091)
(2008,DL,451931)
(2008,NW,347652)
(2008,UA,449515)
(2008,US,453589)
(2008,WN,1201754)
(2005,AA,673569)
(2005,AS,158100)
(2005,F9,53255)
(2005,OH,381337)
(2005,XE,403767)
(2006,B6,155732)
(2006,DL,506086)
(2006,NW,432880)
(2006,TZ,19602)
(2006,UA,500008)
(2006,US,504844)
(2006,WN,1099321)
(2007,AQ,46360)
(2007,CO,323151)
(2007,EV,286234)
(2007,FL,263159)
(2007,HA,56175)
(2007,MQ,540494)
(2007,OO,597882)
(2008,AA,604885)
(2008,AS,151102)
(2008,F9,95762)
(2008,OH,197607)
(2008,XE,374510)
(2008,YV,254930)
(2005,CO,296661)
(2005,DH,136492)
(2005,EV,304647)
(2005,FL,195392)
(2005,HA,48183)
(2005,HP,197853)
(2005,MQ,532032)
(2005,OO,517454)
(2006,AA,643597)
(2006,AS,159404)
(2006,F9,90181)
(2006,OH,278099)
(2006,XE,441470)
(2006,YV,304764)
(2007,9E,258851)
(2007,B6,191450)
(2007,DL,475889)
(2007,NW,414526)
(2007,UA,490002)
(2007,US,485447)
(2007,WN,1168871)
(2008,AQ,7800)
(2008,CO,298455)
(2008,EV,280575)
(2008,FL,261684)
(2008,HA,61826)
(2008,MQ,490693)
(2008,OO,567159)
(,UniqueCarrier,0)*/
