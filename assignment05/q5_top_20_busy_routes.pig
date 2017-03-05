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



-- taking suggestion of prompt --> use frequency table (i,j)
filtered_dataset = foreach flights
                   generate
                      origin as i,
                      dest as j;

freq_table = group filtered_dataset by (i,j);

-- and just count the values in frequency table
frequency_counts = foreach freq_table  -- get each row of table
                   generate group, COUNT(filtered_dataset); -- count bags present
/* results
...
((MDW,LIT),1478)
((MDW,MHT),5949)
((MDW,MIA),437)
((MDW,OMA),8265)
((MDW,PDX),2432)
((MDW,RNO),783)
 ...
*/

-- now order and dump for Users
order_desc = ORDER frequency_counts BY $1 DESC;
TOP_20 = LIMIT order_desc 20;
DUMP TOP_20
/* results
((SAN,LAX),53834)
((LAX,SAN),53801)
((LAX,LAS),52241)
((LAS,LAX),51185)
((BOS,LGA),49848)
((LGA,BOS),49816)
((OGG,HNL),49452)
((HNL,OGG),48971)
((DCA,LGA),47516)
((LGA,DCA),47469)
((LAS,PHX),45287)
((LGA,ORD),45136)
((ORD,LGA),45054)
((PHX,LAX),44595)
((PHX,LAS),44498)
((SFO,LAX),44265)
((LAX,SFO),43491)
((LAX,PHX),43421)
((LIH,HNL),40835)
((LGA,ATL),40611)
*/
