-- Create flights table
CREATE TABLE flight_data(
         year INT,
         month INT,
         dayOfMonth INT,
         dayOfWeek INT,
         depTime INT,
         crsDepTime INT,
         arrTime INT,
         crsArrTime INT,
         uniqueCarrier STRING,
         flightNum INT,
         tailNum STRING,
         actualElapsedTime INT,
         crsElapsedTime INT,
         airTime INT,
         arrDelay INT,
         depDelay INT,
         origin STRING,
         dest STRING,
         distance INT,
         taxiIn INT,
         taxiOut INT,
         cancelled INT,
         cancellationCode INT,
         diverted INT,
         carrierDelay INT,
         weatherDelay INT,
         nasDelay INT,
         securityDelay INT,
         lateAircraftDelay INT)
    COMMENT 'Data about flights'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ",";
LOAD DATA LOCAL INPATH '2005.csv' INTO TABLE flight_data;
LOAD DATA LOCAL INPATH '2006.csv' INTO TABLE flights_db;
LOAD DATA LOCAL INPATH '2007.csv' INTO TABLE flights_db;
LOAD DATA LOCAL INPATH '2008.csv' INTO TABLE flights_db;

-- Create airports table
CREATE TABLE airport_data(
        iata STRING,
        airport STRING,
        city STRING,
        state STRING,
        country STRING,
        lat DOUBLE,
        long DOUBLE)
  COMMENT 'Data about airports'
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ",";
LOAD DATA LOCAL INPATH 'airports.csv' INTO TABLE airport_data;

-- Create plane_data table
CREATE TABLE plane_metadata(
        tailnum STRING,
        type STRING,
        manufacturer STRING,
        issue_date STRING,
        model STRING,
        status STRING,
        aircraft_type STRING,
        engine_type STRING,
        year INT)
      COMMENT 'Metadata about the planes themselves'
      ROW FORMAT DELIMITED
      FIELDS TERMINATED BY ",";
LOAD DATA LOCAL INPATH 'plane-data.csv' INTO TABLE plane_metadata;
