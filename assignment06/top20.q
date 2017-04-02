
CREATE TABLE flight_counts(
    ariport STRING,
    count INT);

-- group by origin airport code
INSERT INTO flight_counts SELECT origin, count(origin) from flight_data GROUP BY origin;

-- group by destination airport code
INSERT INTO flight_counts SELECT dest, count(dest) from flight_data GROUP BY dest;


-- get the totals from our new flight counts table
CREATE TABLE total_flight_counts(
   airport STRING,
   total INT);

INSERT INTO total_flight_counts SELECT ariport, sum(count) from flight_counts GROUP BY ariport;

""" Results
ATL	1676278 --> Atlanta
ORD	1422985 --> Chicago
DFW	1217964 --> Dallas/Fort Worth
LAX	929532 --> Los Angeles
IAH	872160 --> Houston
DEN	830607 --> Denver
PHX	779157 --> Phoenix
LAS	704540 --> Las Vegas
CVG	670478 --> Cincinnati
EWR	627791 --> Newark
SLC	605678 --> Salt Lake City
DTW	529168 --> Detroit
MSP	527819 --> Minneapolis
SFO	521610 --> San Francisco
BOS	518079 --> Boston
LGA	509981 --> New York
MCO	475311 --> Orlando
PHL	469446 --> Philadelphia
CLT	463992 --> Charlotte
IAD	436742 --> Washington, DC
Time taken: 4.839 seconds, Fetched: 20 row(s)
"""
