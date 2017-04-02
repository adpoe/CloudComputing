# CS1699 - Cloud Computing
## Assignment #06 - Hive Queries
### @author Anthony (Tony) Poerio (adp59@pitt.edu)
### Due Date: April 4, 2017

In this directory please find:
  1. Script used for creating the database tables in Hive: `create_tables.hql`
  2. Script used for finding the top 20 airports: `top20.hql`
  3. Script used for answering question about plane delays:  `delays.hql`

Direct answers to both questions are
====================================

top 20 airports
---------------
RD 1422985 --> Chicago  
DFW 1217964 --> Dallas/ Fort Worth   
LAX 929532 --> Los Angeles  
IAH 872160 --> Houston  
DEN 830607 --> Denver  
PHX 779157 --> Phoenix  
LAS 704540 --> Las Vegas  
CVG 670478 --> Cincinnati  
EWR 627791 --> Newark  
SLC 605678 --> Salt Lake City  
DTW 529168 --> Detroit  
MSP 527819 --> Minneapolis  
SFO 521610 --> San Francisco  
BOS 518079 --> Boston  
LGA 509981 --> New York  
MCO 475311 --> Orlando  
PHL 469446 --> Philadelphia  
CLT 463992 --> Charlotte  
IAD 436742 --> Washington, DC  
Time taken: 4.839 seconds, Fetched: 20 row(s)    


do older planes have more delays?  --> not necessarily.
-------------------------------------------------------
1965	12.0  
2006	11.811881188118813  
1997	10.83739837398374  
1977	10.25  
1976	10.125  
1990	10.06015037593985  
NULL	9.913555992141454  
1991	9.55944055944056  
1975	9.555555555555555  
1994	9.297029702970297  
1979	9.181818181818182  
1974	9.0  
1978	9.0  
1992	8.924242424242424  
2005	8.685534591194969  
1999	8.536842105263158  
2002	8.458620689655172  
1986	8.435897435897436  
1988	8.422222222222222  
1993	8.35632183908046  
2004	8.218579234972678  
1996	8.19626168224299  
1987	8.172131147540984  
1989	8.158415841584159  
1998	8.150627615062762  
2000	8.081272084805654  
2007	8.0  
1959	8.0  
1973	8.0  
1980	8.0  
1984	7.961538461538462  
1985	7.924050632911392  
2003	7.837398373983739  
1995	7.777777777777778  
1982	7.666666666666667  
2001	7.634285714285714  
1968	7.583333333333333  
1970	7.5  
1969	7.5  
1963	7.5  
1983	7.111111111111111  
1964	7.0  
1962	7.0  
1966	7.0  
1967	6.8  
1956	6.0  
1971	5.0  
1972	5.0  
0	4.0  
1957	2.0  
Time taken: 6.399 seconds, Fetched: 50 row(s)  

 Thanks for your time,
  Tony
