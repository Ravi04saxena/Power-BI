create database flight_analysis;
use flight_analysis;
select * from airport_data;

-- rename table
rename table airport_data to meta_data;


-- Create 5 tables Airline, Airport, Flight, Flight Metrics and City

create table Airline
(
Airline_Id int primary key,
Unique_Carrier varchar(10),
Unique_Carrier_Name varchar(100),
Unique_Carrier_Entity varchar(10)
);

create table Airport(
Airport_Id int primary key,
Airport_Seq_Id int,
City_market_Id int,
Airport_Code varchar(10),
City_Name varchar(100),
State_ABR char(10),
State_FIPS int,
State_NM varchar(100),
WAC int
);

create table Flight(
Flight_ID int Auto_Increment Primary key,
Airline_ID int,
Origin_Airport_ID int,
Dest_Airport_Id int,
Distance float,
Distance_Group int,
Year int,
Quarter int,
Month int,
Class char(2),
Foreign Key (Airline_Id) references Airline(Airline_Id),
foreign key (Origin_Airport_Id) references Airport(Airport_Id),
foreign key (Dest_Airport_Id) references Airport(Airport_Id)
);


Create table Flight_Metrics(
Flight_Id int,
Passengers float,
Freight float,
Mail float,
foreign key (Flight_Id) references Flight(Flight_ID)
);

create table City(
City_Id int auto_increment primary key,
City_Name Varchar(100),
State_ABR char(10),
unique (City_Name, State_ABR)
);

## Data insertion into newly created table from Master Table Meta_Data

-- IGNORE is used to ignore duplicate entry
INSERT ignore INTO Airline (AIRLINE_ID, UNIQUE_CARRIER, UNIQUE_CARRIER_NAME, UNIQUE_CARRIER_ENTITY)
SELECT DISTINCT
    AIRLINE_ID,
    UNIQUE_CARRIER,
    UNIQUE_CARRIER_NAME,
    UNIQUE_CARRIER_ENTITY
FROM Meta_Data
where Airline_Id is not null;

select * from airline;
select count(distinct airline_id) from airline;

INSERT INTO Airport (
    AIRPORT_ID, AIRPORT_SEQ_ID, CITY_MARKET_ID, AIRPORT_CODE,
    CITY_NAME, STATE_ABR, STATE_FIPS, STATE_NM, WAC
)
SELECT DISTINCT
    ORIGIN_AIRPORT_ID,
    ORIGIN_AIRPORT_SEQ_ID,
    ORIGIN_CITY_MARKET_ID,
    ORIGIN,
    ORIGIN_CITY_NAME,
    ORIGIN_STATE_ABR,
    ORIGIN_STATE_FIPS,
    ORIGIN_STATE_NM,
    ORIGIN_WAC
FROM Meta_Data;

-- Destination
INSERT INTO Airport (
    AIRPORT_ID, AIRPORT_SEQ_ID, CITY_MARKET_ID, AIRPORT_CODE,
    CITY_NAME, STATE_ABR, STATE_FIPS, STATE_NM, WAC
)
SELECT DISTINCT
    DEST_AIRPORT_ID,
    DEST_AIRPORT_SEQ_ID,
    DEST_CITY_MARKET_ID,
    DEST,
    DEST_CITY_NAME,
    DEST_STATE_ABR,
    DEST_STATE_FIPS,
    DEST_STATE_NM,
    DEST_WAC
FROM Meta_Data
where DEST_AIRPORT_ID not in 
(select airport_id from airport);

select * from airport;


INSERT INTO Flight (
    AIRLINE_ID, ORIGIN_AIRPORT_ID, DEST_AIRPORT_ID,
    DISTANCE, DISTANCE_GROUP,
    YEAR, QUARTER, MONTH, CLASS
)
SELECT
    AIRLINE_ID,
    ORIGIN_AIRPORT_ID,
    DEST_AIRPORT_ID,
    DISTANCE,
    DISTANCE_GROUP,
    YEAR,
    QUARTER,
    MONTH,
    CLASS
FROM Meta_Data;

select * from flight;

select freight from meta_data;

INSERT INTO FlightMetrics (
    FLIGHT_ID, PASSENGERS, FREIGHT, MAIL
)
SELECT
    f.FLIGHT_ID,
    m.PASSENGERS,
    cast(m.FREIGHT as float) as FREIGHT,
    m.MAIL
FROM Meta_Data m
JOIN Flight f
  ON f.AIRLINE_ID = m.AIRLINE_ID
 AND f.ORIGIN_AIRPORT_ID = m.ORIGIN_AIRPORT_ID
 AND f.DEST_AIRPORT_ID = m.DEST_AIRPORT_ID
 AND f.YEAR = m.YEAR
 AND f.MONTH = m.MONTH
 AND f.QUARTER = m.QUARTER
 AND f.DISTANCE = m.DISTANCE;
 
select * from city;

insert into city (Cityname, State_ABR, State_NM)
select distinct
   ORIGIN_CITY_NAME,
   ORIGIN_STATE_ABR,
   ORIGIN_STATE_NM
from meta_data;

insert into city (CityName, State_ABR, State_NM)
select distinct
  DEST_CITY_NAME,
  DEST_STATE_ABR,
  DEST_STATE_NM
from meta_data
where DEST_CITY_NAME not in (
  select CityName from city);
  
  select * from city;

## Data analysis
select * from flight_metrics; 
select * from flight;
select * from airport;


select * from city;

### Route wise flight analysis
select 
  f.Origin_Airport_ID,
  f.Dest_Airport_Id,
  a1.City_Name as Origin_City,
  a2.City_Name as Dest_City,
  sum(fm.Passengers) as Total_Passengers
from flight f
join flightmetrics fm ON f.Flight_ID = fm.Flight_ID
join Airport a1 on f.Origin_Airport_ID = a1.Airport_Id
join Airport a2 on f.Dest_Airport_Id = a2.Airport_Id
group by f.Origin_Airport_ID, f.Dest_Airport_Id
order by Total_Passengers Desc
limit 10;

# Total passenger served in the duration
select
  f.year,
  f.month,
  concat(round(sum(fm.Passengers)/1000000,2), ' Million') as Total_Passengers
from flight f
join flightmetrics fm on f.Flight_ID = fm.Flight_ID
group by f.year, f.month
order by  f.year, f.month;

## Average Passengers per Origin City
use flight_analysis;
select * from flight_metrics; 
select * from flight;
select * from airport;

Select
  f.Origin_Airport_ID,
  a.City_Name as Origin_City,
  count(f.Flight_ID) as Total_flight,
  sum(fm.Passengers) as Total_Passengers,
  round(avg(fm.Passengers),2) as Avg_Passengers_per_Flight
from flight f
join flightmetrics fm on f.Flight_ID = fm.Flight_Id
join Airport a on f.Origin_Airport_ID = a.Airport_Id
group by f.Origin_Airport_ID
order by Avg_Passengers_per_Flight desc
limit 7;


## Average Passengers per destination City
select
  f.Dest_Airport_Id,
  a.City_Name as Dest_City,
  count(f.Flight_ID) as Total_Flight,
  sum(fm.Passengers) as Total_Passengers,
  round(avg(fm.Passengers),2) as Avg_Passengers_per_Flight
from flight f
join flight_metrics fm on f.Flight_ID = fm.Flight_Id
join Airport a on f.Dest_Airport_Id = a.Airport_Id
group by f.Dest_Airport_Id
order by Avg_Passengers_per_Flight desc
limit 7;

use flight_analysis;

## Corelation Between Population and Air Traffic.

select * from city;
select * from all_city_pop;

## extracting city name from city table

update city
set CityName = substring_index(CityName, ',',1);

SET SQL_Safe_Updates = 0;

create table City_New
(select City_id,substring_index(CityName,',',1) as City_Name,State_ABR,
State_NM, Population
from city c
left join all_city_pop as a
on a.city_name = c.Cityname);

drop table city;
 
select *  from City_New;


### Analyse the relation between city population and airport traffic. 

## Cities as Origin

-- rename city_new table
alter table city_new rename city;

update airport
SET CITY_NAME = substring_index(CITY_NAME, ',',1);

select * from city;
select * from flightmetrics;
select * from Airport;
select * from flight;

select
  c.City_Name,
  c.Population,
  sum(fm.Passengers) AS Total_Passenger
from city c
join airport a on a.CITY_NAME = c.CITY_NAME
join flight f on f.ORIGIN_AIRPORT_ID = a.AIRPORT_ID
join flightmetrics fm on f.FLIGHT_ID = fm.FLIGHT_ID
group by c.city_name, c.Population
order by Total_Passenger DESC;

--  Passenger Population Ratio
select
  c.City_Name,
  c.Population,
  sum(fm.Passengers) AS Total_Passenger
from city c
join airport a on a.CITY_NAME = c.CITY_NAME
join flight f on f.ORIGIN_AIRPORT_ID = a.AIRPORT_ID
join flightmetrics fm on f.FLIGHT_ID = fm.FLIGHT_ID
group by c.city_name, c.Population
order by Total_Passenger DESC;

select
  c.City_Name,
  c.Population,
  sum(fm.Passengers) AS Total_Passenger,
  round(sum(fm.Passengers)/c.Population,2) as Pass_Pop_Ratio
from city c
join airport a on a.CITY_NAME = c.CITY_NAME
join flight f on f.ORIGIN_AIRPORT_ID = a.AIRPORT_ID
join flightmetrics fm on f.FLIGHT_ID = fm.FLIGHT_ID
group by c.city_name, c.Population
order by Pass_Pop_Ratio DESC;


## Cities as Destination
select
  c.City_Name,
  c.Population,
  sum(fm.Passengers) AS Total_Passenger
from city c
join airport a on a.CITY_NAME = c.CITY_NAME
join flight f on f.Dest_AIRPORT_ID = a.AIRPORT_ID
join flightmetrics fm on f.FLIGHT_ID = fm.FLIGHT_ID
group by c.city_name, c.Population
order by Total_Passenger DESC;

select
  c.City_Name,
  c.Population,
  sum(fm.Passengers) AS Total_Passenger,
  count(f.flight_id) as Total_Flight,
  round(sum(fm.Passengers)/c.Population,2) as Pass_Pop_Ratio
from city c
join airport a on a.CITY_NAME = c.CITY_NAME
join flight f on f.ORIGIN_AIRPORT_ID = a.AIRPORT_ID
join flightmetrics fm on f.FLIGHT_ID = fm.FLIGHT_ID
group by c.city_name, c.Population
order by Pass_Pop_Ratio DESC;


### Acccess flight frequency and identify high-traffic corridors.
# To assess flight frequency and identify high-traffic corridors, we will:
# 1.Count how often each route (origin → destination) appears — that’s flight frequency.
# 2.Identify routes with the highest number of flights — these are high-traffic corridors.

Select * from airport;
select * from flight;
use flight_analysis;
select
  f.Origin_Airport_ID,
  f.Dest_Airport_Id,
  a1.City_Name as Orgin_City,
  a2.City_Name as Dest_City,
  count(*) as Total_Flight
from flight f
join airport a1 on f.Origin_Airport_ID = a1.Airport_Id
join airport a2 on f.Dest_Airport_Id = a2.Airport_Id
group by f.Origin_Airport_ID, f.Dest_Airport_Id
order by Total_Flight desc
limit 10;

## Los Angels is a part of The top 10 busiest air routes.

### Compare passenger numbers across origin cities to identify top-performing airports

## Total Passengers and Total No. of Flights

select * from flight;
select * from flight_metrics;
select * from airport;

select
  a.City_Name as Origin_City,
  sum(fm.Passengers) as Total_Passengers,
  count(f.Flight_ID)  as Total_Flight
from flight f
join flight_metrics fm on f.Flight_ID = fm.Flight_Id
join airport a on f.Origin_Airport_ID = a.airport_id
group by a.City_Name
order by Total_Flight desc;

## Destination city
select
  a.City_Name as Dest_City,
  sum(fm.Passengers) as Total_Passsengers,
  count(f.Flight_ID) as Total_Flight
from flight f
join flight_metrics fm on f.Flight_ID = fm.Flight_Id
join airport a on f.Dest_Airport_Id = a.Airport_Id
group by a.City_Name
order by Total_Flight desc
limit 10;


