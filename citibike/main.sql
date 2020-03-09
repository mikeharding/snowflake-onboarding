-- List all trips in the trips share
list @trips/;


-- List 2019 trips in the trips share
list @trips/2019;

select count(*) from trips;

-- Mike will load trips

select count(*) from trips;

select * from trips limit 20;

/*--------------------------------------------------------------------------------
  Get some answers!
--------------------------------------------------------------------------------*/

-- trip records link to our lookup tables
select * from stations;
select * from programs;

-- in hourly groups, how many trips were taken, how long did they last, and
-- how far did they ride?
select
  date_trunc(hour, starttime) as "Hour",
  count(*) as "Num Trips",
  avg(tripduration)/60 as "Avg Duration (mins)",
  avg(haversine(ss.station_latitude, ss.station_longitude, es.station_latitude, es.station_longitude)) as "Avg Distance (Km)"
from trips t inner join stations ss on t.start_station_id = ss.station_id
             inner join stations es on t.end_station_id = es.station_id
where start_station_id < 200
group by 1
order by 4 desc;


/*--------------------------------------------------------------------------------
  Let's make the demo a bit easier to read going forward...
  We create a view joining TRIPS to STATIONS and PROGRAMS
--------------------------------------------------------------------------------*/

create or replace secure view trips_vw as
select tripduration, starttime, stoptime,
       ss.station_name start_station_name, ss.station_latitude start_station_latitude, ss.station_longitude start_station_longitude,
       es.station_name end_station_name, es.station_latitude end_station_latitude, es.station_longitude end_station_longitude,
       bikeid, usertype, birth_year, gender, program_name
from trips t inner join stations ss on t.start_station_id = ss.station_id
             inner join stations es on t.end_station_id = es.station_id
             inner join programs p on t.program_id = p.program_id;

select * from trips_vw where year(starttime)=2019 limit 20;

/*--------------------------------------------------------------------------------
  Instead of writing SQL, Jane will query it via our BI toolset
  
  Let's create Warehouses for the Analysts
--------------------------------------------------------------------------------*/

create or replace warehouse bi_medium_wh
  with warehouse_size = 'medium'
  auto_suspend = 300
  auto_resume = true
  min_cluster_count = 1
  max_cluster_count = 5
  initially_suspended = true;

grant all on warehouse bi_medium_wh to role analyst_citibike;

create or replace warehouse bi_large_wh
  with warehouse_size = 'large'
  auto_suspend = 300
  auto_resume = true
  min_cluster_count = 1
  max_cluster_count = 5
  initially_suspended = true;

grant all on warehouse bi_large_wh to role analyst_citibike;

/*--------------------------------------------------------------------------------
  LOAD WEATHER V2

  #2 in the core demo flow.
  Run this in your demo account.

  This script loads the WEATHER table from staged JSON files. It shows how
  Snowflake can handle semi-structured data with similar performance to structured
  data. It also shows cloning as a way of supporting agile devops. It also shows
  how Snowflake supports "real world" SQL with CTEs and UDFs.

  Author:   Alan Eldridge
  Updated:  10 June 2019

  #weather #loading #semistructured #json #vertical #scalability #elasticity 
  #cloning #dev #devops #cte #udf #flatten
--------------------------------------------------------------------------------*/

/*--------------------------------------------------------------------------------
  We have staged the weather data, but it's in JSON format
--------------------------------------------------------------------------------*/


list @citibike.public.weather/;

select $1 from @citibike.public.weather/2019/ limit 20;


create or replace table weather_sf (v variant, t timestamp);

/*--------------------------------------------------------------------------------
  We can reference the JSON data as if it were structured
--------------------------------------------------------------------------------*/

select v, t, v:city.name::string city, v:weather[0].main::string conditions from weather
  where v:city.name = 'New York' and v:weather[0].main = 'Snow'
  limit 20;

-- and we can unwrap complex structures such as arrays via FLATTEN
-- to compare the most common weather in different cities

select value:main::string as conditions
  ,sum(iff(v:city.name::string='New York',1,0)) as nyc_freq
  ,sum(iff(v:city.name::string='Seattle',1,0)) as seattle_freq
  ,sum(iff(v:city.name::string='San Francisco',1,0)) as san_fran_freq
  ,sum(iff(v:city.name::string='Miami',1,0)) as miami_freq
  ,sum(iff(v:city.name::string='Washington, D. C.',1,0)) as wash_freq
  from weather w,
  lateral flatten (input => w.v:weather) wf
  where v:city.name in ('New York','Seattle','San Francisco','Miami','Washington, D. C.')
    and year(t) = 2019
  group by 1;

/*--------------------------------------------------------------------------------
  Create a view with trip (structured) and weather (semistructured) data
--------------------------------------------------------------------------------*/

-- note the complex SQL - we support CTEs and UDFs
create or replace secure view trips_weather_vw as (
  with
    t as (
      select date_trunc(hour, starttime) starttime, date_trunc(hour, stoptime),
        start_station_id, end_station_id, program_id, tripduration
      from trips),
    w as (
      select date_trunc(hour, t)                observation_time
        ,avg(degKtoC(v:main.temp::float))       temp_avg_c
        ,min(degKtoC(v:main.temp_min::float))   temp_min_c
        ,max(degKtoC(v:main.temp_max::float))   temp_max_c
        ,avg(degKtoF(v:main.temp::float))       temp_avg_f
        ,min(degKtoF(v:main.temp_min::float))   temp_min_f
        ,max(degKtoF(v:main.temp_max::float))   temp_max_f
        ,avg(v:wind.deg::float)                 wind_dir
        ,avg(v:wind.speed::float)               wind_speed
      from weather
      where v:city.id::int = 5128638
      group by 1)
  select starttime, start_station_id, ss.station_name start_station_name, ss.station_latitude start_station_latitude,
    ss.station_longitude start_station_longitude, end_station_id, es.station_name end_station_name,
    es.station_latitude end_station_latitude, es.station_longitude end_station_longitude, p.program_name,
    tripduration, observation_time, temp_avg_c, temp_min_c, temp_max_c, temp_avg_f, temp_min_f, temp_max_f, wind_dir, wind_speed
  from t inner join stations ss on t.start_station_id = ss.station_id
         inner join stations es on t.end_station_id = es.station_id
         inner join programs p on t.program_id = p.program_id
         left outer join w on t.starttime = w.observation_time);

-- check the results
select count(*) from trips_weather_vw;
select * from trips_weather_vw where year(observation_time) = 2019 limit 100;



