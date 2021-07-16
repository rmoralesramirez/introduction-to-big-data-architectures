-- #1

create database geonames;

use geonames;

-- #2

create external table geonames_spain_raw
(geonameid string,
name string,
ascii_name string,
alternate_names string,
latitude string,
longitude string,
feature_class string,
feature_code string,
country_code string,
country_code2 string,
admin1_code string,
admin2_code string,
admin3_code string,
admin4_code string,
population string,
elevation string,
dem string,
timezone string,
modification_date string)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties ("separatorChar" = "\t")
stored as textfile
location '/user/osbdet/datalake/raw/geonames/spain/';

select * from geonames_spain_raw limit 10;

-- #3

create external table geonames_spain_postalcodes_raw
(country_code string,
postal_code string,
place_name string,
admin_name1 string,
admin_code1 string,
admin_name2 string,
admin_code2 string,
admin_name3 string,
admin_code3 string,
latitude string,
longitude string,
accuracy string)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties ("separatorChar" = "\t")
stored as textfile
location '/user/osbdet/datalake/raw/geonames/spain_postalcodes/';

select * from geonames_spain_postalcodes_raw limit 10;

-- #4 

create external table geonames_spain_std
(geonameid bigint,
name string,
ascii_name string,
alternate_names array<string>,
latitude float,
longitude float,
feature_class string,
feature_code string,
country_code string,
country_code2 array<string>,
admin1_code string,
admin2_code string,
admin3_code string,
admin4_code string,
population bigint,
elevation int,
dem int,
timezone string,
modification_date date)
stored as parquet
location '/user/osbdet/datalake/std/geonames/spain/';

select * from geonames_spain_std limit 10;

-- #5

create external table geonames_spain_postalcodes_std
(country_code string,
postal_code string,
place_name string,
admin_name1 string,
admin_code1 string,
admin_name2 string,
admin_code2 string,
admin_name3 string,
admin_code3 string,
latitude float,
longitude float,
accuracy int)
stored as parquet
location '/user/osbdet/datalake/std/geonames/spain_postalcodes/';

select * from geonames_spain_postalcodes_std limit 10;

-- #6

insert into geonames_spain_std
select
cast(geonameid as bigint),
cast(name as varchar(200)),
cast(ascii_name as varchar(200)),
split(alternate_names,','),
cast(latitude as float),
cast(longitude as float),
cast(feature_class as char(1)),
cast(feature_code as varchar(10)),
cast(country_code as char(2)),
split(country_code2,','),
cast(admin1_code as varchar(20)),
cast(admin2_code as varchar(80)),
cast(admin3_code as varchar(20)),
cast(admin4_code as varchar(20)),
cast(population as bigint),
cast(elevation as int),
cast(dem as int),
cast(timezone as varchar(40)),
from_unixtime(unix_timestamp(modification_date , 'yyyy-MM-dd'))
from geonames_spain_raw;

-- #7

insert into geonames_spain_postalcodes_std
select
cast(country_code as char(2)),
cast(postal_code as varchar(20)),
cast(place_name as varchar(180)),
cast(admin_name1 as varchar(100)),
cast(admin_code1 as varchar(20)),
cast(admin_name2 as varchar(100)),
cast(admin_code2 as varchar(20)),
cast(admin_name3 as varchar(100)),
cast(admin_code3 as varchar(20)),
cast(latitude as float),
cast(longitude as float),
cast(accuracy as int)
from geonames_spain_postalcodes_raw;

-- #8

select feature_class, count(geonameid) as total
from geonames_spain_std
group by feature_class
order by count(geonameid) desc;

-- #9

select name as top_10_most_populated_cities, population
from geonames_spain_std
where feature_class = 'P'
order by population desc
limit 10;

-- #10

select name as airports
from geonames_spain_std
where feature_code = 'AIRP';

-- #11

select distinct name as top_10_highest_mountains, elevation
from geonames_spain_std
where feature_class = 'T'
order by elevation desc
limit 10;

-- #12

select name as hospitals_in_madrid
from geonames_spain_std
where admin3_code = 28079 and feature_code = 'HSP';

-- #13

select timezone, count(geonameid) as total_cities
from geonames_spain_std
where feature_code = 'PPL'
group by timezone
order by count(geonameid) desc; 

-- #14

select admin_name1 as state, count(distinct(postal_code)) as total_postal_codes
from geonames_spain_postalcodes_std
group by admin_name1
order by total_postal_codes desc;

-- #15

-- #15a

create external table amenities_by_city_tmp
(admin3_code varchar(20),
parks int,
hospitals int,
metro_stations int,
schools int
)
stored as parquet
location '/user/osbdet/datalake/std/geonames/tmp/';

-- #15b

insert into amenities_by_city_tmp
select admin3_code,
sum(case when feature_code = 'PRK' then 1 else 0 end) as parks,
sum(case when feature_code = 'HSP' then 1 else 0 end) as hospitals,
sum(case when feature_code = 'MTRO' then 1 else 0 end) as metro_stations,
sum(case when feature_code = 'SCH' then 1 else 0 end) as schools
from geonames_spain_std
group by admin3_code
having (parks + hospitals + metro_stations + schools) > 0; 

-- #15c

select 
postal.admin_code3 as admin_code3,
postal.place_name as city,
collect_set(postal.postal_code) as postal_codes,
sum(tmp.parks + tmp.hospitals + tmp.metro_stations + tmp.schools) as amenities
from amenities_by_city_tmp tmp left outer join geonames_spain_postalcodes_std postal
on tmp.admin3_code = postal.admin_code3
group by postal.admin_code3, postal.place_name
order by amenities desc
limit 5;
