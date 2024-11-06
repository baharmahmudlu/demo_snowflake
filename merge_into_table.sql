
create or replace task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
	warehouse=COMPUTE_WH
	schedule='5 minute'
	as 
    INSERT INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
SELECT logs.ip_address
, logs.user_login as GAMER_NAME
, logs.user_event as GAME_EVENT_NAME
, logs.datetime_iso8601 as GAME_EVENT_UTC
, city
, region
, country
, timezone as GAMER_LTZ_NAME
, convert_timezone('UTC', timezone, logs.datetime_iso8601) as GAME_EVENT_LTZ 
, dayname(GAME_EVENT_LTZ) as dow_name
, tod.tod_name
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
join time_of_day_lu tod
on tod.hour = hour(GAME_EVENT_LTZ);


select count(*)
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;


--Run the task to load more rows
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--check to see how many rows were added (if any!)
select count(*)
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;




truncate table ags_game_audience.enhanced.LOGS_ENHANCED;

--then we put them all back in
INSERT INTO ags_game_audience.enhanced.LOGS_ENHANCED (
SELECT logs.ip_address 
, logs.user_login as GAMER_NAME
, logs.user_event as GAME_EVENT_NAME
, logs.datetime_iso8601 as GAME_EVENT_UTC
, city
, region
, country
, timezone as GAMER_LTZ_NAME
, CONVERT_TIMEZONE( 'UTC',timezone,logs.datetime_iso8601) as game_event_ltz
, DAYNAME(game_event_ltz) as DOW_NAME
, TOD_NAME
from ags_game_audience.raw.LOGS logs
JOIN ipinfo_geoloc.demo.location loc 
ON ipinfo_geoloc.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND ipinfo_geoloc.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
JOIN ags_game_audience.raw.TIME_OF_DAY_LU tod
ON HOUR(game_event_ltz) = tod.hour);


create table ags_game_audience.enhanced.LOGS_ENHANCED_UF 
clone ags_game_audience.enhanced.LOGS_ENHANCED;

MERGE INTO ENHANCED.LOGS_ENHANCED e
USING RAW.LOGS r
ON r.user_login = e.GAMER_NAME
and r.datetime_iso8601 = e.game_event_utc
and r.user_event = e.game_event_name
WHEN MATCHED THEN
UPDATE SET IP_ADDRESS = 'Hey I updated matching rows!';


select * from enhanced.logs_enhanced;



MERGE INTO ENHANCED.LOGS_ENHANCED e
USING (SELECT logs.ip_address 
    , logs.user_login as GAMER_NAME
    , logs.user_event as GAME_EVENT_NAME
    , logs.datetime_iso8601 as GAME_EVENT_UTC
    , city
    , region
    , country
    , timezone as GAMER_LTZ_NAME
    , CONVERT_TIMEZONE( 'UTC',timezone,logs.datetime_iso8601) as game_event_ltz
    , DAYNAME(game_event_ltz) as DOW_NAME
    , TOD_NAME
    from ags_game_audience.raw.LOGS logs
    JOIN ipinfo_geoloc.demo.location loc 
    ON ipinfo_geoloc.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
    AND ipinfo_geoloc.public.TO_INT(logs.ip_address) 
    BETWEEN start_ip_int AND end_ip_int
    JOIN ags_game_audience.raw.TIME_OF_DAY_LU tod
    ON HOUR(game_event_ltz) = tod.hour) r --we'll put our fancy select here
ON r.GAMER_NAME = e.GAMER_NAME
and r.GAME_EVENT_UTC = e.game_event_utc
and r.GAME_EVENT_NAME = e.game_event_name
WHEN NOT MATCHED THEN
insert (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME) --list of columns
values (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME) --list of columns (but we can mark as coming from the r select)
;



truncate table AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

create or replace task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
	warehouse=COMPUTE_WH
	schedule='5 minute'
	as 
MERGE INTO ENHANCED.LOGS_ENHANCED e
USING (SELECT logs.ip_address 
    , logs.user_login as GAMER_NAME
    , logs.user_event as GAME_EVENT_NAME
    , logs.datetime_iso8601 as GAME_EVENT_UTC
    , city
    , region
    , country
    , timezone as GAMER_LTZ_NAME
    , CONVERT_TIMEZONE( 'UTC',timezone,logs.datetime_iso8601) as game_event_ltz
    , DAYNAME(game_event_ltz) as DOW_NAME
    , TOD_NAME
    from ags_game_audience.raw.LOGS logs
    JOIN ipinfo_geoloc.demo.location loc 
    ON ipinfo_geoloc.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
    AND ipinfo_geoloc.public.TO_INT(logs.ip_address) 
    BETWEEN start_ip_int AND end_ip_int
    JOIN ags_game_audience.raw.TIME_OF_DAY_LU tod
    ON HOUR(game_event_ltz) = tod.hour) r --we'll put our fancy select here
ON r.GAMER_NAME = e.GAMER_NAME
and r.GAME_EVENT_UTC = e.game_event_utc
and r.GAME_EVENT_NAME = e.game_event_name
WHEN NOT MATCHED THEN
insert (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME) --list of columns
values (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME) --list of columns (but we can mark as coming from the r select)
;


EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED; 

select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

INSERT INTO ags_game_audience.raw.game_logs 
select PARSE_JSON('{"datetime_iso8601":"2025-01-01 00:00:00.000", "ip_address":"196.197.196.255", "user_event":"fake event", "user_login":"fake user"}');
EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;
delete from ags_game_audience.raw.game_logs where raw_log like '%fake user%';

delete from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
where gamer_name = 'fake user';

select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED; 


create or replace view AGS_GAME_AUDIENCE.RAW.LOGS(
	IP_ADDRESS,
	DATETIME_ISO8601,
	USER_EVENT,
	USER_LOGIN,
	RAW_LOG
) as
select 
RAW_LOG:ip_address::text as IP_ADDRESS
,RAW_LOG:datetime_iso8601::timestamp_ntz as datetime_iso8601
,RAW_LOG:user_event::text as user_event
,RAW_LOG:user_login::text as user_login
,*
from game_logs
where raw_log:agent is null;
;

create table AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS
(RAW_LOG VARIANT);

use database ags_game_audience;
use schema raw;
copy into ags_game_audience.raw.pl_game_logs
from @UNI_KISHORE_PIPELINE
file_format = (format_name = ags_game_audience.raw.ff_json_logs);


execute task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES;

create or replace view AGS_GAME_AUDIENCE.RAW.PL_LOGS(
	IP_ADDRESS,
	DATETIME_ISO8601,
	USER_EVENT,
	USER_LOGIN,
	RAW_LOG
) as
select 
RAW_LOG:ip_address::text as IP_ADDRESS
,RAW_LOG:datetime_iso8601::timestamp_ntz as datetime_iso8601
,RAW_LOG:user_event::text as user_event
,RAW_LOG:user_login::text as user_login
,*
from pl_game_logs
where raw_log:agent is null;

select * from PL_LOGS;


MERGE INTO ENHANCED.LOGS_ENHANCED e
USING (SELECT logs.ip_address 
    , logs.user_login as GAMER_NAME
    , logs.user_event as GAME_EVENT_NAME
    , logs.datetime_iso8601 as GAME_EVENT_UTC
    , city
    , region
    , country
    , timezone as GAMER_LTZ_NAME
    , CONVERT_TIMEZONE( 'UTC',timezone,logs.datetime_iso8601) as game_event_ltz
    , DAYNAME(game_event_ltz) as DOW_NAME
    , TOD_NAME
    from AGS_GAME_AUDIENCE.RAW.PL_LOGS logs
    JOIN ipinfo_geoloc.demo.location loc 
    ON ipinfo_geoloc.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
    AND ipinfo_geoloc.public.TO_INT(logs.ip_address) 
    BETWEEN start_ip_int AND end_ip_int
    JOIN ags_game_audience.raw.TIME_OF_DAY_LU tod
    ON HOUR(game_event_ltz) = tod.hour) r --we'll put our fancy select here
ON r.GAMER_NAME = e.GAMER_NAME
and r.GAME_EVENT_UTC = e.game_event_utc
and r.GAME_EVENT_NAME = e.game_event_name
WHEN NOT MATCHED THEN
insert (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME) --list of columns
values (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME) --list of columns (but we can mark as coming from the r select)
;

select * from ags_game_audience.enhanced.LOGS_ENHANCED;


create or replace task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
	warehouse=COMPUTE_WH
	schedule='5 minute'
	as 
MERGE INTO ENHANCED.LOGS_ENHANCED e
USING (SELECT logs.ip_address 
    , logs.user_login as GAMER_NAME
    , logs.user_event as GAME_EVENT_NAME
    , logs.datetime_iso8601 as GAME_EVENT_UTC
    , city
    , region
    , country
    , timezone as GAMER_LTZ_NAME
    , CONVERT_TIMEZONE( 'UTC',timezone,logs.datetime_iso8601) as game_event_ltz
    , DAYNAME(game_event_ltz) as DOW_NAME
    , TOD_NAME
    from AGS_GAME_AUDIENCE.RAW.PL_LOGS logs
    JOIN ipinfo_geoloc.demo.location loc 
    ON ipinfo_geoloc.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
    AND ipinfo_geoloc.public.TO_INT(logs.ip_address) 
    BETWEEN start_ip_int AND end_ip_int
    JOIN ags_game_audience.raw.TIME_OF_DAY_LU tod
    ON HOUR(game_event_ltz) = tod.hour) r --we'll put our fancy select here
ON r.GAMER_NAME = e.GAMER_NAME
and r.GAME_EVENT_UTC = e.game_event_utc
and r.GAME_EVENT_NAME = e.game_event_name
WHEN NOT MATCHED THEN
insert (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME) --list of columns
values (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME) --list of columns (but we can mark as coming from the r select)
;

truncate table AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Step 1 - how many files in the bucket?
list @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE;

--Step 2 - number of rows in raw table (should be file count x 10)
select count(*) from AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS;

--Step 3 - number of rows in raw view (should be file count x 10)
select count(*) from AGS_GAME_AUDIENCE.RAW.PL_LOGS;

--Step 4 - number of rows in enhanced table (should be file count x 10 but fewer rows is okay because not all IP addresses are available from the IPInfo share)
select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

use role accountadmin;
grant EXECUTE MANAGED TASK on account to SYSADMIN;

--switch back to sysadmin
use role sysadmin;

create warehouse USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE warehouse_size = 'XSMALL';
