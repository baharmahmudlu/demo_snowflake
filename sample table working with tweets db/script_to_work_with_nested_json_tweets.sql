create database SOCIAL_MEDIA_FLOODGATES;
use database SOCIAL_MEDIA_FLOODGATES;

create or replace table TWEET_INGEST (
RAW_STATUS variant
);

copy into TWEET_INGEST
from @util_db.public.my_internal_stage
files = ('nutrition_tweets.json')
file_format = (format_name=LIBRARY_CARD_CATALOG.PUBLIC.JSON_FILE_FORMAT);


-- simple select statements -- are you seeing 9 rows?
select raw_status
from tweet_ingest;

select raw_status:entities
from tweet_ingest;

select raw_status:entities:hashtags
from tweet_ingest;

-- Explore looking at specific hashtags by adding bracketed numbers
-- This query returns just the first hashtag in each tweet
select raw_status:entities:hashtags[0].text
from tweet_ingest;

-- This version adds a WHERE clause to get rid of any tweet that
-- doesn't include any hashtags
select raw_status:entities:hashtags[0].text
from tweet_ingest
where raw_status:entities:hashtags[0].text is not null;

-- Perform a simple CAST on the created_at key
-- Add an ORDER BY clause to sort by the tweet's creation date
select raw_status:created_at::date
from tweet_ingest
order by raw_status:created_at::date;


-- Flatten statements can return nested entities only (and ignore the higher level objects)
select value
from tweet_ingest
,lateral flatten
(input => raw_status:entities:urls);

select value
from tweet_ingest
,table(flatten(raw_status:entities:urls));


-- Flatten and return just the hashtag text, CAST the text as VARCHAR
select value:text::varchar as hashtag_used
from tweet_ingest
,lateral flatten
(input => raw_status:entities:hashtags);

-- Add the Tweet ID and User ID to the returned table so we could join the hashtag back to it's source tweet
select raw_status:user:name::text as user_name
,raw_status:id as tweet_id
,value:text::varchar as hashtag_used
from tweet_ingest
,lateral flatten
(input => raw_status:entities:hashtags);


-- CREATE A VIEW FROM TWEET_INGEST DATA - the URL Data Looking "Normalized"
create or replace view social_media_floodgates.public.urls_normalized as
(select raw_status:user:name::text as user_name
,raw_status:id as tweet_id
,value:display_url:text as url_used
from tweet_ingest
,lateral flatten
(input => raw_status:entities:urls)
);


--  CREATE A VIEW FROM TWEET_INGEST DATA - Makes the Hashtag Data Appear Normalized
create or replace view social_media_floodgates.public.hashtags_normalized as
(select raw_status:user:name::text as user_name
,raw_status:id as tweet_id
,value:text::varchar as hastag_used
from tweet_ingest
,lateral flatten
(input => raw_status:entities:hashtags)
);

select * from social_media_floodgates.public.hashtags_normalized;


