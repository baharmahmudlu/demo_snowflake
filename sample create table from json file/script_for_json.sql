-- First we upload the file to snowflake internal: database -> schema -> create btn -> stage btn -> snowflake managed btn -> upload


-- Create an Ingestion Table for JSON Data
create table library_card_catalog.public.author_ingest_json
(
  raw_author variant
);


--Create File Format for JSON Data
create or replace file format library_card_catalog.public.json_file_format
type = 'JSON'
compression = 'AUTO'
strip_outer_array = TRUE;


-- copy data from stage file into table
copy into AUTHOR_INGEST_JSON
from @util_db.public.my_internal_stage
files=('author_with_header.json')
file_format=(format_name=LIBRARY_CARD_CATALOG.PUBLIC.JSON_FILE_FORMAT);


-- select
select * from AUTHOR_INGEST_JSON;


-- select returns AUTHOR_UID value from top-level object's attribute
select raw_author:AUTHOR_UID
from author_ingest_json;

-- select returns the data in a way that makes it look like a normalized table
SELECT
 raw_author:AUTHOR_UID
,raw_author:FIRST_NAME::STRING as FIRST_NAME
,raw_author:MIDDLE_NAME::STRING as MIDDLE_NAME
,raw_author:LAST_NAME::STRING as LAST_NAME
FROM AUTHOR_INGEST_JSON;