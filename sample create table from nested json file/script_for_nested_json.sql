-- JSON DDL Scripts
use database library_card_catalog;
-- use role sysadmin;

-- Create an Ingestion Table for the NESTED JSON Data
create or replace table library_card_catalog.public.nested_ingest_json
(
  raw_nested_book VARIANT
);


-- copy data from stage file into table
copy into nested_ingest_json
from @util_db.public.my_internal_stage
files = ('json_book_author_nested.txt')
file_format = (format_name=LIBRARY_CARD_CATALOG.PUBLIC.JSON_FILE_FORMAT);


-- a few simple queries
select raw_nested_book
from nested_ingest_json;

select raw_nested_book:year_published
from nested_ingest_json;

select raw_nested_book:authors
from nested_ingest_json;


-- Use these example flatten commands to explore flattening the nested book and author data
select value:first_name
from nested_ingest_json
,lateral flatten(input => raw_nested_book:authors);


select value:first_name
from nested_ingest_json
,table(flatten(raw_nested_book:authors));


-- Add a CAST command to the fields returned
SELECT value:first_name::varchar, value:last_name::varchar
from nested_ingest_json
,lateral flatten(input => raw_nested_book:authors);


-- Assign new column  names to the columns using "AS"
select value:first_name::varchar as first_nm
, value:last_name::varchar as last_nm
from nested_ingest_json
,lateral flatten(input => raw_nested_book:authors);