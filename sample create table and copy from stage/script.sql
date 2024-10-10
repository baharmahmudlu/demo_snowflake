-- First we upload the file to snowflake internal: database -> schema -> create btn -> stage btn -> snowflake managed btn -> upload

-- use database.schema
use database garden_plants;
use schema veggies;

--create or replace table
create or replace table VEGETABLE_DETAILS_PLANT_HEIGHT (
    plant_name varchar(25),
    UOM varchar(1),
    Low_End_of_Range number(2,0),
    High_End_of_Range number(2,0)
);

-- select from stage file
select $1, $2, $3, $4
from @util_db.public.my_internal_stage/veg_plant_height.csv
(file_format => GARDEN_PLANTS.VEGGIES.COMMASEP_DBLQUOT_ONEHEADROW);

-- copy data from stage file into table
copy into VEGETABLE_DETAILS_PLANT_HEIGHT
from @util_db.public.my_internal_stage
files=('veg_plant_height.csv')
file_format=(format_name=GARDEN_PLANTS.VEGGIES.COMMASEP_DBLQUOT_ONEHEADROW);

-- select
select * from VEGETABLE_DETAILS_PLANT_HEIGHT;