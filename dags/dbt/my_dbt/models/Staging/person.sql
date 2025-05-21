




{{ config(materialized='table') }}

with ALL_PERSON_DATA as (

    SELECT * FROM PERSON

)

select *
from ALL_PERSON_DATA