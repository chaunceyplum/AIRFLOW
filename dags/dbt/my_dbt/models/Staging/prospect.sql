







{{ config(materialized='table') }}

with ALL_PERSON_DATA as (
    select * from person

)

with ALL_TRANSACTIONS_DATA as (

    select fk_customer_id from transactions
)

with ALL_CUSTOMER_DATA as (
    select fk_customer_id, fk_person_id from customer
    left join ALL_PERSON_DATA on ALL_PERSON_DATA.person_id=customer.fk_person_id

)


select fk_customer_id from ALL_TRANSACTIONS_DATA
LEFT JOIN ALL_CUSTOMER_DATA ON ALL_CUSTOMER_DATA.customer_id = ALL_TRANSACTIONS_DATA.fk_customer_id