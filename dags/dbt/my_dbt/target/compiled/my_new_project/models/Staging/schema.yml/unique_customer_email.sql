
    
    

select
    email as unique_field,
    count(*) as n_records

from "postgres"."public"."customer"
where email is not null
group by email
having count(*) > 1


