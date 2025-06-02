
    
    

select
    email as unique_field,
    count(*) as n_records

from "postgres"."public"."prospect"
where email is not null
group by email
having count(*) > 1


