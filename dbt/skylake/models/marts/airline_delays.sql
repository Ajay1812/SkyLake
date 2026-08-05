-- "Which airline delays most?"

select
    OP_CARRIER,
    round(avg(ARR_DELAY), 2) as avg_delay,
    count(*) as total_flights
from {{ ref('stg_flights') }}
where ARR_DELAY > 0
group by OP_CARRIER
order by avg_delay desc
limit 10