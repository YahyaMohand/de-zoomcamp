{{ config(materialized='table')}}

select 
    cast(dispatching_base_num as text) as dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast("dropOff_datetime" as timestamp) as dropoff_datetime,
    cast("PUlocationID" as integer) as pickup_locationid,
    cast("DOlocationID" as integer) as dropoff_locationid,
    cast("SR_Flag" as integer) as sr_flag,
    cast("Affiliated_base_number" as text) as affiliated_base_number
from {{ source('staging', 'fhv_2019')}}

{% if var('is_test_run', default=true) %}

limit 100

{% endif %}