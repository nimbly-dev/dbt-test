{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select *,
    row_number() over(partition by fhpickup_datetime) as rn
  from {{ source('staging','fhv_cab_data') }}
  where fhpickup_datetime is not null 
)
select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dwid', 'fhpickup_datetime']) }} as tripid,
    cast(dispatching_base_num as string) as vendorid,
    cast(pulocationid as integer) as pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    
    -- timestamps
    cast(fhpickup_datetime as timestamp) as pickup_datetime,
    cast(fhdropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    cast(sr_flag as numeric) as sr_flag,
    cast(affiliated_base_number as string) as affiliated_base_number
from tripdata
where rn = 1
