{{
    config(
        materialized='table'
    )
}}

with fhv_tripsdata as (
    select *, 
        'fhv' as service_type
    from {{ ref('stg_fhv_tripsdata') }}
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select fhv_tripsdata.dispatching_base_num,
    fhv_tripsdata.pickup_datetime,
    fhv_tripsdata.dropoff_datetime,
    fhv_tripsdata.pulocationid,
    fhv_tripsdata.dolocationid,
    fhv_tripsdata.sr_flag,
    fhv_tripsdata.affiliated_base_number,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,
    fhv_tripsdata.service_type 
from fhv_tripsdata
inner join dim_zones as pickup_zone
on fhv_tripsdata.pulocationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_tripsdata.dolocationid = dropoff_zone.locationid

