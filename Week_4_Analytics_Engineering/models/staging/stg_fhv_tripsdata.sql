{{ config(materialized="view") }}

with
    tripdata as (
        select *,
        from {{ source("staging", "fhv_tripsdata") }}
        where extract(year from pickup_datetime) = 2019
    )

-- Use the UPDATE statement to set null values in sr_flag column to zero
select
    dispatching_base_num,
    pickup_datetime,
    dropoff_datetime,
    pulocationid,
    dolocationid,
    ifnull(sr_flag, '0') as sr_flag,
    {{ dbt.safe_cast("sr_flag", api.Column.translate_type("integer")) }} as new_sr_flag,
    affiliated_base_number

from tripdata
