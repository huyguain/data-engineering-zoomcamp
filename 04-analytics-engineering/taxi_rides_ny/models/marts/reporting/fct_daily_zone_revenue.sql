select
    -- Grouping dimensions
    coalesce(pickup_zone, 'Unknown Zone') as pickup_zone,
    {% if target.type == 'bigquery' %}cast(date_trunc(pickup_datetime, day) as date)
    {% elif target.type == 'duckdb' %}date_trunc('day', pickup_datetime)
    {% endif %} as revenue_day,
    service_type,

    -- Revenue breakdown (summed by zone, day, and service type)
    sum(fare_amount) as revenue_daily_fare,
    sum(extra) as revenue_daily_extra,
    sum(mta_tax) as revenue_daily_mta_tax,
    sum(tip_amount) as revenue_daily_tip_amount,
    sum(tolls_amount) as revenue_daily_tolls_amount,
    sum(ehail_fee) as revenue_daily_ehail_fee,
    sum(improvement_surcharge) as revenue_daily_improvement_surcharge,
    sum(total_amount) as revenue_daily_total_amount,

    -- Additional metrics for operational analysis
    count(trip_id) as total_daily_trips,
    avg(passenger_count) as avg_daily_passenger_count,
    avg(trip_distance) as avg_daily_trip_distance

from {{ ref('fct_trips') }}
group by pickup_zone, revenue_day, service_type