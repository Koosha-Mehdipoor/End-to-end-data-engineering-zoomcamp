SELECT DISTINCT payment_type, 
    
FROM {{ ref('int_trips_unioned')}}