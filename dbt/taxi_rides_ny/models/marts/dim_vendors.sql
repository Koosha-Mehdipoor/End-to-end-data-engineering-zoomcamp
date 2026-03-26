SELECT 
    DISTINCT vendor_id,
    --vendor_name created as macro since maybe later would be needed to be used in other models or changed
    {{ get_vendor_data('vendor_id') }} AS vendor_name

FROM {{ ref('int_trips_unioned') }}