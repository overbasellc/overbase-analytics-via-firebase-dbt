select
    *
    date(timestamp_micros(fb.event_timestamp)) as event_date,
    date_diff(
        date(timestamp_micros(fb.event_timestamp)),
        date(timestamp_micros(user_first_touch_timestamp)),
        day
    ) as age,
    user_pseudo_id,
    user_id,
    event_name
    user_first_touch_timestamp as installed_date,
    struct<revenue float64, currency string>(
        user_ltv.revenue, user_ltv.currency
    ) as users_ltv,
    struct<
        type string,
        brand_name string,
        model_name string,
        marketing_name string,
        os_hardware_model string
    >(
        device.category,
        device.mobile_brand_name,
        device.mobile_model_name,
        device.mobile_marketing_name,
        device.mobile_os_hardware_model
    ) as device
    struct<operating_system string, operating_system_version string>(
        device.operating_system, device.operating_system_version
    ) as device_os
from {{ source("firebase_events", "events") }}
