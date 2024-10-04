select  loads.inserted_at,
        {{ dbt_utils.generate_surrogate_key(['tms_values.station_id']) }} as station_key,
        {{ dbt_utils.generate_surrogate_key(['tms_values.id']) }} as parameter_key,
        measured_time,
        time_window_start,
        time_window_end,
        value as parameter_value
from {{ source("digitraffic", "tms_values")}} tms_values
join {{ source("digitraffic", "_dlt_loads")}} loads
on tms_values._dlt_load_id = loads.load_id