select {{ dbt_utils.generate_surrogate_key(['tms_stations.id']) }} as station_key,
        *
from {{ source("digitraffic", "tms_stations")}} tms_stations
