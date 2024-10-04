select {{ dbt_utils.generate_surrogate_key(['tms_values.id']) }} as parameter_key,
        *
from {{ source("digitraffic", "tms_values")}} tms_values
