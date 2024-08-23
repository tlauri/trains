from rest_api import rest_api_source
import dlt

digitraffic_url = "https://rata.digitraffic.fi/api/v1/"

trains_config = {
    "client": {
        "base_url": digitraffic_url,
    },
    "resource_defaults": {
        "primary_key": "trainNumber",
        "write_disposition": "merge"
        },
    "resources": [
        {
            "name": "hki_trains",
            "endpoint": {
                "path": "live-trains/station/HKI"
            },
        }
    ],
}

trains_source = rest_api_source(trains_config)

if __name__=='__main__':
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name='digitraffic_trains',
        destination=dlt.destinations.duckdb('dt_trains.db'),
        dataset_name='digitraffic_trains'
    )

    pipeline.run(trains_source)