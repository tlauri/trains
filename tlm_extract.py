from rest_api import rest_api_source
from rest_api import RESTAPIConfig, rest_api_resources

import dlt

digitraffic_url = "https://tie.digitraffic.fi//api/tms/v1/"
@dlt.source
def tlm_source():
    config: RESTAPIConfig = {
        "client": {
            "base_url": digitraffic_url,
        },
        "resources": [
            {
                "name": "tlm_stations",
                "write_disposition": 'merge',
                "primary_key": "id",
                "endpoint": {
                    "path": "stations",
                    "paginator": "single_page",
                    "data_selector": "features",
                },
            },
            {
                "name": "tlm_values",
                "write_disposition": 'merge',
                "primary_key": ['stationId', 'shortName', 'measuredTime'],
                "endpoint": {
                    "path": "stations/{station}/data",
                    "paginator": "single_page",
                    "params": {
                        "station": {
                            "type": "resolve",
                            "resource": "tlm_stations",
                            "field": "id",
                        }
                    },
                }
            }
        ],
    }

    yield from rest_api_resources(config)

if __name__=='__main__':
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name='traffic',
        destination=dlt.destinations.postgres(),
        dataset_name='dt_traffic'
    )

    pipeline.run(tlm_source())