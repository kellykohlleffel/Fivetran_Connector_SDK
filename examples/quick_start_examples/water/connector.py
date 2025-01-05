import requests as rq
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op
from datetime import datetime, timedelta
from collections import defaultdict


def schema(configuration: dict):
    """
    Define the table schemas that Fivetran will use.

    Args:
        configuration (dict): Configuration settings for the connector.

    Returns:
        list: Schema definitions for each table to sync.
    """
    return [
        {
            "table": "sites",
            "primary_key": ["site_code"]
        },
        {
            "table": "measurements",
            "primary_key": ["id"]
        }
    ]


def celsius_to_fahrenheit(celsius):
    """
    Convert Celsius temperature to Fahrenheit.
    """
    try:
        celsius_float = float(celsius)
        return (celsius_float * 9/5) + 32
    except (ValueError, TypeError):
        return None


def get_recent_measurements(measurements, limit=5):
    """
    Get the most recent measurements up to the limit.

    Args:
        measurements (list): List of measurement dictionaries
        limit (int): Maximum number of measurements to return

    Returns:
        list: Most recent measurements up to the limit
    """
    sorted_measurements = sorted(
        measurements,
        key=lambda x: x.get('dateTime', ''),
        reverse=True
    )
    return sorted_measurements[:limit]


def update(configuration: dict, state: dict):
    """
    Retrieve data from the USGS Water Services API and send it to Fivetran.
    Limits the data to specified Brazos River sites for the last 10 days.

    Args:
        configuration (dict): Configuration settings for the connector.
        state (dict): Last sync state containing timestamps.
    """
    # Set up parameters for Brazos River data
    base_url = "https://waterservices.usgs.gov/nwis/iv"
    brazos_river_sites = [
        "08098450",  # Brazos River at Hearne, TX
        "08085500",  # Brazos River at Fort Griffin, TX
        "08110200",  # Brazos River at Washington, TX
        "08114000",  # Brazos River at Richmond, TX
        "08098290",  # Brazos River near Highbank, TX
        "08089000",  # Brazos River near Palo Pinto, TX
        "08082500",  # Brazos River near Seymour, TX
        "08111500"   # Brazos River near Hempstead, TX
    ]
    site_filter = ",".join(brazos_river_sites)

    # Use a 10-day window
    end_time = datetime.now()
    start_time = end_time - timedelta(days=10)

    params = {
        "format": "json",
        "sites": site_filter,
        "startDT": start_time.strftime("%Y-%m-%dT%H:%M%z"),
        "endDT": end_time.strftime("%Y-%m-%dT%H:%M%z"),
        "parameterCd": "00060,00065,00010",  # Discharge, Gauge height, Temperature
        "siteStatus": "active"
    }

    log.info(f"Fetching water data for Brazos River sites: {brazos_river_sites}...")
    response = rq.get(base_url, params=params)
    data = response.json()

    if "value" not in data:
        log.error("No data received from USGS API")
        return

    sites = data["value"].get("timeSeries", [])

    # Track processed sites and measurements
    processed_sites = set()
    site_measurements = defaultdict(lambda: defaultdict(list))

    # Collect and organize all measurements
    for series in sites:
        site_info = series.get("sourceInfo", {})
        site_code = site_info.get("siteCode", [{}])[0].get("value")

        variable = series.get("variable", {})
        parameter_code = variable.get("variableCode", [{}])[0].get("value")

        # Collect all measurements for this site/parameter combination
        measurements = series.get("values", [{}])[0].get("value", [])
        site_measurements[site_code][parameter_code].extend(measurements)

    print("\n--- Processing Water Data (5 Most Recent Readings per Parameter) ---")
    print(f"{'Site Code':<15} {'Site Name':<40} {'Parameter':<20} {'Value':<10} {'Unit':<8} {'Time':<25}")
    print("-" * 118)

    # Process the limited measurements
    for series in sites:
        site_info = series.get("sourceInfo", {})
        site_code = site_info.get("siteCode", [{}])[0].get("value")
        site_name = site_info.get("siteName", "Unknown")  # Extract site_name

        # Process site information if we haven't seen it before
        if site_code not in processed_sites:
            site_data = {
                "site_code": site_code,
                "site_name": site_name,
                "latitude": site_info.get("geoLocation", {}).get("geogLocation", {}).get("latitude"),
                "longitude": site_info.get("geoLocation", {}).get("geogLocation", {}).get("longitude"),
                "county": site_info.get("siteProperty", [{}])[0].get("value"),
                "elevation": site_info.get("elevation", {}).get("value")
            }
            yield op.upsert("sites", site_data)
            processed_sites.add(site_code)

        # Process measurements
        variable = series.get("variable", {})
        parameter_code = variable.get("variableCode", [{}])[0].get("value")
        parameter_name = variable.get("variableName")
        unit = variable.get("unit", {}).get("unitCode")

        # Get the 5 most recent measurements for this site/parameter
        recent_measurements = get_recent_measurements(
            site_measurements[site_code][parameter_code]
        )

        for value in recent_measurements:
            measurement_time = value.get("dateTime")
            measurement_value = value.get("value")

            # Convert temperature if needed
            if parameter_code == "00010":
                measurement_value = celsius_to_fahrenheit(measurement_value)
                unit = "degF"
                parameter_name = "Temperature, water, Fahrenheit"

            print(f"{site_code:<15} {site_name[:39]:<40} "
                  f"{parameter_name[:19]:<20} {measurement_value:<10} {unit:<8} {measurement_time:<25}")

            measurement_data = {
                "id": f"{site_code}_{parameter_code}_{measurement_time}",
                "site_code": site_code,
                "site_name": site_name,  # Add site_name here
                "parameter_code": parameter_code,
                "parameter_name": parameter_name,
                "value": measurement_value,
                "unit": unit,
                "measurement_time": measurement_time,
                "quality_code": value.get("qualifiers", [""])[0]
            }
            yield op.upsert("measurements", measurement_data)

    yield op.checkpoint(state={"last_sync": end_time.isoformat()})


# Create connector instance
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    print("Running the USGS Water Services connector...")
    connector.debug()
    print("Connector run complete.")



