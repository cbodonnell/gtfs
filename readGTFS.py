from zipfile import ZipFile
from io import StringIO
import pandas as pd


def zipToDataframes(file_path):
    with ZipFile(file_path) as zip:
        dataframes = {}
        for file in zip.filelist:
            file_name = file.filename
            with zip.open(file_name) as f:
                bytes = f.read()
                s = str(bytes, 'utf-8')
                data = StringIO(s)
                df = pd.read_csv(data)
                name = file_name.split('.txt')[0]
                dataframes[name] = df
                # print()
                # print(name)
                # print(df)
        return dataframes


def main():
    gtfs_dataframes = zipToDataframes('data/njt_bus_gtfs_20200128.zip')
    agency = gtfs_dataframes['agency']
    routes = gtfs_dataframes['routes']
    agency_routes = agency.join(
        routes.set_index('agency_id'),
        on='agency_id'
    )
    print(agency_routes)
    trips = gtfs_dataframes['trips']
    routes_trips = agency_routes.join(
        trips.set_index('route_id'),
        on='route_id'
    )
    print(routes_trips)
    route_94_trips = routes_trips.loc[routes_trips['route_short_name'] == '94']
    stop_times = gtfs_dataframes['stop_times']
    trips_stop_times = routes_trips.join(
        stop_times.set_index('trip_id'),
        on='trip_id'
    )
    print(trips_stop_times)
    calendar_dates = gtfs_dataframes['calendar_dates']
    trips_calendar_dates = routes_trips.join(
        calendar_dates.set_index('service_id'),
        on='service_id'
    )
    print(trips_calendar_dates)
    # frequencies = gtfs_dataframes['frequencies']
    # trips_frequencies = routes_trips.join(
    #     frequencies.set_index('trip_id'),
    #     on='trip_id'
    # )
    # print(trips_frequencies)
    shapes = gtfs_dataframes['shapes']
    shapes_trips = shapes.join(
        routes_trips.set_index('shape_id'),
        on='shape_id'
    )
    print(shapes_trips)


if __name__ == "__main__":
    main()
