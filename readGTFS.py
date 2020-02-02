from zipfile import ZipFile
from io import StringIO
import pandas as pd
from datetime import datetime, timedelta
from pytz import timezone
import pytz


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


def timeToPytz(time, time_zone):
    hms = time.split(':')
    hours = int(hms[0])
    mins = int(hms[1])
    secs = int(hms[2])
    loc_tz = timezone(time_zone)
    loc_dt = loc_tz.localize(datetime(2020, 1, 28, hours, mins, secs))
    return loc_dt


def fmtPytz(pytz, fmt):
    return pytz.strftime(fmt)


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
    route_94_trips_stop_times = route_94_trips.join(
        stop_times.set_index('trip_id'),
        on='trip_id'
    )
    print(route_94_trips_stop_times)
    route_94_trips_stop_times['arrival_time'] = route_94_trips_stop_times.apply(
        lambda x: fmtPytz(timeToPytz(x['arrival_time'], x['agency_timezone']), '%Y-%m-%d %H:%M:%S %Z%z'),
        axis=1
    )
    route_94_trips_stop_times['departure_time'] = route_94_trips_stop_times.apply(
        lambda x: fmtPytz(timeToPytz(x['departure_time'], x['agency_timezone']), '%Y-%m-%d %H:%M:%S %Z%z'),
        axis=1
    )
    print(route_94_trips_stop_times)
    stops = gtfs_dataframes['stops']
    route_94_trips_stops = route_94_trips_stop_times.join(
        stops.set_index('stop_id'),
        on='stop_id'
    )
    print(route_94_trips_stops)
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
