import os
from typing import List, Dict

from gtfs_builder.main import GtfsFormater

import geopandas as gpd

import json

def open_json_file(json_file_path: str) -> Dict:
    with open(json_file_path) as input_file:
        file_contents = input_file.read()
    return json.loads(file_contents)

def output_files() -> List[str]:
    return ["fake_base_lines_data.parq",
            "fake_base_stops_data.parq",
            "fake_moving_stops.parq",
            "fake_gtfsData.json",
            "fake_routeGtfsData.json"]

def remove_output_file(output_data_dir: str):
    for input_file in output_file_paths(output_data_dir):
        if os.path.isfile(input_file):
            os.remove(input_file)


def output_file_paths(output_data_dir: str) -> List[str]:
    output_files_moved = []
    for input_file in output_files():
        path = os.path.join(output_data_dir, input_file)
        output_files_moved.append(path)

    return output_files_moved


def test_data_processing_full_data_thresh_2(credentials):
    remove_output_file(credentials["output_data_dir"])
    GtfsFormater(
        study_area_name=credentials["study_area_name"],
        data_path=credentials["input_data_dir"],
        transport_modes=credentials["transport_modes"],
        date_mode=credentials["date_mode"],
        date=credentials["date"],
        build_shape_data=False,
        interpolation_threshold=500,
        output_format="file"
    )
    data_paths = output_file_paths(credentials["output_data_dir"])
    lines_data_path = data_paths[0]
    stops_data_path = data_paths[1]
    moving_stops_data_path = data_paths[2]

    stops_gtfs_data_json = data_paths[3]
    routes_gtfs_data_json = data_paths[4]

    base_lines = gpd.read_parquet(lines_data_path)
    assert base_lines.shape == (7, 8)
    assert set(base_lines.columns.tolist()) == {'shape_id', 'geometry', 'route_desc', 'route_type', 'route_short_name',
                                                'direction_id', 'route_color', 'route_text_color'}

    base_stops = gpd.read_parquet(stops_data_path)
    assert base_stops.shape == (8, 8)
    assert set(base_stops.columns.tolist()) == {'stop_code', 'geometry', 'stop_name', 'route_short_name', 'route_desc',
                                                'route_type', 'route_color', 'route_text_color'}

    moving_stops = gpd.read_parquet(moving_stops_data_path)
    assert moving_stops.shape == (407, 11)
    assert set(moving_stops.columns.tolist()) == {"shape_id", 'start_date', 'end_date', "geometry", 'stop_code', 'x',
                                                  'y', 'stop_name', 'route_type', 'route_long_name', 'route_short_name'}

    stops_data = open_json_file(stops_gtfs_data_json)
    assert set(stops_data[0].keys()) == {'x', 'start_date', 'end_date', 'shape_id', 'y', 'route_long_name', 'route_id',
                                         'index', 'route_type'}

    routes_data = open_json_file(routes_gtfs_data_json)
    assert set(routes_data[0].keys()) == {'route_long_name', 'route_id'}

    remove_output_file(credentials["output_data_dir"])


def test_data_processing_full_data_calendar_dates(credentials):
    remove_output_file(credentials["output_data_dir"])
    GtfsFormater(
        study_area_name=credentials["study_area_name"],
        data_path=credentials["input_data_dir"],
        transport_modes=credentials["transport_modes"],
        date_mode="calendar_dates",
        date=credentials["date"],
        build_shape_data=False,
        interpolation_threshold=500,
        output_format="file"
    )
    data_paths = output_file_paths(credentials["output_data_dir"])
    lines_data_path = data_paths[0]
    stops_data_path = data_paths[1]
    moving_stops_data_path = data_paths[2]

    stops_gtfs_data_json = data_paths[3]
    routes_gtfs_data_json = data_paths[4]

    base_lines = gpd.read_parquet(lines_data_path)
    assert base_lines.shape == (7, 8)
    assert set(base_lines.columns.tolist()) == {'shape_id', 'geometry', 'route_desc', 'route_type', 'route_short_name',
                                                'direction_id', 'route_color', 'route_text_color'}

    base_stops = gpd.read_parquet(stops_data_path)
    assert base_stops.shape == (8, 8)
    assert set(base_stops.columns.tolist()) == {'stop_code', 'geometry', 'stop_name', 'route_short_name', 'route_desc',
                                                'route_type', 'route_color', 'route_text_color'}

    moving_stops = gpd.read_parquet(moving_stops_data_path)
    assert moving_stops.shape == (407, 11)
    assert set(moving_stops.columns.tolist()) == {"shape_id", 'start_date', 'end_date', "geometry", 'stop_code', 'x',
                                                  'y', 'stop_name', 'route_type', 'route_long_name', 'route_short_name'}

    stops_data = open_json_file(stops_gtfs_data_json)
    assert set(stops_data[0].keys()) == {'x', 'start_date', 'end_date', 'shape_id', 'y', 'route_long_name', 'route_id',
                                         'index', 'route_type'}

    routes_data = open_json_file(routes_gtfs_data_json)
    assert set(routes_data[0].keys()) == {'route_long_name', 'route_id'}

    remove_output_file(credentials["output_data_dir"])


def test_data_processing_with_shape_id_computed(credentials):
    remove_output_file(credentials["output_data_dir"])

    GtfsFormater(
        study_area_name=credentials["study_area_name"],
        data_path=credentials["input_data_dir"],
        transport_modes=credentials["transport_modes"],
        date_mode=credentials["date_mode"],
        date=credentials["date"],
        build_shape_data=True,
        interpolation_threshold=200,
        output_format="file"
    )
    data_paths = output_file_paths(credentials["output_data_dir"])
    lines_data_path = data_paths[0]
    stops_data_path = data_paths[1]
    moving_stops_data_path = data_paths[2]

    stops_gtfs_data_json = data_paths[3]
    routes_gtfs_data_json = data_paths[4]

    base_lines = gpd.read_parquet(lines_data_path)
    assert base_lines.shape == (7, 8)
    assert set(base_lines.columns.tolist()) == {'shape_id', 'geometry', 'route_desc', 'route_type', 'route_short_name',
                                                'direction_id', 'route_color', 'route_text_color'}

    base_stops = gpd.read_parquet(stops_data_path)
    assert base_stops.shape == (8, 8)
    assert set(base_stops.columns.tolist()) == {'stop_code', 'geometry', 'stop_name', 'route_short_name', 'route_desc',
                                                'route_type', 'route_color', 'route_text_color'}

    moving_stops = gpd.read_parquet(moving_stops_data_path)
    assert moving_stops.shape == (735, 11)
    assert set(moving_stops.columns.tolist()) == {"shape_id", 'start_date', 'end_date', "geometry", 'stop_code', 'x',
                                                  'y', 'stop_name', 'route_type', 'route_long_name', 'route_short_name'}

    stops_data = open_json_file(stops_gtfs_data_json)
    assert set(stops_data[0].keys()) == {'x', 'start_date', 'end_date', 'shape_id', 'y', 'route_long_name', 'route_id',
                                         'index', 'route_type'}

    routes_data = open_json_file(routes_gtfs_data_json)
    assert set(routes_data[0].keys()) == {'route_long_name', 'route_id'}

    remove_output_file(credentials["output_data_dir"])


def test_data_processing_full_data_threshold(credentials):
    # Warning let this test at the end of the file
    remove_output_file(credentials["output_data_dir"])
    GtfsFormater(
        study_area_name=credentials["study_area_name"],
        data_path=credentials["input_data_dir"],
        transport_modes=credentials["transport_modes"],
        date_mode=credentials["date_mode"],
        date="20070603",
        build_shape_data=False,
        interpolation_threshold=1000,
        output_format="file"
    )
    data_paths = output_file_paths(credentials["output_data_dir"])
    lines_data_path = data_paths[0]
    stops_data_path = data_paths[1]
    moving_stops_data_path = data_paths[2]

    stops_gtfs_data_json = data_paths[3]
    routes_gtfs_data_json = data_paths[4]

    base_lines = gpd.read_parquet(lines_data_path)
    assert base_lines.shape == (9, 8)
    assert set(base_lines.columns.tolist()) == {'shape_id', 'geometry', 'route_desc', 'route_type', 'route_short_name',
                                                'direction_id', 'route_color', 'route_text_color'}

    base_stops = gpd.read_parquet(stops_data_path)
    assert base_stops.shape == (9, 8)
    assert set(base_stops.columns.tolist()) == {'stop_code', 'geometry', 'stop_name', 'route_short_name', 'route_desc',
                                                'route_type', 'route_color', 'route_text_color'}

    moving_stops = gpd.read_parquet(moving_stops_data_path)
    assert moving_stops.shape == (256, 11)
    assert set(moving_stops.columns.tolist()) == {"shape_id", 'start_date', 'end_date', "geometry", 'stop_code', 'x',
                                                  'y', 'stop_name', 'route_type', 'route_long_name', 'route_short_name'}

    stops_data = open_json_file(stops_gtfs_data_json)
    assert set(stops_data[0].keys()) == {'x', 'start_date', 'end_date', 'shape_id', 'y', 'route_long_name', 'route_id',
                                         'index', 'route_type'}

    routes_data = open_json_file(routes_gtfs_data_json)
    assert set(routes_data[0].keys()) == {'route_long_name', 'route_id'}

    remove_output_file(credentials["output_data_dir"])
