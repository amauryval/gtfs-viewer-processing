http://192.168.1.73:7000/api/v1/gtfs/nodes_by_date?current_date=2019-11-15%2005:01:00





CREATE MATERIALIZED VIEW gtfs_data.nodes as
SELECT 
	gtfs_data.stops_geom.stop_code AS stop_code,
	gtfs_data.stops_geom.stop_name AS stop_name,
	gtfs_data.stops_geom.stop_type AS stop_type,
	gtfs_data.stops_geom.line_name AS line_name,
	gtfs_data.stops_times_values.direction_id AS direction_id,
	gtfs_data.stops_times_values.trip_id AS trip_id,
	gtfs_data.stops_geom.line_name_short AS line_name_short,
	gtfs_data.stops_geom.geometry AS geometry,
	lower(gtfs_data.stops_times_values.validity_range) as start_date,
	upper(gtfs_data.stops_times_values.validity_range) as end_date
FROM 
	gtfs_data.stops_geom,
	gtfs_data.stops_times_values 
WHERE 
	gtfs_data.stops_geom.stop_code = gtfs_data.stops_times_values.stop_code 
	--AND gtfs_data.stops_times_values.validity_range @> '2019-11-15 05:01:00'::timestamp
;

CREATE INDEX ix_geom ON gtfs_data.nodes USING gist (geometry);
CREATE INDEX ix_date ON gtfs_data.nodes USING btree (start_date, end_date);
CREATE INDEX ix_trip_id ON gtfs_data.nodes USING btree (trip_id);