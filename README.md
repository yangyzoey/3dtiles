# 3dtile

## Step:

### 1. Load data
#### First, load 3D data into Postgres
Downloaded a tile from 3dbag.nl in geopackage format (gpkg) https://3dbag.nl/en/download?tid=9-284-556. (tile around TU Delft Aula).


#### Then, convert the 3D layer (3D Multi Polygon) with LOD 1.2 into PostGIS dump format, using ogr2ogr:
$ ogr2ogr --config PG_USE_COPY YES -f PGDump test_9-284-556.dmp 9-284-556.gpkg -sql "SELECT * FROM lod12_3d" -nln "test_lod12_3d" -lco SCHEMA=dbuser

#### The command helps check the information of the dataset:
$ ogrinfo -so 9-284-556.gpkg


#### Next, load the dump file into Postgres. This results a table 'test_lod12_3d', where the 3D geometry is stored as multipolygonz, with coordinate reference system EPSG:7415:
$ psql -d [database_name] -U [database_host] -h localhost -f test_9-284-556.dmp


#### Next, perform a coordinate transformation from EPSG:7415 (RD+NAP) to EPSG:4978. Harmonise geometries to valid polygonz. Excute the scripts in Postgres:
```sql
DROP TABLE IF EXISTS test_epsg4978;
create table test_epsg4978 as select fid as gid, st_transform(geom, 4978) as geom4978 from dbuser.test_lod12_3d;

--Drop object table if exists
DROP TABLE IF EXISTS object_test;
--Create object table
CREATE TABLE object_test AS (
SELECT 
	ROW_NUMBER() OVER () AS id,
	geom4978 as geom
	FROM test_epsg4978  
);

--Drop face table if exists
DROP TABLE IF EXISTS face_test;
--Create face table
CREATE TABLE face_test AS (
	SELECT 
	ROW_NUMBER() OVER (ORDER BY (object_id, fid)) AS id,
	object_id,
	fid,
	polygon
	FROM
		(
		SELECT 
		ROW_NUMBER() OVER (PARTITION BY object_id ORDER BY object_id) AS fid,
		object_id,
		polygon
		FROM 
			(
			SELECT 
			ROW_NUMBER() OVER () AS object_id,
			--gid, --replace large-number gid with increamental id              
			ST_AsText((ST_DUMP(geom)).geom) AS polygon
			FROM object_test
			) AS f
		) AS ff
) ORDER BY id;

-- Update geometries with SRID 0 to SRID 4978
UPDATE face_test
SET polygon = ST_SetSRID(polygon, 4978)
WHERE ST_SRID(polygon) = 0;

-- Delete invlid polyongs
DELETE FROM face_test
WHERE ST_IsValid(polygon) = false;

DROP TABLE test_epsg4978;
```

### 2. Run the prototype

#### Preparation
Download Cesium-1.110.zip from https://github.com/CesiumGS/cesium/releases/tag/1.110, and put under path 3dtile/

Set up python environment (See requirements.txt)

#### Serve the 3D Tile on-the-fly
Compute tile information (normal, position, triangulated topology, and hierarchical structure) in the Postgres. Run the webservice and complete the tile creation. Then visualise on Cesium.

$ python tile_creator.py

$ python server.py

Now connect with a webbrowser to the service running on your own laptop: http://127.0.0.1:5000

#### Note:
You can also add your dataset theme and change parameters in the input.json file. Then specify the dataset theme in tile_creator.py


## Result
![Visualisation](https://github.com/zoeysunrise/3dtile/tree/test1/9-284-556.png)


## Based on:
This prototype is based on https://github.com/bmmeijers/lis3d

// Continue..
Add geom_type(polygon, polygonz, etc) flag in the input.json. Put step1 SQL scripts() as a function inside tile_function/creator

//