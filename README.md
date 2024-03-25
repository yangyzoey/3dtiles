# 3dtile

## Step:

### 1. Load data
#### First, load 3D data into Postgres
Downloaded a tile from 3dbag.nl in geopackage format (gpkg) https://3dbag.nl/en/download?tid=9-284-556. (tile around TU Delft Aula).


#### Then, convert the 3D layer (3D Multi Polygon) with LOD 1.2 into PostGIS dump format, using ogr2ogr:
```cmd
$ ogr2ogr --config PG_USE_COPY YES -f PGDump test_9-284-556.dmp 9-284-556.gpkg -sql "SELECT * FROM lod12_3d" -nln "test_lod12_3d" -lco SCHEMA=dbuser
```

#### The command helps check the information of the dataset:
```cmd
$ ogrinfo -so 9-284-556.gpkg
```


#### Next, load the dump file into Postgres. Make sure PostGIS is enabled in the target database. This results a table 'test_lod12_3d', where the 3D geometry is stored as multipolygonz, with coordinate reference system EPSG:7415:
```sql
CREATE EXTENSION IF NOT EXISTS postgis;
```
```cmd
$ psql -d [database_name] -U [database_host] -h localhost -f test_9-284-556.dmp
```

#### Next, perform a coordinate transformation from EPSG:7415 (RD+NAP) to EPSG:4978. Harmonise geometries to valid polygonz. Execute script in Postgres:
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

### 2. Run the prototype:

#### Preparation
Set up for your local database in database.ini

Download Cesium-1.110.zip from https://github.com/CesiumGS/cesium/releases/tag/1.110, and put in the project root directory.

Set up python environment (See requirements.txt)

#### Serve the 3D Tile on-the-fly
Compute tile information (normal, position, triangulated topology, and hierarchical structure) in the Postgres. Run the webservice and complete the tile creation. Then visualise on Cesium.

```cmd
$ python server.py
```

Now connect with a webbrowser to the service running on your own laptop: http://127.0.0.1:5000

#### Additional Notes:
Feel free to customize configuration for the application.
- In the input.json file, you can modify the parameters according to your requirements.
- In the server.py file, under the if __name__ == '__main__': block, you can change the parameter dataset_theme.


## Result:
![Visualisation](https://github.com/zoeysunrise/3dtile/tree/test1/9-284-556.png)


## Based on:
This prototype is based on https://github.com/bmmeijers/lis3d

// Continue..
Explain geom_type(polygon, polygonz, etc) description in the input.json. 