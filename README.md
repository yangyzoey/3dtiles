# 3dtile

## Step:

### 1. Load data into Postgres
#### First, download gpkg dataset:
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

### 2. Run the prototype:

#### Preparation
Set up for your local database in database.ini

Download Cesium-1.110.zip from https://github.com/CesiumGS/cesium/releases/tag/1.110, and put in the project root directory.

Set up python environment (See requirements.txt)

#### Serve the 3D Tile on-the-fly
- Perform a coordinate transformation from EPSG:7415 (RD+NAP) to EPSG:4978, and harmonise geometries to valid polygonz. 
- Compute and prepare 3D Tiles information (normal, position, triangulated topology, and tileset structure) 
- Run the webservice and complete the tile creation. Then visualise on Cesium.

```cmd
$ python server.py
```

Now connect with a web browser to the service running on your own laptop: http://127.0.0.1:5000

#### Additional Notes:
Feel free to customise configuration for the application.
- In the input.json file, you can modify the parameters according to your requirements.
- In the server.py file, under the if __name__ == '__main__': block, you can change the parameter dataset_theme.


## Result:
![Visualisation](https://github.com/zoeysunrise/3dtile/tree/test1/9-284-556.png)


## Based on:
This prototype is based on https://github.com/bmmeijers/lis3d