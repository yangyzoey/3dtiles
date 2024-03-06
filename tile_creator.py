import psycopg2 as pg
# from sqlalchemy import create_engine
import numpy as np
import time
import json

from tile_function import array_coord, rotate_X,rotate_Y, triangulation, k_means, schema_update, write_all_tile, input_data
from dbconfig import config


# Load JSON file
with open('input.json', 'r') as file:
    data = json.load(file)

# specfy the dataset theme 
theme = "test" # "campus" # "37en1"  # "37en2" # "campus_lod1"

# attrib
attrib_object = {'height': 'float'}  


# Extract info
object_input = "object_{}".format(theme) 
face_input = "face_{}".format(theme) 
cnum1, cnum2 = data[theme]['cluster_number'][0], data[theme]['cluster_number'][1]
triangulation_flag = data[theme]['triangulation_flag']
pre_b3dm_flag = data[theme]['precomputed_b3dm'] #0: non-indexed; 1: indexed; -1: no pre-computed
print("triangulation_flag: ", triangulation_flag)
sql_filter = data[theme]['filter']


ge_parent = 200

# total time start
total_start_time = time.time()

# database connection
conn_params = config(filename='database.ini', section='postgresql')

conn = pg.connect(
    dbname=conn_params["database"], 
    user=conn_params["user"], 
    password=conn_params["password"],
    port=conn_params["port"], 
    host=conn_params["host"])

# drop tables if exists
cursor = conn.cursor() # Create a cursor object

sql = """
DO $$ 
BEGIN 
    -- Check if the constraint exists
    IF EXISTS (
        SELECT constraint_name 
        FROM information_schema.table_constraints 
        WHERE table_name = 'object' 
        AND constraint_name = 'object_tile_id_fkey'
    ) THEN 
        -- If the constraint exists, drop it
        ALTER TABLE object 
        DROP CONSTRAINT object_tile_id_fkey; 
    END IF; 
END $$;
        DROP TABLE IF EXISTS tile CASCADE;
"""

cursor.execute(sql)
conn.commit()


# create table object/ face/ vertex
sql =  "CREATE EXTENSION IF NOT EXISTS postgis;\
        CREATE EXTENSION IF NOT EXISTS postgis_sfcgal;\
        \
        DROP TABLE IF EXISTS property;\
        DROP TABLE IF EXISTS face;\
        DROP TABLE IF EXISTS object;\
        DROP TABLE IF EXISTS temp;\
        CREATE TABLE object(\
        id SERIAL PRIMARY KEY,\
        tile_id INT,\
        nodes DOUBLE PRECISION[],\
        LoD INT,\
        object_root INT,\
        envelope geometry,\
        UNIQUE(id)\
        );\
        CREATE TABLE face(\
        id SERIAL PRIMARY KEY,\
        fid INT,\
        tri_node_id int[]  ,\
        object_id INT REFERENCES object(id),\
        polygon geometry,\
        normal DOUBLE PRECISION[],\
        position DOUBLE PRECISION[],\
        pos_idx_test DOUBLE PRECISION[],\
        tri_index int[],\
        if_planar boolean,\
        UNIQUE(id)\
        );\
        CREATE TABLE property(\
        id integer PRIMARY KEY,\
        name TEXT,\
        type TEXT,\
        unit TEXT,\
        UNIQUE(id)\
        );\
        CREATE TABLE temp(\
        id SERIAL PRIMARY KEY,\
        object_id INT,\
        normal DOUBLE PRECISION[],\
        if_vertical boolean,\
        if_planar boolean,\
        polygon3d geometry,\
        polygon2d geometry,\
        position_reference JSONB,\
        position3d DOUBLE PRECISION[],\
        position2d DOUBLE PRECISION[],\
        triangle geometry,\
        triangle_non geometry,\
        tri_position DOUBLE PRECISION[],\
        tri_non DOUBLE PRECISION[],\
        UNIQUE(id)\
        );\
        "
cursor.execute(sql)
conn.commit()


# functioin geometric operation ST_Subtruct, ST_CrossProduct
sql = """
--Compute normal vectors for polygons and store as an array

DROP FUNCTION IF EXISTS ST_Subtruct(geometry, int, int);

CREATE OR REPLACE FUNCTION ST_Subtruct(p1 geometry, p2 geometry)
  RETURNS geometry AS
$$
DECLARE
    x1 numeric := ST_X(p1);
    y1 numeric := ST_Y(p1);
    z1 numeric := COALESCE(ST_Z(p1), 0.0);
    x2 numeric := ST_X(p2);
    y2 numeric := ST_Y(p2);
    z2 numeric := COALESCE(ST_Z(p2), 0.0);
BEGIN
    RETURN ST_SetSRID(ST_MakePoint(
        x1 - x2,
        y1 - y2,
        z1 - z2
    ), ST_SRID(p1));
END;
$$
LANGUAGE plpgsql IMMUTABLE;

DROP FUNCTION IF EXISTS ST_CrossProduct(geometry, geometry);

CREATE OR REPLACE FUNCTION ST_CrossProduct(p1 geometry, p2 geometry)
  RETURNS geometry AS
$$
DECLARE
    a1 numeric := ST_X(p1);
    a2 numeric := ST_Y(p1);
    a3 numeric := COALESCE(ST_Z(p1), 0.0);
    b1 numeric := ST_X(p2);
    b2 numeric := ST_Y(p2);
    b3 numeric := COALESCE(ST_Z(p2), 0.0);
BEGIN
    RETURN ST_SetSRID(ST_MakePoint(
        a2 * b3 - a3 * b2,
        a3 * b1 - a1 * b3,
        a1 * b2 - a2 * b1
    ), ST_SRID(p1));
END;
$$
LANGUAGE plpgsql IMMUTABLE;
"""
cursor.execute(sql)
conn.commit()


# map dataset to the face table and object table
input_data(conn, cursor, object_input, face_input)


print("normalised normal start")


# functioin compute normalised normal
cursor.execute("""
-- Drop the table if it exists
DROP TABLE IF EXISTS temp_nn;

CREATE TEMP TABLE temp_nn AS
(with points AS (
SELECT id, poly, ST_AsText(linestr) AS linestr,
ST_AsText(ST_Subtruct(ST_PointN(t.linestr, 2), ST_PointN(t.linestr, 3))) AS p1,
ST_AsText(ST_Subtruct(ST_PointN(t.linestr, 2), ST_PointN(t.linestr, 1))) AS p2
FROM (
SELECT id, ST_AsText(polygon) AS poly, ST_ExteriorRing(polygon) AS linestr
FROM face
)AS t
)
SELECT id, poly, ARRAY[x,
    y,
    z ] as nn
FROM
(SELECT id, poly, ST_X(n) as x, ST_Y(n) as y, ST_Z(n) as z  FROM
(SELECT id, poly, linestr, ST_AsText(ST_CrossProduct(
p1, p2
)) AS n from points) as tn) as tnn)
;

-- Update the 'face' table with normalised normal
UPDATE face
SET normal = CASE
    WHEN (temp_nn.nn[1]*temp_nn.nn[1] + temp_nn.nn[2]*temp_nn.nn[2] + temp_nn.nn[3]*temp_nn.nn[3]) != 0 THEN
        ARRAY[
            temp_nn.nn[1] / sqrt(temp_nn.nn[1]*temp_nn.nn[1] + temp_nn.nn[2]*temp_nn.nn[2] + temp_nn.nn[3]*temp_nn.nn[3]),
            temp_nn.nn[2] / sqrt(temp_nn.nn[1]*temp_nn.nn[1] + temp_nn.nn[2]*temp_nn.nn[2] + temp_nn.nn[3]*temp_nn.nn[3]),
            temp_nn.nn[3] / sqrt(temp_nn.nn[1]*temp_nn.nn[1] + temp_nn.nn[2]*temp_nn.nn[2] + temp_nn.nn[3]*temp_nn.nn[3])
        ]
    ELSE ARRAY[0, 0, 0]
END
FROM temp_nn
WHERE face.id = temp_nn.id;


--select * from temp_nn;
""")
conn.commit()

print("normalised normal end")

cursor.execute(
"""
INSERT INTO temp (object_id, id)
SELECT object_id, id
FROM face;
UPDATE temp AS t
SET normal = f.normal
FROM face AS f
WHERE t.id = f.id;
"""
)
conn.commit()


# normal flag vertical or not
epsilon = 1e-6  
sql_nor = f"""
    UPDATE temp
    SET if_vertical = CASE 
        WHEN ABS(normal[3]) <= {epsilon} THEN true 
        ELSE false
    END;
"""
cursor.execute(sql_nor)
conn.commit()


# update table property
# attribute_toatal = {**attrib_object}
# items = list(attribute_toatal.items())
# # print(type(items))
# for i in range(len(items)):  
#     # print(len(attribute_toatal))
#     # print(i)
#     sql =  "INSERT INTO property(id, name, type) VALUES({0}, '{1}', '{2}');".format(i+1, items[i][0], items[i][1])
#     # print(sql)
#     cursor.execute(sql)
#     conn.commit()


# update table object  
items = list(attrib_object.items())
# print(type(items))
for i in range(len(items)):
    # print(len(attrib_object))
    # print(i)
    sql =  "ALTER TABLE object ADD {0} {1};".format(items[i][0], items[i][1])
    # print(sql)
    cursor.execute(sql)
    conn.commit()


# operation on table temp
cursor.execute("""
-- Drop the table if it exists
DROP TABLE IF EXISTS h;

CREATE TEMP TABLE h AS
With e as
(
SELECT object_id, ST_3DExtent(f.polygon) as envelope
FROM face AS f
GROUP BY object_id
)
SELECT object_id, (ST_ZMax(e.envelope) - ST_ZMin(e.envelope)) AS height
FROM e;

UPDATE object SET height = h.height FROM h WHERE id = object_id;


--store rotated polygon3d
UPDATE temp SET polygon3d = face.polygon FROM face where temp.id = face.id and if_vertical = false;
UPDATE temp SET polygon3d = ST_RotateY(ST_RotateX(face.polygon, pi()/6), pi()/6) FROM face \
where temp.id = face.id and if_vertical = true;

UPDATE temp
SET if_planar = ST_IsPlanar(polygon3d);

--UPDATE face
--SET if_planar = temp.if_planar
--FROM temp
--WHERE face.id = temp.id;
""")
conn.commit()


#-------------------------------------------------------------triangulation start--------------------------------------------------------------------------------------------
print("triangulation start")
start_time = time.time()


triangulation(conn, cursor, triangulation_flag)

end_time = time.time()
tri_execution_time = end_time - start_time
print(f"triangulation end, execution time: {tri_execution_time} seconds")
# ---------------------------------------------------------------------------triangulation end--------------------------------------------------------------------------------------

# Update table object envelope
cursor.execute("""
UPDATE object AS o
SET envelope = (
    SELECT ST_3DExtent(f.polygon)
    FROM face AS f
    WHERE f.object_id = o.id
);
""")
conn.commit() 

# # Create tables tile
cursor.execute("""
    CREATE TABLE IF NOT EXISTS tile (
        id SERIAL PRIMARY KEY,
        tileset_id INT,
        parent_id INT REFERENCES tile(id),
        bounding_volume double precision[],
        geometric_error INT,
        refine TEXT,
        content TEXT,
        b3dm BYTEA
    )
""")
# BYTEA:"byte array", store binary data as a variable-length array of bytes
conn.commit() 


print("clustering start")
# cluster objects to different tiles
k_means(conn, cursor, cnum1, cnum2) 
print("clustering end")


# INSERT statement to update the id in the tile table
t1 = 1
tileset = 1
sql = "INSERT INTO tile (id, geometric_error, refine) VALUES({0}, {2}, 'ADD');\
INSERT INTO tile (id) SELECT row_number FROM hierarchy WHERE level = 2 and row_number != 1;\
UPDATE tile SET tileset_id = {1} WHERE id IS NOT NULL;\
UPDATE tile SET parent_id = {0} \
WHERE id IN (SELECT row_number FROM hierarchy WHERE level = 2) and id != 1;".format(t1,tileset, ge_parent)
cursor.execute(sql)
conn.commit() 


# UPDATE bounding_volume for children in the tile table
cursor.execute("""
DROP TABLE IF EXISTS bv;

CREATE TEMP TABLE bv AS
SELECT
    row_number AS tile_id,
    h_envelope AS combined_bbx
FROM hierarchy
WHERE level = 2
;

UPDATE tile AS t
SET bounding_volume = (
SELECT
    ARRAY[
        (ST_XMin(combined_bbx) + ST_XMax(combined_bbx)) / 2, -- centerX
        (ST_YMin(combined_bbx) + ST_YMax(combined_bbx)) / 2, -- centerY
        (ST_ZMin(combined_bbx) + ST_ZMax(combined_bbx)) / 2, -- centerZ
        (ST_XMax(combined_bbx) - ST_XMin(combined_bbx)) / 2, 0, 0, -- halfX
		0,(ST_YMax(combined_bbx) - ST_YMin(combined_bbx)) / 2, 0, -- halfY
		0,0,(ST_ZMax(combined_bbx) - ST_ZMin(combined_bbx)) / 2  -- halfZ
    ]
FROM bv
WHERE bv.tile_id = t.id
);
""")
conn.commit() 


# UPDATE geometric_error in the tile table
cursor.execute("""
UPDATE tile SET geometric_error = 0 WHERE parent_id IS NOT NULL;
""")
conn.commit() 


# UPDATE tile_id in the object table
cursor.execute("""
WITH UnnestedIDs AS (
    SELECT unnest(object_id) AS id, row_number
    FROM hierarchy
    WHERE level = (SELECT level FROM hierarchy ORDER BY level DESC LIMIT 1)
)
UPDATE object AS o
SET tile_id = ui.row_number
FROM UnnestedIDs AS ui
WHERE o.id = ui.id;
""")
conn.commit() 


# UPDATE bounding volume for parent the tile table
cursor.execute("""
DROP TABLE IF EXISTS bv_root;

CREATE TEMP TABLE bv_root AS
WITH bbx AS (
    SELECT
        ST_3DExtent(f.polygon) AS combined_bbx
    FROM face AS f
)
SELECT
combined_bbx
FROM bbx;


UPDATE tile SET
bounding_volume = (
SELECT
    ARRAY[
        (ST_XMin(combined_bbx) + ST_XMax(combined_bbx)) / 2, -- centerX
        (ST_YMin(combined_bbx) + ST_YMax(combined_bbx)) / 2, -- centerY
        (ST_ZMin(combined_bbx) + ST_ZMax(combined_bbx)) / 2, -- centerZ
        (ST_XMax(combined_bbx) - ST_XMin(combined_bbx)) / 2, 0, 0, -- halfX
		0,(ST_YMax(combined_bbx) - ST_YMin(combined_bbx)) / 2, 0, -- halfY
		0,0,(ST_ZMax(combined_bbx) - ST_ZMin(combined_bbx)) / 2  -- halfZ
    ]
FROM bv_root
);
update tile set content = id::text;

""")
conn.commit() 



# covert coord to node_idx, update table face.tri_node_id, object.nodes
print("Coord to node_idx start")
start_time = time.time()

sql = "SELECT id FROM tile;"
cursor.execute(sql)
results = cursor.fetchall()
# print(results)
conn.commit() 

tid_list= [int(i[0]) for i in results]
# print(tid_list)
array_coord(conn, cursor)

end_time = time.time()
execution_time = end_time - start_time
print(f"Coord to node_idx end, execution time: {execution_time} seconds")



# pre-computed b3dm in DB
print("pre-computed b3dm stored in DB start")
start_time = time.time()

write_all_tile(conn, cursor, pre_b3dm_flag, tid_list, sql_filter)

end_time = time.time()
execution_time = end_time - start_time
print(f"Pre-computatioin end, execution time: {execution_time} seconds")



# delete unnecessary tables
schema_update(conn, cursor)


# total time end
total_end_time = time.time()
execution_time = total_end_time - total_start_time
print(f"Total execution time: {execution_time} seconds")

# 



if __name__ == "__main__":
    pass