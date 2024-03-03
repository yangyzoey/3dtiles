import psycopg2 as pg
from sqlalchemy import create_engine
import numpy as np
import time
import json

from tile_function import write_tile, array_coord, rotate_X,rotate_Y, input_data

# Load JSON file
with open('input.json', 'r') as file:
    data = json.load(file)

# specfy the dataset theme 
theme =  "test"    # "campus_lod1"  #"campus"   # "37en2"

# Extract table names, cluster_numbers
object_input = "object_{}".format(theme) #data[theme]['object_table']
face_input = "face_{}".format(theme) # data[theme]['face_table']
cnum1, cnum2 = data[theme]['cluster_number'][0], data[theme]['cluster_number'][1]



# total time start
total_start_time = time.time()

# database connection
conn = pg.connect(dbname="sunrise", user="postgres", password="120598",
                            port="5432", host="localhost")

engine = create_engine('postgresql://postgres:120598@localhost:5432/sunrise')


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
cursor.execute("""
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

with points AS (
SELECT id, poly, ST_AsText(linestr) AS linestr,
ST_AsText(ST_Subtruct(ST_PointN(t.linestr, 2), ST_PointN(t.linestr, 3))) AS p1,
ST_AsText(ST_Subtruct(ST_PointN(t.linestr, 2), ST_PointN(t.linestr, 1))) AS p2
FROM (
SELECT id, ST_AsText(polygon) AS poly, ST_ExteriorRing(polygon) AS linestr
FROM tesselate_test
ORDER BY id
)AS t
)

SELECT  ARRAY[x / sqrt(x*x + y*y + z*z) ,
       y / sqrt(x*x + y*y + z*z),
       z / sqrt(x*x + y*y + z*z)]
FROM
(SELECT ST_X(n) as x, ST_Y(n) as y, ST_Z(n) as z  FROM
(SELECT poly, linestr, ST_AsText(ST_CrossProduct(
p1, p2
)) AS n from points) as n) as nn;
""")
conn.commit()


# map dataset to the face table and object table
input_data(object_input, face_input)


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
attrib_object = {'height': 'float'}  

attribute_toatal = {**attrib_object}
items = list(attribute_toatal.items())
# print(type(items))
for i in range(len(items)):  
    # print(len(attribute_toatal))
    # print(i)
    sql =  "INSERT INTO property(id, name, type) VALUES({0}, '{1}', '{2}');".format(i+1, items[i][0], items[i][1])
    # print(sql)
    cursor.execute(sql)
    conn.commit()


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


def delaunay_tess(cursor, conn):

    # delaunay
    cursor.execute("""
    -- Drop the table if it exists
    DROP TABLE IF EXISTS temp_delaunay;

    CREATE TEMP TABLE temp_delaunay AS
    with triangles as
    (
    select 
    id,
    ST_DelaunayTriangles(polygon3d) as tri 
    from temp
    WHERE normal[1] != 0 AND normal[2] != 0
    )
    select id, tri 
    from triangles;

    -- Update the 'temp' table with values from 'temp_delaunay', condition if_vertical
    UPDATE temp
    SET triangle = 
        CASE 
            WHEN temp.if_vertical = false THEN temp_delaunay.tri
            ELSE ST_RotateX(ST_RotateY(temp_delaunay.tri, -pi()/6), -pi()/6)
        END
    FROM temp_delaunay
    WHERE temp.id = temp_delaunay.id;
    """)
    conn.commit() 


    # tri_positioins from delaunay
    cursor.execute("""
    -- Drop the table if it exists
    DROP TABLE IF EXISTS temp_tri_pos;

    CREATE TEMP TABLE temp_tri_pos AS
    SELECT
    id, 
    --array_agg(ARRAY[ST_X(geom), ST_Y(geom), ST_Z(geom)]) AS coordinates
    array_agg(ARRAY[ST_X(geom), ST_Y(geom), ST_Z(geom)] ORDER BY nid) AS coordinates
    from
    (
    select --distinct 
        geom, id, path, ROW_NUMBER() OVER() AS nid
        FROM (
            SELECT
            id,
            ST_AsText((ST_DumpPoints(mpoly)).geom) AS geom,
            (ST_DumpPoints(mpoly)).path AS path
            FROM (
                SELECT id, 
                ST_CollectionExtract(triangle, 3) as mpoly 
                FROM temp
    WHERE normal[1] != 0 AND normal[2] != 0
            ) AS polys
        ) AS points
        WHERE path[3] != 4
    ORDER BY nid ASC
    ) AS gpoints
    GROUP BY id;

    UPDATE face 
    SET position = temp_tri_pos.coordinates
    FROM temp_tri_pos 
    WHERE face.id = temp_tri_pos.id
    AND temp_tri_pos.coordinates IS NOT NULL;
    """)
    conn.commit() 



    # #--------------------------------top and bottom----------------------------------------------------------------

    # Tesselate
    cursor.execute("""
    -- Drop the table if it exists
    DROP TABLE IF EXISTS temp_delaunay;

    CREATE TEMP TABLE temp_delaunay AS
    with triangles as
    (
    select 
    id,
    ST_Tesselate(polygon3d) as tri 
    from temp
    WHERE normal[1] = 0 AND normal[2] = 0 AND normal[3] != 0
    )
    select id, tri 
    from triangles;

    -- Update the 'temp' table with values from 'temp_delaunay', condition if_vertical
    UPDATE temp
    SET triangle = temp_delaunay.tri
    FROM temp_delaunay
    WHERE temp.id = temp_delaunay.id;
    """)
    conn.commit() 


    # tri_positioins from tesselate
    cursor.execute("""
    -- Drop the table if it exists
    DROP TABLE IF EXISTS temp_tri_pos;

    CREATE TEMP TABLE temp_tri_pos AS
    SELECT
    id, 
        array_agg(ARRAY[ST_X(geom), ST_Y(geom), ST_Z(geom)] ORDER BY nid) AS coordinates,
        array_agg(nid ORDER BY nid) AS idx_test
    --array_agg(ARRAY[ST_X(geom), ST_Y(geom), ST_Z(geom)]) AS coordinates, 
    --array_agg(nid) AS idx_test
    from
    (
    select --distinct 
        geom, id, path, ROW_NUMBER() OVER() AS nid
        FROM (
            SELECT
            id,
            ST_AsText((ST_DumpPoints(mpoly)).geom) AS geom,
            (ST_DumpPoints(mpoly)).path AS path
            FROM (
                SELECT id, 
                triangle as mpoly 
                FROM temp
    WHERE normal[1] = 0 AND normal[2] = 0 AND normal[3] != 0
            ) AS polys
        ) AS points
        WHERE path[3] != 4
        ORDER BY nid ASC
        ) AS gpoints
    GROUP BY id;


    UPDATE face 
    SET position = temp_tri_pos.coordinates, pos_idx_test = temp_tri_pos.idx_test
    FROM temp_tri_pos 
    WHERE face.id = temp_tri_pos.id
    AND temp_tri_pos.coordinates IS NOT NULL;
    """)
    conn.commit() 
    # #--------------------------------top and bottom----------------------------------------------------------------

    return 0

# delaunay_tess(cursor, conn)

def tess(cursor, conn):

    # position3d from temp.polygon3d
    cursor.execute("""
    -- Drop the table if it exists
    --DROP TABLE IF EXISTS temp_pos3d;

    CREATE TEMP TABLE temp_pos3d AS
    SELECT
    id, array_agg(ARRAY[ST_X(geom), ST_Y(geom), ST_Z(geom)]) AS xyz_coordinates
    from
    (
        select distinct 
        geom, id
        FROM (
            SELECT
            id,
            (ST_DumpPoints(polygon3d)).geom AS geom
            FROM temp
        ) AS points
    order by id
    ) AS distinctpoints
    GROUP BY id;

    -- Update the 'temp_pos3d' table with distinct coordinates
    UPDATE temp
    SET position3d = temp_pos3d.xyz_coordinates
    FROM temp_pos3d
    WHERE temp.id = temp_pos3d.id;

    --select position3d from temp;
    """)


    # tessellate and get tins
    cursor.execute("""
    -- Drop the table if it exists
    --DROP TABLE IF EXISTS temp_tins;

    CREATE TEMP TABLE temp_tins AS
    -- tesselate
    with triangles as
    (
    select 
    id,
    ST_Tesselate(ST_Force2D(polygon3d)) as tri 
    from temp
    WHERE NOT (normal[1] = 0 AND normal[2] = 0 AND normal[3] = 0)
    )
    select id, tri 
    from triangles;

    -- Update the 'temp' table with values from 'temp_tins'
    UPDATE temp
    SET triangle = temp_tins.tri
    FROM temp_tins
    WHERE temp.id = temp_tins.id;
    """)

    # tri_positioins from tins
    cursor.execute("""
    -- Drop the table if it exists
    --DROP TABLE IF EXISTS temp_tri_pos;

    CREATE TEMP TABLE temp_tri_pos AS
    SELECT
    id, --array_agg(ARRAY[ST_X(geom), ST_Y(geom)]) AS xy_coordinates
    array_agg(ARRAY[ST_X(geom), ST_Y(geom)] ORDER BY nid) AS xy_coordinates
    from
    (
        select --distinct 
        geom, id, ROW_NUMBER() OVER() AS nid
        FROM (
            SELECT
            id,
            (ST_DumpPoints(triangle)).geom AS geom
            FROM temp
        ) AS points
    order by id
    ) AS gpoints
    GROUP BY id;

    --select array_length(xy_coordinates, 1) from temp_tri_pos

    -- Update the 'temp' table with values from 'temp_tri_pos'
    UPDATE temp
    SET tri_position = temp_tri_pos.xy_coordinates
    FROM temp_tri_pos
    WHERE temp.id = temp_tri_pos.id;

    --select tri_position from temp;
    """)
    conn.commit() 

    # update position, indices in table face
    sql = "SELECT id FROM object;"
    cursor.execute(sql)
    results = cursor.fetchall()
    # print(results)

    oid= [int(i[0]) for i in results]


    face_total = 0
    for object_id in oid:
        # object_id = object_idx + 1
        sql = "SELECT tri_position, position3d, if_vertical, id FROM temp where object_id = {0} \
        and tri_position is not null\
        ORDER BY id\
        ;".format(object_id)
        cursor.execute(sql)

        results = cursor.fetchall()

        # print(results)
        face_count = len(results)
        # print('face_count', face_count)

        vert_total = 0
        for face_idx in range(face_count):
            # print("\nface_idx: ", face_idx)


            face_id = results[face_idx][3]
            # print("face_id", type(face_id), face_id)

            tri_position = results[face_idx][0]
            position3d = results[face_idx][1]
        
            # print("position3d: ", position3d)
            flag = results[face_idx][2]
            # print("if_vertical flag: ", type(flag), flag)

            
            #-----------------------------------add for order-----------------------------------------------------
            # set 2d = 3d, keep the order
            position2d = np.array(position3d)[:, :-1].tolist()
            # print("slice: ", position2d)
            #-----------------------------------end-----------------------------------------------------

            vert_count = len(position2d)
            # print("\nvert_count: ", vert_count)

            # print("position2d : ", position2d)
            # print("\ntri_position : ", tri_position)
            # print("\nlen(tri_position) : ",len(tri_position))

            # Create a list of indices of excluding elements
            indices_to_remove = [i for i in range(len(tri_position)) if (i+1) % 4 == 0]
            # print("\nindices_to_remove: ", indices_to_remove)

            tri_array = np.array(tri_position)
            # print("\ntri_array shape: ", np.shape(tri_array))


            # Create a mask to select elements to keep (False for elements to remove)
            mask = np.ones_like(tri_array, dtype=bool)
            # print("\nmask: ", mask)
            mask[indices_to_remove] = False

            # Slice the array to keep only the elements
            result_array = tri_array[mask]
            # print("\nafter remove: ", result_array)

            row = int(len(result_array)/2)
            result_array = result_array.reshape((row, 2))
            # print("\nafter remove and reshape: ", np.shape(result_array))

            # Find the indices of elements
            unique_tri = result_array.tolist()
            # print("unique_tri: ", unique_tri)
            indices = [position2d.index(i) for i in unique_tri ]
            # print("indices: ", indices)


            # update indices in an object
            c = np.array(indices) + np.array(len(indices)*[vert_total])

            # print(indices)
            indices_in_obj = c.tolist()
            # print('indices in an object', indices_in_obj)
            vert_total += vert_count


            sql = "UPDATE face SET tri_index = ARRAY{0} where id = {1};".format(indices_in_obj, int(face_id))
            cursor.execute(sql)
            # print("\n")

            #-----------this is for simply convert indices to coord START--------------------------------------------------
            # c.tolist(), 
            position3d = [position3d[i] for i in indices]
            # print("find position3d back: ", position3d)


            #-----------this is for simply convert indices to coord END--------------------------------------------------


            #-----------------------------------add for update position-----------------------------------------------------
            # UPDATE position in table face

            if flag == True:
                # print("Rotate applied: ")

                rotate_pos_collection = []
                for pos in position3d:


                    beta, alpha = -np.pi/6, -np.pi/6
                    
                    x,y,z = pos[0],pos[1],pos[2]
                    x,y,z = rotate_Y(x, y, z, beta)
                    x,y,z = rotate_X(x, y, z, alpha)

                    new_pos= [x,y,z]

                    # print(new_pos)

                    rotate_pos_collection.append(new_pos)

                #     sql_ro = "SELECT ARRAY[ST_X(geom), ST_Y(geom), ST_Z(geom)] FROM \
                #         (SELECT \
                #         ST_RotateX(ST_RotateY(\
                #             ST_SetSRID(ST_MakePoint({0}, {1}, {2}), 4978)\
                #             , pi()/6), pi()/6)\
                #         as geom) as b".format(pos[0], pos[1], pos[2])
                #     cursor.execute(sql_ro )

                #     results = cursor.fetchall()
                #     print("Rotate applied results: ", results)


                # ST_RotateY(ST_RotateX(face.polygon, pi()/6), pi()/6) need to rotate back
                rotate_pos = rotate_pos_collection # position3d (this is unrotated)
                # print('rotate_pos', rotate_pos)
                sql = "UPDATE face SET position = ARRAY{0} where id = {1};".format(rotate_pos, face_id)
                # print(sql)
                cursor.execute(sql)
                conn.commit() 
            else:
                # print('position3d', position3d)
                sql = "UPDATE face SET position = ARRAY{0} where id = {1};".format(position3d, face_id)
                cursor.execute(sql)
                conn.commit() 
            #-----------------------------------end-----------------------------------------------------


        face_total += face_count
    conn.commit() 

tess(cursor, conn)


end_time = time.time()
tri_execution_time = end_time - start_time
print(f"triangulation end, execution time: {tri_execution_time} seconds")
# ---------------------------------------------------------------------------triangulation end--------------------------------------------------------------------------------------

# Update table object envelope
cursor = conn.cursor()
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
cursor.execute(
"""
DROP TABLE IF EXISTS temp_centroids;

CREATE TEMP TABLE temp_centroids AS
With p
AS
(
SELECT id, (ST_Dump(envelope)).geom AS polygon from object
)
SELECT id, 
ST_AsText(ST_Force2D(ST_Centroid(ST_Collect(polygon)))) AS cc
FROM p GROUP BY id;

SELECT tmpc.id, cc, envelope FROM temp_centroids tmpc
JOIN object
ON tmpc.id = object.id;	


DROP TABLE IF EXISTS hierarchical_clusters;

-- Create a table to store hierarchical cluster information
CREATE TEMP TABLE hierarchical_clusters(
    object_id INTEGER, -- Assuming this column references the object's unique identifier
    level INTEGER, -- Level of clustering (e.g., 0 for initial clustering, 1 for sub-clustering, 2 for sub-sub-clustering)
    cluster_id INTEGER, -- Cluster ID at this level
    parent_cluster_id INTEGER, -- Cluster ID at the previous level (for hierarchy)
	name TEXT --unique name for all clusters
);

-- Perform k-means clustering for the first level (level 0)
INSERT INTO hierarchical_clusters (object_id, level, cluster_id, parent_cluster_id, name)
SELECT object_id, 1 AS level, cid AS cluster_id, NULL AS parent_cluster_id, 'Level_0_Cluster_' || cid AS name
FROM (
    SELECT ST_ClusterKMeans(cc, {})  OVER()  AS cid, id as object_id, cc
    FROM temp_centroids AS obj
) level_0_clusters;


-- Subsequent levels of clustering (if required)
-- Example: Second level clustering
INSERT INTO hierarchical_clusters(object_id, level, cluster_id, parent_cluster_id, name)
SELECT object_id, 2 AS level, cid AS cluster_id, parent_cluster_id, 'Level_1_Cluster_' || parent_cluster_id || '_SubCluster_' || cid AS name
FROM (
    SELECT 
        ST_ClusterKMeans(o.cc, {}) OVER(PARTITION BY t.cluster_id ORDER BY t.cluster_id) AS cid, 
        id AS object_id, 
        t.cluster_id AS parent_cluster_id
    FROM hierarchical_clusters t
    JOIN temp_centroids o ON t.object_id = o.id
    WHERE t.level = 1 -- Consider removing specific Cluster_id filter here
) level_1_clusters;


DROP TABLE IF EXISTS hierarchy;

CREATE TABLE hierarchy AS
SELECT (ARRAY_AGG(DISTINCT level))[1] AS level, 
ARRAY_AGG(object_id) AS object_id,
(ARRAY_AGG(DISTINCT cluster_id))[1] AS cluster_id,
(ARRAY_AGG(DISTINCT parent_cluster_id))[1] AS parent_cluster_id,
--ST_AsText(ST_Collect(envelope))
ST_Extent(envelope) AS envelope, 
ST_3DExtent(envelope) AS h_envelope
FROM hierarchical_clusters 
JOIN object
on object.id = object_id
GROUP BY name;


ALTER TABLE hierarchy
ADD COLUMN row_number INTEGER,
ADD COLUMN id SERIAL;

--Update row_number column with values generated by ROW_NUMBER() window function
WITH n AS (
    SELECT id,
           ROW_NUMBER() OVER (PARTITION BY level ORDER BY cluster_id) AS rn
    FROM hierarchy
)
UPDATE hierarchy AS h
SET row_number = n.rn
FROM n
WHERE h.id = n.id;

--SELECT * FROM hierarchy;
""".format(cnum1, cnum2)
)
conn.commit() 
print("clustering end")


# INSERT statement to update the id in the tile table
t1 = 1
tileset = 1
sql = "INSERT INTO tile (id, geometric_error, refine) VALUES({0}, 200, 'ADD');\
INSERT INTO tile (id) SELECT row_number FROM hierarchy WHERE level = 2 and row_number != 1;\
UPDATE tile SET tileset_id = {1} WHERE id IS NOT NULL;\
UPDATE tile SET parent_id = {0} \
WHERE id IN (SELECT row_number FROM hierarchy WHERE level = 2) and id != 1;".format(t1,tileset)
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


sql = "SELECT id FROM tile;"
cursor.execute(sql)
results = cursor.fetchall()
# print(results)
conn.commit() 

tid_list= [int(i[0]) for i in results]
# print(tid_list)

# covert coord to node_idx, update table face.tri_node_id, object.nodes
array_coord(conn, cursor)

cursor.close()
conn.close()    # Close the database connection


def schema_update():
    # database connection
    conn = pg.connect(dbname="sunrise", user="postgres", password="120598",
                                port="5432", host="localhost")

    engine = create_engine('postgresql://postgres:120598@localhost:5432/sunrise')
    cursor = conn.cursor() # Create a cursor object
    cursor.execute("""
    ALTER TABLE face
    DROP COLUMN fid,
    DROP COLUMN position,
    DROP COLUMN tri_index,
    DROP COLUMN pos_idx_test,
    DROP COLUMN if_planar,
    DROP COLUMN polygon;
    ALTER TABLE object
    DROP COLUMN lod,
    DROP COLUMN envelope,
    DROP COLUMN object_root;
    ALTER TABLE hierarchy
    DROP COLUMN h_envelope;
    DROP table property;
    """)
    conn.commit() 
    cursor.close()
    conn.close()    # Close the database connection

schema_update()


print("pre-computed b3dm stored in DB start")
start_time = time.time()

# write pre-computed b3dm to DB
for id in tid_list:
    write_tile(id, 1)   # 1: indexed
    # write_tile(id, 0)   # 0: non-idexed

end_time = time.time()
execution_time = end_time - start_time
print(f"Pre-computatioin execution time: {execution_time} seconds")


# total time end
total_end_time = time.time()
execution_time = total_end_time - total_start_time
print(f"Total execution time: {execution_time} seconds")


if __name__ == "__main__":
    pass