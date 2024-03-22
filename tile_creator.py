import psycopg2 as pg
import numpy as np
import time
import json

from tile_function import array_coord, rotate_X,rotate_Y, triangulation, k_means, schema_update, input_data, write_b3dm, write_glb
from dbconfig import config


def tiles_creator(theme, ge1, ge2, refine):

    # Load JSON file
    with open('input.json', 'r') as file:
        data = json.load(file)


    # Extract info
    theme_data = data[theme]
    object_input = "object_{}".format(theme) 
    face_input = "face_{}".format(theme) 
    attrib_object_str = theme_data['property']
    attrib_object = json.loads(attrib_object_str.replace("'", "\"")) # Convert the string to a dictionary
    cnum1, cnum2 = theme_data['cluster_number'][0], theme_data['cluster_number'][1]
    triangulation_flag = theme_data['triangulation_flag']
    sql_filter = theme_data['filter']
    index_flag = int(theme_data['index_flag']) #0: non-indexed; 1: indexed; -1: no pre-computed
    b3dm_flag = int(theme_data["b3dm_flag"])
    glb_flag = int(theme_data["glb_flag"])
    

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
            nodes DOUBLE PRECISION[],\
            envelope box3d,\
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


    # update table object to add properties
    items = list(attrib_object.items())
    # print(type(items))
    for i in range(len(items)):
        # print(len(attrib_object))
        # print(i)
        attrib = items[i][0]
        sql =  "ALTER TABLE object ADD {0} {1};".format(attrib, items[i][1])
        # print(sql)
        cursor.execute(sql)

        if attrib == "construction_year":
            cursor.execute("""
            -- Set seed for the random number generator
            SELECT SETSEED(0.5);

            -- Update the 'construction_year' column using the random number generator
            UPDATE object SET construction_year = 1950 + FLOOR(RANDOM() * (2021 - 1950));
            """)
        elif attrib == "class":
            cursor.execute("UPDATE object SET class = 'building';")
        elif attrib == "type":
            cursor.execute("UPDATE object SET type = 'public space';")
        elif attrib == "owner":
            cursor.execute("UPDATE object SET owner = 'TU Delft';")
        elif attrib == "city":
            cursor.execute("UPDATE object SET city = 'Delft';")
        else:
            pass

        # #'tmin': int, 
        # #'tmax': int
        # cursor.execute("""
        # --UPDATE object SET tmin = 2000;
        # --UPDATE object SET tmax = 3000;

        conn.commit()


    # Add 3D envelope on the table object
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
    SELECT object_id, e.envelope AS envelope
    FROM e;

    UPDATE object SET envelope = h.envelope FROM h WHERE id = object_id;
    """)
    conn.commit()


    # operation 3D envelope, compute height
    cursor.execute("""
    UPDATE object SET height = (ST_ZMax(envelope) - ST_ZMin(envelope));
    """)
    conn.commit()


    # operation on table temp
    cursor.execute("""
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


    print("clustering start")
    # cluster objects to different tiles
    k_means(conn, cursor, cnum1, cnum2) 
    print("clustering end")


    # attemp to undound tile_id in object
    # RENAME row_num to temp_tile_id in the hierarchy table
    cursor.execute("""
    ALTER TABLE hierarchy RENAME COLUMN row_number TO temp_tile_id;
    """)
    conn.commit() 


    # ----------------------------------------------create vw_tile from hierarchy-------------------------------------------------
    sql_vw_tile ="""
    CREATE OR REPLACE FUNCTION create_vw_tile(ge1 INT, ge2 INT, refine TEXT) RETURNS VOID AS $$
    BEGIN
        EXECUTE 'DROP VIEW IF EXISTS vw_tile CASCADE';

        -- Create iew vw_tile
        EXECUTE '
        CREATE VIEW vw_tile AS
        WITH e AS (
        SELECT ST_3DExtent(envelope) AS envelope FROM hierarchy WHERE level = 2
    )
        SELECT
            h.temp_tile_id AS id,
            1 AS tileset_id,

            CASE WHEN h.temp_tile_id != 1 THEN
                1
            ELSE
                NULL
            END AS parent_id,

            CASE WHEN h.temp_tile_id != 1 THEN
            ARRAY[
            (ST_XMin(h.envelope) + ST_XMax(h.envelope)) / 2, -- centerX
            (ST_YMin(h.envelope) + ST_YMax(h.envelope)) / 2, -- centerY
            (ST_ZMin(h.envelope) + ST_ZMax(h.envelope)) / 2, -- centerZ
            (ST_XMax(h.envelope) - ST_XMin(h.envelope)) / 2, 0, 0, -- halfX
            0, -(ST_YMax(h.envelope) - ST_YMin(h.envelope)) / 2, 0, -- halfY
            0, 0, (ST_ZMax(h.envelope) - ST_ZMin(h.envelope)) / 2 -- halfZ
        ]
            ELSE
                ARRAY[
            (ST_XMin(e.envelope) + ST_XMax(e.envelope)) / 2, -- centerX
            (ST_YMin(e.envelope) + ST_YMax(e.envelope)) / 2, -- centerY
            (ST_ZMin(e.envelope) + ST_ZMax(e.envelope)) / 2, -- centerZ
            (ST_XMax(e.envelope) - ST_XMin(e.envelope)) / 2, 0, 0, -- halfX
            0, -(ST_YMax(e.envelope) - ST_YMin(e.envelope)) / 2, 0, -- halfY
            0, 0, (ST_ZMax(e.envelope) - ST_ZMin(e.envelope)) / 2 -- halfZ
        ] 
            END AS bounding_volume,

            CASE WHEN h.temp_tile_id = 1 THEN
                ' || ge1|| '
            ELSE
                ' || ge2|| '
            END AS geometric_error,

            CASE WHEN h.temp_tile_id = 1 THEN
                ''' || refine || '''
            ELSE
                NULL
            END AS refine,

            h.temp_tile_id AS content,
            null AS b3dm
        FROM hierarchy h, e
        WHERE h.level = 2
        ';

        RETURN;
    END;   
    $$ LANGUAGE plpgsql;

    SELECT create_vw_tile({0}, {1}, '{2}');
    """.format(ge1, ge2, refine)
    cursor.execute(sql_vw_tile)
    conn.commit() 
    # ---------------------------------------------create vw_tile from hierarchy-----------------------------------------------------------

    sql_vw_tileset ="""
    CREATE OR REPLACE FUNCTION create_vw_tileset() RETURNS VOID AS $$
    BEGIN

        -- Drop the existing view if it exists
        EXECUTE 'DROP VIEW IF EXISTS vw_tileset;';

        -- Create the view vw_tileset
        EXECUTE '
            CREATE VIEW vw_tileset AS
            WITH property AS (
                SELECT json_object_agg(initcap(properties), json_build_object()) AS property_json
                FROM (
                    SELECT column_name AS properties
                    FROM information_schema.columns
                    WHERE table_name = ''object''
                    ORDER BY ordinal_position
                    OFFSET 3
                ) AS property_data
            ),
            children AS (
                SELECT
                    tile_data.id,
                    json_agg(json_build_object(
                        ''boundingVolume'', json_build_object(
                            ''box'', tile_data.bounding_volume
                        ),
                        ''geometricError'', tile_data.geometric_error,
                        ''content'', json_build_object(''uri'', CONCAT(''/tiles/'', tile_data.content, ''.b3dm''))
                    )) AS children_json
                FROM vw_tile AS tile_data
                WHERE tile_data.tileset_id = 1 AND tile_data.parent_id IS NOT NULL
                GROUP BY tile_data.id
            )
            SELECT 
                1 AS id,
                json_build_object(
                    ''asset'', json_build_object(
                        ''version'', ''1.0'',
                        ''tilesetVersion'', ''1.2.3''
                    ),
                    ''properties'', property.property_json,
                    ''geometricError'', tile_data.geometric_error,
                    ''root'', json_build_object(
                        ''boundingVolume'', json_build_object(
                            ''box'', tile_data.bounding_volume
                        ),
                        ''geometricError'', tile_data.geometric_error,
                        ''refine'', tile_data.refine,
                        ''content'', json_build_object(
                            ''boundingVolume'', json_build_object(
                                ''box'', tile_data.bounding_volume
                            ),
                            ''uri'', CONCAT(''/tiles/'', tile_data.content, ''.b3dm'')
                        ),
                        ''children'', children.children_json,
                        ''transform'', ARRAY[1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1, 1, 1, 1.0]
                    )
                ) AS tileset_json
            FROM (
                SELECT *
                FROM vw_tile
                WHERE tileset_id = 1 AND parent_id IS NULL
            ) AS tile_data
            ,property
            ,children;';

        RETURN;
    END;
    $$ LANGUAGE plpgsql;

    -- Call the function to create the view
    SELECT create_vw_tileset();

    -- Retrieve data from the created view
    --SELECT * FROM vw_tileset;
    """#.format(ge1, ge2, refine)
    cursor.execute(sql_vw_tileset)
    conn.commit() 


    # covert coord to node_idx, update table face.tri_node_id, object.nodes
    print("Coord to node_idx start")
    start_time = time.time()

    # sql = "SELECT id FROM tile;"
    sql = "SELECT id FROM vw_tile;"
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

    # ---------------------------------------------fully pre-computed b3dm time-------------------
    # full pre-computed b3dm in DB
    print("Pre-computed b3dm stored in DB start")
    start_time = time.time()


    if b3dm_flag == -1:
        print("No pre-computed b3dm")
    else:
        print("Write pre-computed b3dm to DB, {0}".format(["composed non-indexed b3dm", "composed indexed b3dm"][index_flag]))
        cursor.execute("ALTER TABLE hierarchy ADD COLUMN b3dm BYTEA;")
        for tid in tid_list:
            write_b3dm(conn, cursor, tid, index_flag, sql_filter, attrib_object)   # 1: indexed; 0: non-idexed


    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Pre-computed b3dm stored in DB end, execution time: {execution_time} seconds")
    # ---------------------------------------------fully pre-computed b3dm time---------------------


    # ---------------------------------------------pre-computed gltf time-------------------
    # full pre-computed b3dm in DB
    print("Pre-computed gltf stored in DB start")
    start_time = time.time()

    if glb_flag == -1:
        print("No pre-computed binary gltf")
    else:
        print("Write pre-computed binary gltf to DB, {0}".format(["composed non-indexed glb", "composed indexed glb"][index_flag]))
        cursor.execute("ALTER TABLE hierarchy ADD COLUMN glb BYTEA;")
        for tid in tid_list:
            write_glb(conn, cursor, tid, index_flag, sql_filter)

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Pre-computed gltf stored in DB end, execution time: {execution_time} seconds")
    # ---------------------------------------------pre-computed gltf time---------------------

    # delete unnecessary tables
    schema_update(conn, cursor)


if __name__ == "__main__":

    # specfy the dataset theme 
    theme = "test"  #"test" #"37en2" #"test" #  # "37en1"  # "37en2" # "campus_lod1"
    ge1 = 1000
    ge2 = 0
    refine = 'ADD'

    # total time start
    total_start_time = time.time()
    
    tiles_creator(theme, ge1, ge2, refine)

    # total time end
    total_end_time = time.time()
    execution_time = total_end_time - total_start_time
    print(f"Total execution time: {execution_time} seconds")