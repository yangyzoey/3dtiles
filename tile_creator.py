import psycopg2 as pg
import numpy as np
import time
import json

from tile_function import (array_coord, rotate_X,rotate_Y, 
triangulation, normal_compute, k_means, 
schema_update, input_data, write_b3dm, write_glb, input_primitive)
from dbconfig import config


def tiles_creator(theme):

    # Load JSON file
    with open('input.json', 'r') as file:
        data = json.load(file)

    # Extract info
    theme_data = data[theme]
    # object_input = "object_{}".format(theme) 
    # face_input = "face_{}".format(theme) 

    attrib_object_str = theme_data['property']
    # attrib_object = json.loads(attrib_object_str.replace("'", "\"")) # Convert the string to a dictionary
    
    keys = attrib_object_str
    property_info = {}
    for key in keys:
        if key == 'height':
            property_info[key] = 'float'
        elif key == 'construction_year':
            property_info[key] = 'int'
        elif key == 'building_type':
            property_info[key] = 'text'
        elif key == 'city':
            property_info[key] = 'text'
        else:
            pass
    attrib_object = property_info
    print("attrib_object: ", attrib_object)


    cnum1, cnum2 = theme_data['cluster_number'][0], theme_data['cluster_number'][1]
    triangulation_flag = theme_data['triangulation_flag']
    sql_filter = theme_data['filter']
    index_flag = int(theme_data['index_flag']) #0: non-indexed; 1: indexed; -1: no pre-composed
    b3dm_flag = int(theme_data["b3dm_flag"])
    glb_flag = int(theme_data["glb_flag"])
    lod_flag = theme_data["lod"]
    print("lod: ",lod_flag)

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
            nodes DOUBLE PRECISION[][],\
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
            position3d DOUBLE PRECISION[],\
            position2d DOUBLE PRECISION[],\
            triangle geometry,\
            tri_position DOUBLE PRECISION[],\
            tri_pos_xy DOUBLE PRECISION[],\
            UNIQUE(id)\
            );\
            "
    cursor.execute(sql)
    conn.commit()


    # functioin geometric operation ST_Subtract, ST_CrossProduct
    sql = """
    --Compute normal vectors for polygons and store as an array

    DROP FUNCTION IF EXISTS ST_Subtract(geometry, int, int);

    CREATE OR REPLACE FUNCTION ST_Subtract(p1 geometry, p2 geometry)
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

    print("\ninput data starts")
    # map dataset to the face table and object table
    if theme == "primitive":
        sql = input_primitive(conn, cursor, lod_flag, theme)
        # print(sql)
    else:
        sql = input_data(conn, cursor, lod_flag, theme)
        # print(sql)
    print("input data ends\n")

    
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

    print("\nproperty start")
    start_time = time.time()
    # update table property
    cursor.execute("""
    -- Create the property table without the foreign key constraint
    CREATE TABLE property AS (
        SELECT 
            ROW_NUMBER() OVER () AS pid,
            id AS object_id
        FROM object
    );

    -- Alter the property table to add the foreign key constraint
    ALTER TABLE property
    ADD CONSTRAINT fk_object_id FOREIGN KEY (object_id) REFERENCES object(id);
    """)

    items = list(attrib_object.items())
    # print(type(items))
    for i in range(len(items)):
        # print(len(attrib_object))
        # print(i)
        attrib = items[i][0]
        sql =  "ALTER TABLE property ADD {0} {1};".format(attrib, items[i][1])
        # print(sql)
        cursor.execute(sql)

        if attrib == "height": # compute height from 3D envelope
            cursor.execute("UPDATE property SET height = (ST_ZMax(o.envelope) - ST_ZMin(o.envelope)) FROM object o WHERE object_id = id;") 
        elif attrib == "construction_year":
            cursor.execute("""
            -- Set seed for the random number generator
            SELECT SETSEED(0.5);
            -- Update the 'construction_year' column using the random number generator
            UPDATE property SET construction_year = 1950 + FLOOR(RANDOM() * (2021 - 1950));
            """)
        elif attrib == "building_type":
            cursor.execute("""
            -- Set seed for the random number generator
            SELECT SETSEED(0.5);
            UPDATE property 
            SET building_type = 
            CASE 
                WHEN RANDOM() < 0.25 THEN 'Residential'
                WHEN RANDOM() >= 0.25 AND RANDOM() < 0.5 THEN 'Commercial'
                WHEN RANDOM() >= 0.5 AND RANDOM() < 0.75 THEN 'Industrial'
                ELSE 'Infrastructural'
            END;
        """)
        elif attrib == "class":
            cursor.execute("UPDATE property SET class = 'building';")
        elif attrib == "gid":
            cursor.execute("UPDATE property SET gid = object_id;")
        elif attrib == "type":
            cursor.execute("UPDATE property SET type = 'public space';")
        elif attrib == "owner":
            cursor.execute("UPDATE property SET owner = 'TU Delft';")
        elif attrib == "city":
            cursor.execute("UPDATE property SET city = 'Delft';")
        else:
            print("Error: property automation is not supported")

        # #'tmin': int, 
        # #'tmax': int
        # cursor.execute("""
        # --UPDATE property SET tmin = 2000;
        # --UPDATE property SET tmax = 3000;

        conn.commit()
    end_time = time.time()
    nor_execution_time = end_time - start_time
    print(f"property end, execution time: {nor_execution_time} seconds\n")

    #-------------------------------------------------------------normal start------------------------------------
    print("\nnormalised normal start")
    start_time = time.time()

    normal_compute(conn, cursor)
    end_time = time.time()
    nor_execution_time = end_time - start_time
    print(f"normalised end, execution time: {nor_execution_time} seconds\n")
    #-------------------------------------------------------------normal end------------------------------------
    #-------------------------------------------------------------triangulation start--------------------------------------------------------------------------------------------
    print("\ntriangulation start")
    start_time = time.time()

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

    # count vertical faces
    sql_faces = """
    SELECT 
        SUM(CASE WHEN if_vertical = true THEN 1 ELSE 0 END) AS vertical_faces,
        SUM(CASE WHEN if_vertical = false THEN 1 ELSE 0 END) AS non_vertical_faces
    FROM temp;
    """
    cursor.execute(sql_faces)
    counts = cursor.fetchone()
    count_vertical = counts[0]
    count_non_vertical = counts[1]
    print("Number of vertical faces:", count_vertical)
    print("Number of non-vertical faces:", count_non_vertical)


    # planar flag or not
    sql_planar = f"""
        UPDATE temp
        SET if_planar = ST_IsPlanar(face.polygon) FROM face where temp.id = face.id;
    """
    cursor.execute(sql_planar)
    conn.commit()


    # count planar faces
    sql_faces = """
    SELECT 
        SUM(CASE WHEN if_planar = true THEN 1 ELSE 0 END) AS planar_faces,
        SUM(CASE WHEN if_planar = false THEN 1 ELSE 0 END) AS non_planar_faces,
        SUM(CASE WHEN if_planar = false AND if_vertical = true THEN 1 ELSE 0 END) 
        AS non_planar_vertical_faces
    FROM temp;
    """
    cursor.execute(sql_faces)
    counts = cursor.fetchone()
    count_planar = counts[0]
    count_non_planar = counts[1]
    count_non_planar_vertical = counts[2]
    print("Number of planar faces:", count_planar)
    print("Number of non-planar faces:", count_non_planar)
    print("Number of non-planar vertical faces:", count_non_planar_vertical)

    triangulation(conn, cursor, triangulation_flag)

    end_time = time.time()
    tri_execution_time = end_time - start_time
    print(f"triangulation end, execution time: {tri_execution_time} seconds\n")
    # -----------------------------------------------------------triangulation end--------------------------------------------------------------------------------------


    # covert coord to node_idx, update table face.tri_node_id, object.nodes
    print("\nCoord to node_idx start")
    start_time = time.time()
    array_coord(conn, cursor)
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Coord to node_idx end, execution time: {execution_time} seconds\n")


    print("\nclustering start")
    start_time = time.time()
    # cluster objects to different tiles
    k_means(conn, cursor, cnum1, cnum2) 
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"clustering end, execution time: {execution_time} seconds\n")


    # tile ids
    cursor.execute("""
        SELECT temp_tid FROM hierarchy
        WHERE level = (SELECT level FROM hierarchy ORDER BY level DESC LIMIT 1);
        """)
    results = cursor.fetchall()
    conn.commit() 

    tid_list= [int(i[0]) for i in results]
    # print(tid_list)


    # ---------------------------------------------pre-composed gltf time-------------------
    # full pre-composed b3dm in DB
    print("\nPre-composed gltf stored in DB start")
    start_time = time.time()

    if glb_flag == -1:
        print("No pre-composed binary gltf")
    else:
        print("Write to DB, {0}".format(["composed non-indexed binary gltf", "composed indexed binary gltf"][index_flag]))
        cursor.execute("ALTER TABLE hierarchy ADD COLUMN glb BYTEA;")
        for tid in tid_list:
            write_glb(conn, cursor, tid, index_flag, sql_filter)

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Pre-composed gltf stored in DB end, execution time: {execution_time} seconds\n")
    # ---------------------------------------------pre-composed gltf time---------------------




    # ---------------------------------------------fully pre-composed b3dm time-------------------
    # full pre-composed b3dm in DB
    print("\nPre-composed b3dm stored in DB start")
    start_time = time.time()


    if b3dm_flag == -1:
        print("No pre-composed b3dm")
    else:
        print("Write to DB, {0}".format(["composed non-indexed b3dm", "composed indexed b3dm"][index_flag]))
        cursor.execute("ALTER TABLE hierarchy ADD COLUMN b3dm BYTEA;")
        for tid in tid_list:
            write_b3dm(conn, cursor, tid, index_flag, sql_filter, attrib_object)   # 1: indexed; 0: non-idexed


    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Pre-composed b3dm stored in DB end, execution time: {execution_time} seconds\n")
    # ---------------------------------------------fully pre-composed b3dm time---------------------


 

    # # ----------------------------------------------create vw_content from hierarchy start-------------------------------------------------
    # print("\nStore in view content start")
    # start_time = time.time()

    # sql_add = ""
    # if glb_flag == 1:
    #     sql_add = ", glb AS glb"
    # elif b3dm_flag == 1:
    #     sql_add = ", b3dm AS b3dm"
    
    # sql_vw_content ="""
    # CREATE OR REPLACE FUNCTION create_vw_content() RETURNS VOID AS $$
    # BEGIN
    #     EXECUTE 'DROP VIEW IF EXISTS vw_content CASCADE';

    #     -- Create iew vw_content
    #     EXECUTE '
    #     CREATE VIEW vw_content AS
    #     WITH e AS (
    #     SELECT ST_3DExtent(envelope) AS envelope FROM hierarchy WHERE level = 2
    # )
    #     SELECT
    #         h.temp_tid AS tid,
    #         1 AS tileset_id
    #         --h.temp_tid AS content
    #         {0}
    #     FROM hierarchy h, e 
    #     WHERE h.level = 2
    #     ';

    #     RETURN;
    # END;   
    # $$ LANGUAGE plpgsql;

    # SELECT create_vw_content();
    # SELECT * FROM vw_content;
    # """.format(sql_add)
    # cursor.execute(sql_vw_content)
    # conn.commit() 

    # end_time = time.time()
    # execution_time = end_time - start_time
    # print(f"Store in view content end, execution time: {execution_time} seconds\n")
    # # ---------------------------------------------create vw_tile from hierarchy end-----------------------------------------------------------
    # ---------------------------------------create vw_tileset from hierarchy start--------------------------
    print("\nStore in view tileset start")
    start_time = time.time()

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
                    WHERE table_name = ''property''
                    ORDER BY ordinal_position
                    OFFSET 2
                ) AS property_data
            ),
		tile
		AS (
		WITH 
        e AS (
        SELECT ST_3DExtent(envelope) AS envelope FROM hierarchy WHERE level = 2
    )
        SELECT
            h.temp_tid AS id,
            1 AS tileset_id,

            CASE WHEN h.temp_tid != 1 THEN
                1
            ELSE
                NULL
            END AS parent_id,

            CASE WHEN h.temp_tid != 1 THEN
            ARRAY[
            (ST_XMin(h.envelope) + ST_XMax(h.envelope)) / 2, -- centerX
            (ST_YMin(h.envelope) + ST_YMax(h.envelope)) / 2, -- centerY
            (ST_ZMin(h.envelope) + ST_ZMax(h.envelope)) / 2, -- centerZ
            (ST_XMax(h.envelope) - ST_XMin(h.envelope)) / 2, 0, 0, -- halfX
            0, (ST_YMax(h.envelope) - ST_YMin(h.envelope)) / 2, 0, -- halfY
            0, 0, (ST_ZMax(h.envelope) - ST_ZMin(h.envelope)) / 2 -- halfZ
        ]
            ELSE
                ARRAY[
            (ST_XMin(e.envelope) + ST_XMax(e.envelope)) / 2, -- centerX
            (ST_YMin(e.envelope) + ST_YMax(e.envelope)) / 2, -- centerY
            (ST_ZMin(e.envelope) + ST_ZMax(e.envelope)) / 2, -- centerZ
            (ST_XMax(e.envelope) - ST_XMin(e.envelope)) / 2, 0, 0, -- halfX
            0, (ST_YMax(e.envelope) - ST_YMin(e.envelope)) / 2, 0, -- halfY
            0, 0, (ST_ZMax(e.envelope) - ST_ZMin(e.envelope)) / 2 -- halfZ
        ] 
            END AS bounding_volume,

            CASE WHEN h.temp_tid = 1 THEN
                ROUND(
                sqrt(
                    power(ST_XMax(h.envelope) - ST_XMin(h.envelope), 2) +
                    power(ST_YMax(h.envelope) - ST_YMin(h.envelope), 2) +
                    power(ST_ZMax(h.envelope) - ST_ZMin(h.envelope), 2)
                )::numeric/2,
                2) 
                --diagonal_length
            ELSE
                0
            END AS geometric_error,

            CASE WHEN h.temp_tid = 1 THEN
                ''ADD''
            ELSE
                NULL
            END AS refine,

            h.temp_tid AS content
        FROM hierarchy h, e 
        WHERE h.level = 2
		),
			
			children AS (
                SELECT
                    array_agg(json_build_object(
                        ''boundingVolume'', json_build_object(
                            ''box'', tile_data.bounding_volume
                        ),
                        ''geometricError'', tile_data.geometric_error,
                        ''content'', json_build_object(''uri'', CONCAT(''/tiles/'', tile_data.content, ''.b3dm''))
                    )) AS children_json
                FROM 
				
					(
					SELECT *
					FROM tile
					WHERE tileset_id = 1 --AND tile_data.parent_id IS NOT NULL
					 --AND tile_data.parent_id IS NOT NULL
					ORDER BY id -- Order by tile_id
					) AS tile_data
				 GROUP BY tile_data.tileset_id
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
                        ''children'', (children.children_json)[2:],
                        ''transform'', ARRAY[1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1, 1, 1, 1.0]
                    )
                ) AS tileset_json  
            FROM (
                SELECT *
                FROM tile
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
    SELECT * FROM vw_tileset;
    """
    cursor.execute(sql_vw_tileset)
    conn.commit() 
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Store in view tileset end, execution time: {execution_time} seconds\n")
    # -------------------------------------create vw_tileset from hierarchy start----------------

    # delete unnecessary tables
    schema_update(conn, cursor)


if __name__ == "__main__":

    # specfy the dataset theme 
    theme = "9_284_556" #"primitive"  


    # total time start
    total_start_time = time.time()
    
    tiles_creator(theme)

    # total time end
    total_end_time = time.time()
    execution_time = total_end_time - total_start_time
    print(f"Total execution time: {execution_time} seconds\n")