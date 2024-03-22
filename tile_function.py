import psycopg2 as pg
from sqlalchemy import create_engine
import numpy as np
import json

from glb_generator import GLB
from b3dm_generator import B3DM, glb_test


def fetch_tile(conn, cursor, tile_id, index_flag, sql_filter, attrib_object):

        attrib_list = list(attrib_object.keys())
        # Convert attrib_list to a string
        attrib_str = ", ".join(attrib_list)
 
        sql = """
        WITH UnnestedIDs AS (
        SELECT object_id as oid_list, temp_tile_id
        FROM hierarchy
        WHERE level = (SELECT level FROM hierarchy ORDER BY level DESC LIMIT 1) AND
        temp_tile_id = {1}
        )
        SELECT id, nodes, {0}  
        FROM object 
        WHERE id IN (SELECT unnest(oid_list) FROM UnnestedIDs) 
        {2}
        ORDER BY id;
        """.format(attrib_str, tile_id, sql_filter)
        # print(sql)

        cursor.execute(sql)
        results = cursor.fetchall()
        # print(results)

        oid_list= [int(i[0]) for i in results]
        # print("tile No.{} object_id list: ".format(tile_id),oid_list)

        nodes_values= [i[1] for i in results]
        # print('nodes_values: ', nodes_values)

        # -----------------------------------------gltf start--------------------------------
        glbBytesData = compose_gltf(conn, cursor, tile_id, oid_list, nodes_values, index_flag)
        # -----------------------------------------gltf end--------------------------------

        #-------------------------------------------featureTable,batchTable start------------------
        featureTableData, batchTableData = compose_featureTable_batchTable(oid_list, attrib_object, results)
        #-------------------------------------------featureTable,batchTable end----------------------------------------------------

        conn.commit()

        # # test
        # featureTableData, batchTableData = None, None 

        return glbBytesData, featureTableData, batchTableData


def fetch_featureTable_batchTable(conn, cursor, tile_id, sql_filter, attrib_object):
    attrib_list = list(attrib_object.keys())
    # Convert attrib_list to a string
    attrib_str = ", ".join(attrib_list)

    sql = """
    WITH UnnestedIDs AS (
    SELECT object_id as oid_list, temp_tile_id
    FROM hierarchy
    WHERE level = (SELECT level FROM hierarchy ORDER BY level DESC LIMIT 1) AND
    temp_tile_id = {1}
    )
    SELECT id, nodes, {0}  
    FROM object 
    WHERE id IN (SELECT unnest(oid_list) FROM UnnestedIDs) 
    {2}
    ORDER BY id;
    """.format(attrib_str, tile_id, sql_filter)
    # print(sql)

    cursor.execute(sql)
    results = cursor.fetchall()
    # print(results)

    oid_list= [int(i[0]) for i in results]
    # print("tile No.{} object_id list: ".format(tile_id),oid_list)


    #-------------------------------------------featureTable,batchTable start------------------
    featureTableData, batchTableData = compose_featureTable_batchTable(oid_list, attrib_object, results)
    #-------------------------------------------featureTable,batchTable end----------------------------------------------------

    conn.commit()

    return featureTableData, batchTableData


def compose_gltf(conn, cursor, tile_id, oid_list, nodes_values, index_flag):
    # update position, indices in table face
    pos = []
    nor = []
    indices = []
    ids = []

    if index_flag == 0:
        object_vert_total = 0
        for idx, object_id in enumerate(oid_list):
            
            sql = "SELECT normal, tri_node_id FROM face \
            where object_id = {0} and tri_node_id is not null \
            ORDER BY id;".format(object_id)
            # print(sql)
            cursor.execute(sql)

            res = cursor.fetchall()
            # print(res)

            coord = nodes_values[idx]

            pos_list = []
            nor_list = []

            for item in res:
                
                tri_node_id = item[1]
                
                # print('tri_node_id: ', tri_node_id)

                p = [coord[i] for i in tri_node_id]
                # p = [[round(num) for num in sublist] for sublist in p]
                # print('p repos: ', p)
                # print('\n')

                n = [item[0]]*len(p)
                
                pos_list.extend(p)
                nor_list.extend(n)
            
            ids_list = [idx]*len(pos_list)
            # print("pos_list: ", len(pos_list), pos_list)
            # print("nor_list: ", len(nor_list), nor_list)
            # print("ids_list: ", len(ids_list), ids_list)


            pos.extend(pos_list)
            nor.extend(nor_list)
            ids.extend(ids_list)
        indices = list(range(len(pos)))
        # print("\npos: ", len(pos), pos)
        # print("\nnor: ", len(nor),nor)
        # print("\nindices: ", len(indices))
        # print("\nids: ", len(ids),ids)

    if index_flag == 1:
        object_vert_total = 0
        for idx, object_id in enumerate(oid_list):
            
            sql = "SELECT normal, tri_node_id FROM face \
            where object_id = {0} and tri_node_id is not null \
            ORDER BY id;".format(object_id)
            # print(sql)
            cursor.execute(sql)

            res = cursor.fetchall()
            # print(res)

            coord = nodes_values[idx]

            pos_list = []
            nor_list = []
            tri_indices = []

            face_vert_total = 0
            for item in res:

                tri_node_id = item[1]
                # print('tri_node_id: ', tri_node_id)
                p = [coord[i] for i in tri_node_id]
                n = [item[0]]*len(p)

                # print('p before: ', len(p), p)   
                p_unique = list(map(list, set(map(tuple, p))))  
                # print('p after: ', len(p_unique), p_unique) 

                index = [p_unique.index(i) for i in p] 
                # print('index before: ', index) 
                index_array = np.array(index) + np.array(len(index)*[face_vert_total])
                idx_list = index_array.tolist()
                # print('index after: ', idx_list)

                n = [item[0]]*len(p_unique)
                # positions in a face:
                # step1: find unique positions 
                # step2: find indices of the positions #
                # step3: append p to pos_list, append index to tri_indices
                # step4: later: ids_list = [idx]*len(pos_list)
                
                pos_list.extend(p_unique)
                nor_list.extend(n)
                tri_indices.extend(idx_list)
                face_vert_total = len(pos_list)
                            
            ids_list = [idx]*len(pos_list)
            # print("pos_list: ", len(pos_list), pos_list)
            # print("nor_list: ", len(nor_list), nor_list)
            # print("ids_list: ", len(ids_list), ids_list)
            # print("origin tri_indices: ", len(tri_indices), tri_indices)


            idx_array= np.array(tri_indices) + np.array(len(tri_indices)*[object_vert_total])
            idx_list = idx_array.tolist()
            # print("updated idx_list: ", len(idx_list), idx_list)


            pos.extend(pos_list)
            nor.extend(nor_list)
            ids.extend(ids_list)
            indices.extend(idx_list)
            object_vert_total = len(pos)

        # print("\npos: ", len(pos), pos)
        # print("\nnor: ", len(nor), nor)
        # print("\nindices: ", len(indices), indices)
        # print("\nids: ", len(ids), ids)

    # List of 20 unique colors in GLB-compatible format
    colors = [
        [0.596, 0.9843, 0.596, 1],   # Mint Green
        [1, 0.7529, 0.7961, 1],     # Soft Pink
        [0.6784, 0.8471, 0.902, 1], # Light Blue
        [0.8, 0.6, 0.8, 1],         # Light Purple
        [1, 0.6, 0.6, 1],           # Light Red
        [1, 0.8, 0.6, 1],           # Light Orange
        [1, 1, 0.5, 1],             # Light Yellow
        [1, 1, 0, 1],               # Yellow
        [0, 1, 0, 1],               # Green
        [0, 0, 1, 1],               # Blue
        [0.5, 0, 0.5, 1],           # Purple
        [1, 0, 0, 1],               # Red
        [1, 1, 1, 1],               # White
        [0.7, 0.7, 0.7, 1],         # Gray
        [0.9, 0.9, 0.9, 1],         # Light Gray
        [0.6, 0.6, 1, 1],           # Lighter Blue
        [0.6, 1, 0.6, 1],           # Lighter Green
        [1, 0.6, 0.6, 1],           # Lighter Red
        [1, 1, 0.6, 1],             # Lighter Yellow
        [0, 0.7, 0.7, 1],           # Lighter Teal
    ]*150

    rgb = colors[tile_id]
    print("\ntile No.{} rgb: ".format(tile_id), rgb)
    print("\n")


    # Set glb data
    # Initialization and generate the glTF file
    glb_generator = GLB()
    glbBytesData = glb_generator.draw_glb(pos, nor, ids, indices, rgb)

    conn.commit()

    return glbBytesData


def compose_featureTable_batchTable(oid_list, attrib_object, results):
    #------------------------------------------------featureTable start------------------
    object_count = len(oid_list)
    # Set the Feature Table
    json_data = {"BATCH_LENGTH": object_count }  #,"RTC_CENTER":[1215019.2111447915,-4736339.477299974,4081627.9570209784]
    featureTableData = json.dumps(json_data, separators=(',', ':'))
    #------------------------------------------------featureTable end--------------------

    #------------------------------------------------batchTable start--------------------
    # Capitalize each element in the list
    Cap_attrib_list = [attrib.capitalize() for attrib in list(attrib_object.keys())]
    # Adding "ID" to Cap_attrib_list
    # Cap_attrib_list.extend(["ID"])

    ### loop attrib name, attrib type, attrib value
    json_dict = {}
    for idx in range(len(results[0])):
        if idx == 0 or idx == 1:
            pass
        else:
            attrib = Cap_attrib_list[idx - 2]
            # print("attrib:", attrib)
            attrib_lower = attrib.lower()
            attrib_type = str(attrib_object[attrib_lower])
            # print(attrib_type, type(attrib_type))
            
            if attrib_type == 'float':
                values = [float(i[idx]) for i in results]
            if attrib_type == 'int':
                values = [int(i[idx]) for i in results]    
            if attrib_type == 'text':
                values = [str(i[idx]) for i in results]
            else:
                pass
            
            json_dict[attrib] = values

    json_dict["ID"] = list(range(len(json_dict[Cap_attrib_list[0]])))

    properties = json_dict
    # print("properties", properties)

    # Set the Batch Table data
    batchTableData = json.dumps(properties, separators=(',', ':'))
    #------------------------------------------------batchTable end--------------------


    return featureTableData, batchTableData


def write_b3dm(conn, cursor, tile_id, index_flag, sql_filter, attrib_object):

    glbBytesData, featureTableData, batchTableData = fetch_tile(conn, cursor, tile_id, index_flag, sql_filter, attrib_object)

    # Create an instance of the B3DM class
    b3dm = B3DM()
    # generate b3dm
    b3dm_bytes = b3dm.draw_b3dm(featureTableData, batchTableData, glbBytesData)


    # # write for debuging
    # glb_bytes = glbBytesData
    # write_path = "DBtest_b3dm/{}.glb".format(tile_id)
    # with open(write_path, 'wb') as glb_f:
    #     glb_f.write(glb_bytes)

    # output = b3dm_bytes
    # write_path = "DBtest_b3dm/{}.b3dm".format(tile_id)
    # with open(write_path, 'wb') as b3dm_f:
    #     b3dm_f.write(output)

    sql = "UPDATE hierarchy SET b3dm = %s WHERE temp_tile_id = %s and level = 2"
    values = (b3dm_bytes, tile_id)
    cursor.execute(sql, values)
    print("write tile successfully: {0}".format(tile_id))

    conn.commit() 

    return 0


def write_glb(conn, cursor, tile_id, index_flag, sql_filter):

        sql = """
        WITH UnnestedIDs AS (
        SELECT object_id as oid_list, temp_tile_id
        FROM hierarchy
        WHERE level = (SELECT level FROM hierarchy ORDER BY level DESC LIMIT 1) AND
        temp_tile_id = {0}
        )
        SELECT id, nodes 
        FROM object 
        WHERE id IN (SELECT unnest(oid_list) FROM UnnestedIDs) 
        {1}
        ORDER BY id;
        """.format(tile_id, sql_filter)
        # print(sql)

        cursor.execute(sql)
        results = cursor.fetchall()
        # print(results)

        oid_list= [int(i[0]) for i in results]
        # print("tile No.{} object_id list: ".format(tile_id),oid_list)

        nodes_values= [i[1] for i in results]
        # print('nodes_values: ', nodes_values)

        # -----------------------------------------gltf start--------------------------------
        glbBytesData = compose_gltf(conn, cursor, tile_id, oid_list, nodes_values, index_flag)
        # -----------------------------------------gltf end--------------------------------

        sql = "UPDATE hierarchy SET glb = %s WHERE temp_tile_id = %s and level = 2"
        values = (glbBytesData, tile_id)
        cursor.execute(sql, values)
        print("write tile successfully: {0}".format(tile_id))

        conn.commit()

        return 0



def array_coord(conn, cursor):

    sql = "SELECT id, object_id, position FROM face WHERE position is not null ORDER BY id;"
    cursor.execute(sql)

    results = cursor.fetchall()
    # print("results: ", results)
    # print("results length: ", len(results))
    # print("results id: ", results[0][0])
    # print("results oid: ", results[0][1])
    # print("results pos: ", results[0][2])

    pos_by_oid = {}
    fids_by_oid = {}
    

    for row in results:
        # print(row)
        fid, oid, pos= row
        # print("fid: ", fid)
        # print("oid: ", oid)
        # print("pos: ", pos)


        if oid not in pos_by_oid:
            # If object_id is not yet in the dictionary, create a new array for positions
            pos_by_oid[oid] = [pos]
            fids_by_oid[oid] = [fid]
        else:
            # If object_id already exists, append the position to its corresponding array
            pos_by_oid[oid].append(pos)
            fids_by_oid[oid].append(fid)

        # print('dic: ', len(pos_by_oid), pos_by_oid)
        # print('dic: ', len(fids_by_oid), fids_by_oid)

    

    for key, item in pos_by_oid.items():
        # print(f"Key: {key}, Item: {item}")

        oid = key
        pos = item
        fids = fids_by_oid[oid]
        # print('oid: ',  oid)
        # print('pos: ', len(pos), pos)
        # print('fids: ', len(fids), fids)

        pos_collection = [point for sublist in pos for point in sublist]
        # print('pos collection: ', len(pos_collection), pos_collection) 


        p_unique = []
        threshold = 1e-6     #1e-10
        for point in pos_collection:
            is_unique = True
            for unique_point in p_unique:
                if arrays_equal(point, unique_point, threshold):
                    is_unique = False
                    break
            if is_unique:
                p_unique.append(point)

        # print('p_unique: ', len(p_unique), p_unique) 
        sql = "UPDATE object SET nodes = %s WHERE id = %s"
        values = (p_unique, oid)
        cursor.execute(sql, values)


        #face_array = [[0.0, 1.0, 0.0], [1.0, 0.0, 0.0], [1.0, 1.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 0.0], [1.0, 0.0, 0.0]]


        for fid, face_array in zip(fids, pos):
            indices_in_p_unique = []
            for point in face_array:
                found = False
                for i, unique_point in enumerate(p_unique):
                    if arrays_equal(point, unique_point, threshold):
                        indices_in_p_unique.append(i)  # Found similar point, store its index
                        found = True
                        break
                
                if not found:
                    indices_in_p_unique.append(None)  # Point not found, store None

            # print('indices_in_p_unique: ', len(indices_in_p_unique), indices_in_p_unique)
            sql = "UPDATE face SET tri_node_id = %s WHERE id = %s"
            values = (indices_in_p_unique, fid)
            cursor.execute(sql, values)

    conn.commit() 

    return 0


def arrays_equal(arr1, arr2, threshold):
    # Check if two arrays are equal based on a threshold
    if len(arr1) != len(arr2):
        return False  # Arrays of different lengths are not equal
    
    for i in range(len(arr1)):
        if abs(arr1[i] - arr2[i]) > threshold:
            return False  # If the absolute difference is greater than threshold, arrays are not equal
    
    return True 


def rotate_X(x, y, z, alpha):
    x_r = x
    y_r = np.cos(alpha)*y - np.sin(alpha)*z
    z_r = np.sin(alpha)*y + np.cos(alpha)*z
    # print(f"{(x, y, z)} rotate {alpha*(180/np.pi)} degrees around the X-axis,result {(x_r, y_r, z_r)}")
    return x_r, y_r, z_r


def rotate_Y(x, y, z, beta):
    x_r = np.cos(beta)*x + np.sin(beta)*z
    y_r = y
    z_r = -np.sin(beta)*x + np.cos(beta)*z
    # print(f"{(x, y, z)} rotate {beta*(180/np.pi)} degrees around the Y-axis,result {(x_r, y_r, z_r)}")
    return x_r, y_r, z_r


def delaunay_tess(conn, cursor):

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


def tess(conn, cursor):

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


def triangulation(conn, cursor, flag):

    if flag == "tesselation":
        tess(conn, cursor)
    else:
        delaunay_tess(conn, cursor)

    conn.commit() 
    # cursor.close()
    # conn.close()  
    return 0


def k_means(conn, cursor, cnum1, cnum2):

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
        SELECT ST_ClusterKMeans(cc, {0})  OVER()  AS cid, id as object_id, cc
        FROM temp_centroids AS obj
    ) level_0_clusters;


    -- Subsequent levels of clustering (if required)
    -- Example: Second level clustering
    INSERT INTO hierarchical_clusters(object_id, level, cluster_id, parent_cluster_id, name)
    SELECT object_id, 2 AS level, cid AS cluster_id, parent_cluster_id, 'Level_1_Cluster_' || parent_cluster_id || '_SubCluster_' || cid AS name
    FROM (
        SELECT 
            ST_ClusterKMeans(o.cc, {1}) OVER(PARTITION BY t.cluster_id ORDER BY t.cluster_id) AS cid, 
            id AS object_id, 
            t.cluster_id AS parent_cluster_id
        FROM hierarchical_clusters t
        JOIN temp_centroids o ON t.object_id = o.id
        WHERE t.level = 1 -- Consider removing specific Cluster_id filter here
    ) level_1_clusters;

    DROP TABLE IF EXISTS hierarchy CASCADE;
    --DETAIL:  view vw_tile depends on table hierarchy
    --HINT:  Use DROP .. CASCADE to    delete dependent objects altogether

    CREATE TABLE hierarchy AS
    SELECT (ARRAY_AGG(DISTINCT level))[1] AS level, 
    ARRAY_AGG(object_id) AS object_id,
    (ARRAY_AGG(DISTINCT cluster_id))[1] AS cluster_id,   
    (ARRAY_AGG(DISTINCT parent_cluster_id))[1] AS parent_cluster_id,
    --ST_AsText(ST_Collect(envelope))
    --ST_Extent(envelope) AS envelope, 
    ST_3DExtent(envelope) AS envelope
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
    # cursor.close()
    # conn.close()    
    return 0


def schema_update(conn, cursor):

    cursor.execute("""
    ALTER TABLE face
    DROP COLUMN fid,
    DROP COLUMN position,
    DROP COLUMN tri_index,
    DROP COLUMN pos_idx_test,
    DROP COLUMN if_planar,
    DROP COLUMN polygon;
    --ALTER TABLE hierarchy
    --DROP COLUMN h_envelope;
    DROP TABLE IF EXISTS tile;
    DROP table IF EXISTS temp;
    """)
    conn.commit() 
    # cursor.close()
    # conn.close() 
    return 0


def input_data(conn, cursor, object_table, face_table):

    # # test cubes
    # cursor.execute(
    # """
    # INSERT INTO object (id)
    # SELECT id
    # FROM object_input;
    # INSERT INTO face (id, object_id, polygon)
    # SELECT id, object_id, polygon
    # FROM face_input;
    # """
    # )


    cursor.execute(
    """
    INSERT INTO object (id)
    SELECT id
    FROM {0};
    INSERT INTO face (id, object_id, polygon)
    SELECT id, object_id, polygon
    FROM {1}
    ORDER BY id;
    """.format(object_table, face_table)
    )

    conn.commit() 
    # cursor.close()
    # conn.close()   
    return 0 



# # test cube
# cursor.execute(
# """
# -- Inserting a record into the 'object' table
# INSERT INTO object (id)
# VALUES (1);

# -- Inserting records into the 'face' table with respective polygons
# INSERT INTO face (id, object_id, polygon)
# VALUES
#     (1, 1, ST_MakePolygon(ST_GeomFromText('LINESTRING(0 0 0, 1 0 0, 1 1 0, 0 1 0, 0 0 0)'))),
#     (2, 1, ST_MakePolygon(ST_GeomFromText('LINESTRING(0 0 0, 0 1 0, 0 1 1, 0 0 1, 0 0 0)'))),
#     (3, 1, ST_MakePolygon(ST_GeomFromText('LINESTRING(0 0 0, 0 0 1, 1 0 1, 1 0 0, 0 0 0)'))),
#     (4, 1, ST_MakePolygon(ST_GeomFromText('LINESTRING(1 1 1, 1 0 1, 0 0 1, 0 1 1, 1 1 1)'))),
#     (5, 1, ST_MakePolygon(ST_GeomFromText('LINESTRING(1 1 1, 0 1 1, 0 1 0, 1 1 0, 1 1 1)'))),
#     (6, 1, ST_MakePolygon(ST_GeomFromText('LINESTRING(1 1 1, 1 1 0, 1 0 0, 1 0 1, 1 1 1)')));

# """
# )



# # test polygon
# cursor.execute(
# """
# -- Inserting a record into the 'object' table
# INSERT INTO object (id)
# VALUES (1);

# -- Inserting records into the 'face' table with respective polygons
# INSERT INTO face (id, object_id, polygon)
# VALUES
#     (1, 1, ST_MakePolygon(ST_GeomFromText('LINESTRING(0 0 0, 1 0 0, 1 1 0, 0 1 0, 0 0 0)')));

# """
# )



if __name__ == "__main__":
  
    pass


    #-------------------------------test function------------------------------------------------
    # positions, normals, ids, indices = fetch_pos_nor_ids_indices(tile_id)
    # print(type(positions[0][0]))
    # print(type(normals[0][0]))
    # print(type(ids[0]))
    # print(type(indices[0]))

    # first, write bytes, then, fetch bytes
    # write_b3dm(tile_id)
    # fetch_tile(tile_id)


    # ------------------------------test function------------------------------------------------
    # attrib = ["height", "longitude", "latitude"]

    # properties = fetch_property(object_count, attrib)
    # print(properties)



    # # -------------------------------test glb-------------------------------------------------
    # positions, normals, ids, indices = fetch_pos_nor_ids_indices(object_count)


    # # Initialization and generate the glTF file
    # glb_generator = GLB()

    # glb_generator.update_value(positions, normals, ids, indices)
    
    # # glb_generator.export_glb("1106output.glb")

    # glb_bytes = glb_generator.draw_glb()
    # with open("1109output.glb", 'wb') as glb_f:
    #   glb_f.write(glb_bytes)


    # # -------------------------------test b3dm-------------------------------------------------
    # # Create an instance of the B3DM class
    # b3dm = B3DM()

    # # Set glb data
    # #glbBytesData = glb_bytes
    # glbBytesData = glb_test()

    # # # Set the Feature Table data
    # json_data = {"BATCH_LENGTH":object_count,"RTC_CENTER":[1215019.2111447915,-4736339.477299974,4081627.9570209784]}
    # # Serialize the JSON data to a string
    # featureTableData = json.dumps(json_data, separators=(',', ':'))


    # # # Set the Batch Table data
    # attrib = ["longitude", "latitude","height"]
    # json_object = fetch_property(object_count, attrib)
    # print(json_object)
    # # change 'height' to 'Height' for unknown reason???
    # modified_json_object = {
    # 'longitude': json_object['longitude'],
    # 'latitude': json_object['latitude'],
    # 'Height': json_object['height']
    # }

    # # Serialize the JSON object to a JSON string
    # batchTableData = json.dumps(modified_json_object, separators=(',', ':'))


    # # Write b3dm
    # output = b3dm.draw_b3dm(featureTableData, batchTableData, glbBytesData)
    # # output = b3dm.draw_b3dm(None, None, glbBytesData)

    # write_path = "test_b3dm/zoeyyy_test_table.b3dm"
    # with open(write_path, 'wb') as b3dm_f:
    #         b3dm_f.write(output)


