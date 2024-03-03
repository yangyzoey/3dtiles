import psycopg2 as pg
from sqlalchemy import create_engine
import numpy as np
import json

from glb_generator import GLB
from b3dm_generator import B3DM, glb_test


def fetch_tile_info(tile_id):

        # database connection
        conn = pg.connect(dbname="sunrise", user="postgres", password="120598",
                                    port="5432", host="localhost")

        engine = create_engine('postgresql://postgres:120598@localhost:5432/sunrise')

        cur = conn.cursor()


        # update position, indices in table face
        pos = []
        nor = []
        indices = []
        ids = []


        sql = "SELECT id, height, nodes FROM object WHERE tile_id = {0} ORDER BY id".format(tile_id)
        cur.execute(sql)
        results = cur.fetchall()
        # print(results)

        oid_list= [int(i[0]) for i in results]
        # print("tile No.{} object_id list: ".format(tile_id),oid_list)


        # # for test purpose, can be set as 1
        # oid_list = [7035064, 8683138, 7037093, 6215444, 7042150, 7042095, 760994, 6200408]


        height_values= [float(i[1]) for i in results]
        # print("height property list: ",height_values)


        nodes_values= [i[2] for i in results]
        # print('nodes_values: ', nodes_values)


        # # "longitude", "latitude"
        attrib_list = ["Height", "ID"]
        json_dict = {attrib_list[0]: height_values, attrib_list[1]: list(range(len(height_values)))}
        # print('')
        properties = json_dict
        # print("properties", properties)

        object_count = len(oid_list)


        # Set the Feature Table
        json_data = {"BATCH_LENGTH": object_count }  #,"RTC_CENTER":[1215019.2111447915,-4736339.477299974,4081627.9570209784]
        featureTableData = json.dumps(json_data, separators=(',', ':'))
        # Set the Batch Table data
        batchTableData = json.dumps(properties, separators=(',', ':'))


        object_vert_total = 0
        for idx, object_id in enumerate(oid_list):
            
            sql = "SELECT normal, tri_node_id FROM face \
            where object_id = {0} and tri_node_id is not null \
            ORDER BY id;".format(object_id)
            # print(sql)
            cur.execute(sql)

            res = cur.fetchall()
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


        conn.commit() 
        cur.close()
        conn.close()    # Close the database connection


        return pos, nor, indices, ids, featureTableData, batchTableData


def fetch_tile_indexed_info(tile_id):

        # database connection
        conn = pg.connect(dbname="sunrise", user="postgres", password="120598",
                                    port="5432", host="localhost")

        engine = create_engine('postgresql://postgres:120598@localhost:5432/sunrise')

        cur = conn.cursor() # Create a cursor object


        # update position, indices in table face
        pos = []
        nor = []
        indices = []
        ids = []

        sql = "SELECT id, height, nodes FROM object WHERE tile_id = {0} ORDER BY id".format(tile_id)
        cur.execute(sql)
        results = cur.fetchall()
        # print(results)

        oid_list= [int(i[0]) for i in results]
        # print("tile No.{} object_id list: ".format(tile_id),oid_list)


        # for test purpose, can be set as 1
        # oid_list = [1]

        nodes_values= [i[2] for i in results]
        # print('nodes_values: ', nodes_values)


        height_values= [float(i[1]) for i in results]
        # print("height property list: ",height_values)

        # # "longitude", "latitude"
        attrib_list = ["Height", "ID"]
        json_dict = {attrib_list[0]: height_values, attrib_list[1]: list(range(len(height_values)))}
    
        properties = json_dict
        # print("properties", properties)

        object_count = len(oid_list)


        # Set the Feature Table
        json_data = {"BATCH_LENGTH": object_count }  #,"RTC_CENTER":[1215019.2111447915,-4736339.477299974,4081627.9570209784]
        featureTableData = json.dumps(json_data, separators=(',', ':'))
        # Set the Batch Table data
        batchTableData = json.dumps(properties, separators=(',', ':'))


        object_vert_total = 0
        
        for idx, object_id in enumerate(oid_list):
            
            sql = "SELECT normal, tri_node_id FROM face \
            where object_id = {0} and tri_node_id is not null \
            ORDER BY id;".format(object_id)
            # print(sql)
            cur.execute(sql)

            res = cur.fetchall()
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
        # indices = list(range(len(pos)))


        # print("\npos: ", len(pos), pos)
        # print("\nnor: ", len(nor), nor)
        # print("\nindices: ", len(indices), indices)
        # print("\nids: ", len(ids), ids)



        # positions = pos
        # ids = ids
        # normals = nor
        # indices = indices
        # rgb = [1, 0.75, 0.8, 1]
        # # Initialization and generate the glTF file
        # glb_generator = GLB()
        # glb_bytes = glb_generator.draw_glb(positions, normals, ids, indices, rgb)
        # glb_generator.export_gltf_file(positions, normals, ids, indices, rgb, "1125output.json")
        # write_path = "1125draw.glb"
        # with open(write_path, 'wb') as glb_f:
        #     glb_f.write(glb_bytes)


        conn.commit() 
        cur.close()
        conn.close()    # Close the database connection


        return pos, nor, indices, ids, featureTableData, batchTableData


def fetch_tile(tile_id):
    
    # database connection
    conn = pg.connect(dbname="sunrise", user="postgres", password="120598",
                                port="5432", host="localhost")
    engine = create_engine('postgresql://postgres:120598@localhost:5432/sunrise')
    cur = conn.cursor() # Create a cursor object


    # Define the SQL query with placeholders
    sql = "SELECT b3dm from tile where id = {0}".format(tile_id)
    cur.execute(sql)

    print("fetch tile successfully: {0}".format(tile_id))

    results = cur.fetchall()
    bytes = results[0][0]

    conn.commit() 
    cur.close()
    conn.close()    # Close the database connection

    return bytes


# 0: non-indexed; 1: indexed
def write_tile(tile_id, tag):
 

    # database connection
    conn = pg.connect(dbname="sunrise", user="postgres", password="120598",
                                port="5432", host="localhost")
    engine = create_engine('postgresql://postgres:120598@localhost:5432/sunrise')
    cur = conn.cursor() # Create a cursor object

    if tag == 0:
        # object_count  defines how the objects in this tile
        positions, normals, indices, ids, featureTableData, batchTableData = fetch_tile_info(tile_id)
    else:
        positions, normals, indices, ids, featureTableData, batchTableData =  fetch_tile_indexed_info(tile_id)


    # print("positions", positions)
    # print("normals", normals)


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

    # Create an instance of the B3DM class
    b3dm = B3DM()
    # Set glb data
    # Initialization and generate the glTF file
    glb_generator = GLB()
    glbBytesData = glb_generator.draw_glb(positions, normals, ids, indices, rgb)
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


    # Define the SQL query with placeholders
    sql = "UPDATE tile SET b3dm = %s WHERE id = %s"
    # Provide the values for the placeholders as a tuple
    values = (b3dm_bytes, tile_id)
    # Execute the query with the provided values
    cur.execute(sql, values)


    print("write tile successfully: {0}".format(tile_id))

    conn.commit() 
    cur.close()
    conn.close()    # Close the database connection
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


def input_data( object_table, face_table):
    # database connection
    conn = pg.connect(dbname="sunrise", user="postgres", password="120598",
                                port="5432", host="localhost")

    engine = create_engine('postgresql://postgres:120598@localhost:5432/sunrise')

    cursor = conn.cursor() # Create a cursor object

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
    FROM {};
    INSERT INTO face (id, object_id, polygon)
    SELECT id, object_id, polygon
    FROM {}
    ORDER BY id;
    """.format(object_table, face_table)
    )


    conn.commit() 
    cursor.close()
    conn.close()    


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


# # delft 37en2, set id and object_id for test
# cursor.execute(
# """
# INSERT INTO object (id)
# SELECT id
# FROM object_37en2
# WHERE id IN (7035064, 8683138, 7037093, 7042150, 7042095) --(7035064)
# ;
# INSERT INTO face (id, object_id, polygon)
# SELECT id, object_id, polygon
# FROM face_37en2
# WHERE object_id IN (7035064, 8683138, 7037093, 7042150, 7042095) --(7035064)
# ORDER BY id;
# """
# )




if __name__ == "__main__":
    tile_id = 1
    fetch_tile_indexed_info(tile_id)
    # fetch_tile_info(tile_id)


    # # database connection
    # conn = pg.connect(dbname="sunrise", user="postgres", password="120598",
    #                             port="5432", host="localhost")
    # engine = create_engine('postgresql://postgres:120598@localhost:5432/sunrise')
    # cursor = conn.cursor() # Create a cursor object
    # array_coord(conn, cursor)

    #-------------------------------test function------------------------------------------------
    # positions, normals, ids, indices = fetch_pos_nor_ids_indices(tile_id)
    # print(type(positions[0][0]))
    # print(type(normals[0][0]))
    # print(type(ids[0]))
    # print(type(indices[0]))

    # first, write bytes, then, fetch bytes
    # write_tile(tile_id)
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