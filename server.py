# based on: https://gist.github.com/vulcan25/55ce270d76bf78044d067c51e23ae5ad
from psycopg2 import pool

from flask import Flask, g, jsonify, Response, url_for, send_from_directory, send_file
import numpy as np
import json
import psycopg2 as pg
from sqlalchemy import create_engine

from glb_generator import GLB
from b3dm_generator import B3DM, glb_test

from tile_function import fetch_tile, fetch_featureTable_batchTable

from dbconfig import config
from tile_creator import tiles_creator


def get_db():
    print("Getting DbConnection")
    if "db" not in g:
        g.db = app.config["postgreSQL_pool"].getconn()
    return g.db


def create_app(dataset_theme):
    app = Flask(__name__)

    # Load JSON file
    with open('input.json', 'r') as file:
        data = json.load(file)

    
    # Extract info
    theme_data = data[dataset_theme]
    attrib_object_str = theme_data['property']
    attrib_object = json.loads(attrib_object_str.replace("'", "\"")) # Convert the string to a dictionary
    sql_filter = theme_data['filter']
    index_flag = int(theme_data['index_flag']) 
    b3dm_flag = int(theme_data["b3dm_flag"])
    glb_flag = int(theme_data["glb_flag"])

    
    conn_params = config()
    app.config["postgreSQL_pool"] = pool.SimpleConnectionPool(
        1,
        20,
        user=conn_params["user"],
        password=conn_params["password"],
        host=conn_params["host"],
        port=conn_params["port"],
        database=conn_params["database"],
    )

    @app.teardown_appcontext
    def close_conn(e):
        db = g.pop("db", None)
        if db is not None:
            print("Closing DbConnection")
            app.config["postgreSQL_pool"].putconn(db)

    # @app.route("/Cesium-1.91/<path:name>")
    # def ui(name):
    #     print("ui route")
    #     return send_from_directory("Cesium-1.91", name, as_attachment=False)

    # @app.route("/")
    # def index():
    #     print("Index route")
    #     return send_file("static/index.html")


    # @app.route("/ui/")
    # def cesium_ui():
    #     # print("cesium_ui route")
    #     # return send_file("static/cesium_ui_server.html")
        
    #     print("cesium_ui route")

    #     # Record the start time for this route
    #     route_start_time = time.time()

    #     # Perform the task for this route (e.g., sending a file)
    #     file_sent = send_file("static/cesium_ui_server.html")

    #     # Record the end time for this route
    #     route_end_time = time.time()

    #     # Calculate the execution time for this route
    #     route_execution_time = route_end_time - route_start_time

    #     print(f"Execution time for cesium_ui route: {route_execution_time} seconds")

    #     return file_sent

    # -----------------------------------Cesium sandcastle start---------------------------------------------------------------------------
    @app.route("/Cesium-1.110/<path:name>")
    def ui(name):
        print("ui route")
        return send_from_directory("Cesium-1.110", name, as_attachment=False)

    @app.route("/")
    def index():
        print("Index route")
        
        # with open("index.html", "r") as file:
        #     html_content = file.read()
        #
        # return html_content
        return send_file("static/index.html")

    @app.route("/ui/")
    def cesium_ui():
        print("cesium_ui route")
        return send_file("static/cesium_ui_server_map.html")
    # -----------------------------------Cesium sandcastle end---------------------------------------------------------------------------
    
    
    @app.route("/tiles/tileset.json")  
    # @app.route("/tiles/tileset.json/<theme>") # def tiles_tileset(theme)
    def tiles_tileset():
        print("tiles_tileset")

        # database connection
        conn = get_db()

        tileset_id = 1
        # Create a cursor object
        cursor = conn.cursor()

        #-----------------------------------------------------file
        # # load the JSON file
        # read_path = "WEBtest_b3dm/tileset.json"
        # with open(read_path, "r") as json_file:
        #     tileset_json = json.load(json_file)
        #-----------------------------------------------------file


        #------------------------------------------------------vw
        cursor.execute("SELECT tileset_json FROM vw_tileset WHERE id = {0};".format(tileset_id))
        vw_tileset = cursor.fetchall()
        tileset_json = vw_tileset[0][0]
        # print('json type', type(tileset_json))
        #------------------------------------------------------vw

        # write dictionary to file with indentation
        data = tileset_json
        write_path = "WEBtest_b3dm/{}.json".format('tileset')
        with open(write_path, "w") as json_file:
            json.dump(data, json_file, indent=4)

        contents = tileset_json

        return jsonify(contents)
    

    @app.route("/tiles/<string:tile_name>.b3dm")  
    def tiles_one_tile(tile_name):
        route_start_time = time.time()

        # database connection
        conn = get_db()
        # Create a cursor object
        cursor = conn.cursor()

        # experiments: fetch one specific tile
        # tile_name = 2

        # defines which tile
        tile_id = int(tile_name)
        

        # # ---------------------------Approach I: generate b3dm from DB start----------------------------------------
        if b3dm_flag == -1 and glb_flag == -1:
            glbBytesData, featureTableData, batchTableData = fetch_tile(conn, cursor, tile_id, index_flag, sql_filter, attrib_object)

            print("Approach 1: dynamic b3dm creation successfully: {0}".format(tile_id))
            # Create an instance of the B3DM class
            b3dm = B3DM()
            # generate b3dm
            b3dm_bytes = b3dm.draw_b3dm(featureTableData, batchTableData, glbBytesData)

        # # ---------------------------Approach I: generate b3dm from DB end----------------------------------------

        # # ----------------------Approach II: fetch precomposed glb from DB start-------------------------------------
        elif glb_flag == 1:
            featureTableData, batchTableData = fetch_featureTable_batchTable(conn, cursor, tile_id, sql_filter, attrib_object)

            sql = "SELECT glb from hierarchy where temp_tile_id = {0} and level =2".format(tile_id)
            cursor.execute(sql)
            print("Approach 2: fetch precomposed binary gltf successfully: {0}".format(tile_id))
            results = cursor.fetchall()
            glbBytesData = results[0][0]
            conn.commit() 

            # Create an instance of the B3DM class
            b3dm = B3DM()
            # generate b3dm
            b3dm_bytes = b3dm.draw_b3dm(featureTableData, batchTableData, glbBytesData)

        # # ----------------------Approach II: fetch precomposed glb from DB end---------------------------------------


        # # ----------------------Approach III: fetch precomposed b3dm from DB start-------------------------------------
        elif b3dm_flag == 1:
            sql = "SELECT b3dm from hierarchy where temp_tile_id = {0} and level =2".format(tile_id)
            cursor.execute(sql)
            print("Approach 3: fetch precomposed b3dm successfully: {0}".format(tile_id))
            results = cursor.fetchall()
            b3dm_bytes = results[0][0]
            conn.commit() 

        # # ----------------------Approach III: fetch precomposed b3dm from DB end---------------------------------------
  

        # # --------------------------------------------Approach IV: read from path-------------------------------------
        else:
            b3dm_file_path = f"WEBtest_b3dm\\{tile_name}.b3dm"   
            b3dm_bytes = read_glb(b3dm_file_path)
            print("Approach 4:read b3dm from the disk successfully: {0}".format(tile_id))
        # # --------------------------------------------Approach IV: read from path-------------------------------------


        # Record the end time for this route
        route_end_time = time.time()
        # Calculate the execution time for this route
        route_execution_time = route_end_time - route_start_time
        print(f"Execution time for b3dm fetch: {route_execution_time} seconds")

        # ---------------------------------write for debuging-------------------------------------------------------
        # glb_bytes = glbBytesData
        # write_path = "WEBtest_b3dm/{}.glb".format(tile_id)
        # with open(write_path, 'wb') as glb_f:
        #     glb_f.write(glb_bytes)

        output = b3dm_bytes
        write_path = "WEBtest_b3dm/{}.b3dm".format(tile_id)
        with open(write_path, 'wb') as b3dm_f:
                b3dm_f.write(output)
        # ---------------------------------write for debuging-------------------------------------------------------


        response = Response(b3dm_bytes, mimetype="application/octet-stream")

        print("b3dm ready to send: {0}.b3dm".format(tile_name))
        return response

    return app


def read_glb(glb_file_path): # can also read b3dm
    try:
        # Open the GLB file in binary mode
        with open(glb_file_path, 'rb') as glb_file:

            glb_bytes = glb_file.read()

        print(f"GLB file size: {len(glb_bytes)} bytes")
        # print(glb_bytes)

    except FileNotFoundError:
        print(f"GLB file not found at {glb_file_path}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

    return glb_bytes


def read_data(file_path): 

    # Open the JSON file in read mode # 'rb' "read binary"
    with open(file_path, 'r') as file:
        # Load and parse the JSON data
        data = json.load(file)

    # Access specific keys in the JSON data
    positions = data["position"]
    normals = data["normal"]
    ids = data["ID"]
    indices = data["indices"]

    return positions, normals, ids, indices


if __name__ == "__main__":
    
    import time

    # specfy the dataset theme 
    dataset_theme = "test" #"cube"  #"37en2" # # "37en1"  # "37en2" # "campus_lod1"

    #----------------------------------------------3D Tiles database------------------------------------------

    refine = 'ADD'

    # total time start
    total_start_time = time.time()
    
    tiles_creator(dataset_theme, refine)

    # total time end
    total_end_time = time.time()
    execution_time = total_end_time - total_start_time
    print(f"Total execution time: {execution_time} seconds")
    #----------------------------------------------3D Tiles database------------------------------------------

    start_time = time.time()

    app = create_app(dataset_theme)
    app.run(debug=False) #debug=True

    end_time = time.time()
    execution_time = end_time - start_time

    print(f"Total execution time: {execution_time} seconds")