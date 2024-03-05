from flask import Flask, g, jsonify, Response, url_for, send_from_directory, send_file
import numpy as np
import json
import psycopg2 as pg
from sqlalchemy import create_engine

from glb_generator import GLB
from b3dm_generator import B3DM, glb_test

from tile_function import fetch_tile, fetch_tile_indexed_info


# # Load JSON file
# with open('input.json', 'r') as file:
#     data = json.load(file)

# # specfy the dataset theme 
# theme = "test"  # "37en2" # "campus_lod1"  #"campus"   # "37en2"
# sql_filter = data[theme]['filter']

sql_filter = ""


def create_app():
    app = Flask(__name__)


    # # https: // github.com / CesiumGS / cesium / wiki / CesiumJS - Features - Checklist
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

        # contents = fetch_tileset(theme)

        # tileset_file_path = 'data\\{}.json'.format(theme)

        # # write json file check
        # with open(tileset_file_path, 'w') as file:
        #     # Use the json.dump() function to write the data to the file.
        #     json.dump(contents, file, indent=2)
        # print(f'Data has been written to {tileset_file_path}')


        # database connection
        conn = pg.connect(dbname="sunrise", user="postgres", password="120598",
                          port="5432", host="localhost")
        engine = create_engine('postgresql://postgres:120598@localhost:5432/sunrise')

        id = 1
        # Create a cursor object
        cursor = conn.cursor()
        # Fetch data from the database
        # cursor.execute("SELECT * FROM tileset WHERE id = {0}".format(id))
        # tileset_data = cursor.fetchone()
        # print('tileset_data:')
        # print(tileset_data,'\n')

        cursor.execute("SELECT * FROM tile WHERE tileset_id = {0} and parent_id IS NULL".format(id))
        tile_data = cursor.fetchall()
        # print('tile_data: ')
        # print(tile_data,'\n')

        cursor.execute("SELECT * FROM tile WHERE tileset_id = {0} and parent_id IS NOT NULL ORDER BY id LIMIT 500".format(id))
        children_data = cursor.fetchall()
        # print('children_data: ', type(children_data))
        # print(children_data,'\n')

        # Structure the fetched data into a JSON structure
        # tileset, tile
        tileset_json = {
            "asset": 
                # tileset_data[1]
                {
                "version": "1.0",
                "tilesetVersion": "1.2.3"
                }
            ,
            "properties":
                # tileset_data[2]
                {
                "Height": {}
                }
            , 
            "geometricError": 200, #float(   _data[3]), 
            "root": {
                "boundingVolume": {
                    "box": [float(pt) for pt in tile_data[0][3]] #[float(pt) for pt in tileset_data[4]]  
                },
                "geometricError": float(tile_data[0][4]), 
                "refine": tile_data[0][5],
                "content": {
                    "boundingVolume": {
                    "box": [float(pt) for pt in tile_data[0][3]]  
                }
                },
                "children": []
            }
        }
        # print(tileset_json)
        # Check if tile_data[0][6] is not null
        if tile_data[0][6] is not None:
            tileset_json["root"]["content"]["uri"] = url_for('tiles_one_tile', tile_name = tile_data[0][0]) 


        # Add child to the root
        for child_data in children_data:
            child_tile = {
                "boundingVolume": {
                    "box": [float(pt) for pt in child_data[3]] 
                },
                "geometricError": float(child_data[4]), 
                "content": {
                    "uri":  url_for('tiles_one_tile', tile_name = child_data[0]) #url_for('tiles_one_tile')
                }
            }
            tileset_json["root"]["children"].append(child_tile)
        #     print(child_tile, "\n")

        formatted_json = json.dumps(tileset_json, indent=4)
        print("tileset_json: \n", formatted_json, "\n")
        # print("tileset_json type: ", type(tileset_json))

        # write json file
        output = formatted_json
        write_path = "test_b3dm/{}.json".format('tileset')
        with open(write_path, "w") as json_file:
            json.dump(output, json_file)


        contents = tileset_json

        return jsonify(contents)


    # # tile_name
    # # <string:tile_name> # url_for('tiles_one_tile', tile_name = "parent")
    # # <int:tile_name> # url_for('tiles_one_tile', tile_name = 1)
    # @app.route("/tiles/<string:tile_name>.glb")  
    # def tiles_one_tile(tile_name):
    #     #------------------------------------------dataset-----------------------------------------------
    #     #positions, normals, ids, indices = read_data("data/parent_pos_nor_id.json")
    #     #------------------------------------------dataset-----------------------------------------------
    #     #  object_count  defines how the objects in this tile
    #     object_count = 2
    #     positions, normals, ids, indices = fetch_pos_nor_ids_indices(object_count)

    #     # Initialization and generate the glTF file
    #     glb_generator = GLB()

    #     glb_generator.update_value(positions, normals, ids, indices)

    #     glb_bytes = glb_generator.draw_glb()
    #     #------------------------------------------dataset-----------------------------------------------
    #     # # read from path
    #     # glb_file_path = f"data\\{tile_name}.glb"   
    #     # glb_bytes = read_glb(glb_file_path)
    #     #------------------------------------------dataset end-----------------------------------------------
    #     response = Response(glb_bytes, mimetype="application/octet-stream")
    #     return response

    

    @app.route("/tiles/<string:tile_name>.b3dm")  
    def tiles_one_tile(tile_name):
        route_start_time = time.time()

        # experiments: fetch one specific tile
        # tile_name = 2

        # defines which tile
        tile_id = int(tile_name)


        # # ----------------------Approach I: fetch pre-created b3dm from DB start-------------------------------------
        # b3dm_bytes = fetch_tile(tile_id)

        # # ----------------------Approach I: fetch pre-created b3dm from DB end---------------------------------------


        # # ---------------------------Approach II: generate b3dm from DB start----------------------------------------

        positions, normals, indices, ids, featureTableData, batchTableData = fetch_tile_indexed_info(tile_id, sql_filter) 

        # indices = None

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
        ]*500

        rgb = colors[tile_id]
        print("\ntile No.{} rgb: ".format(tile_id), rgb)
        print("\n")


        # Create an instance of the B3DM class
        b3dm = B3DM()
        # Set glb data
        # Initialization and generate the glTF file
        glb_generator = GLB()
        glbBytesData = glb_generator.draw_glb(positions, normals, ids, indices, rgb)
        # generate b3dm
        b3dm_bytes = b3dm.draw_b3dm(featureTableData, batchTableData, glbBytesData)

        # # ---------------------------Approach II: generate b3dm from DB end----------------------------------------


        # # --------------------------------------------Approach III: read from path-------------------------------------
        # b3dm_file_path = f"test_b3dm\\{tile_name}.b3dm"   
        # b3dm_bytes = read_glb(b3dm_file_path)
        # # --------------------------------------------Approach III: read from path-------------------------------------


        # Record the end time for this route
        route_end_time = time.time()
        # Calculate the execution time for this route
        route_execution_time = route_end_time - route_start_time
        print(f"Execution time for b3dm fetch: {route_execution_time} seconds")

        # fetch_time = fetch_time + route_execution_time
        # print(f"Total time for b3dm fetch: {fetch_time} seconds")

        # ---------------------------------write for debuging-------------------------------------------------------
        # glb_bytes = glbBytesData
        # write_path = "test_b3dm/{}.glb".format(tile_id)
        # with open(write_path, 'wb') as glb_f:
        #     glb_f.write(glb_bytes)

        output = b3dm_bytes
        write_path = "test_b3dm/{}.b3dm".format(tile_id)
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
            # Read the entire contents of the GLB file as bytes
            glb_bytes = glb_file.read()

        # Now, 'glb_bytes' contains the binary data of the GLB file
        # You can process, manipulate, or save these bytes as needed

        # For example, you can print the length of the byte data
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


# test purpose
def fetch_tileset(example):

    if example == "multiple_boxes" or "parent" or "cube_and_icosphere" or "3DTilesFormats_region" or "3DTilesFormats_box" or "3DTilesFormats_box_b3dm":
        tileset_file_path = 'data\\{}.json'.format(theme)

        # read tileset directly
        f = open(tileset_file_path)
        tileset_json = json.load(f)

    # if example == "3DTilesFormats":
    #     tileset_file_path = 'data\\3DTilesFormats.json'

    #     # read tileset directly
    #     f = open(tileset_file_path)
    #     tileset_json = json.load(f)


    if example == "one_box":
        # database connection
        conn = pg.connect(dbname="3dtiles", user="postgres", password="120598",
                            port="5432", host="localhost")
        engine = create_engine('postgresql://postgres:120598@localhost:5432/zoey')

        # Create a cursor object
        cursor = conn.cursor()
        # Fetch data from the database
        cursor.execute("SELECT * FROM tileset WHERE id = 1")
        tileset_data = cursor.fetchone()
        # print('tileset_data: ')
        # print(tileset_data)

        cursor.execute("SELECT * FROM tile WHERE tileset_id = 1")
        tile_data = cursor.fetchall()
        # print('tile_data: ')
        # print(tile_data)

        cursor.execute("SELECT * FROM content WHERE tile_id = 1")
        children_data = cursor.fetchall()
        # print('children_data: ')
        # print(children_data)

        # Structure the fetched data into a JSON structure
        tileset_json = {
            "asset": {
                "version": tileset_data[1]  # Assuming 'version' is in the second column
            },
            "geometricError": float(tileset_data[2]),  # Assuming 'geometricError' is in the third column
            "root": {
                "boundingVolume": {
                    "box": [float(pt) for pt in tile_data[0][2]]  # Assuming 'bounding_volume' is in the third column
                },
                "geometricError": float(tile_data[0][3]),  # Assuming 'geometricError' is in the fourth column
                "children": [],
                "refine": tile_data[0][4],
                "transform": [float(pt) for pt in tile_data[0][5]]
            }
        }
        # print(tileset_json)

        # Add child tiles to the root tile's "children" list
        for child_data in children_data:
            child_tile = {
                "boundingVolume": {
                    "box": [float(pt) for pt in child_data[2]]  # Assuming 'bounding_volume' is in the third column
                },
                "geometricError": float(child_data[3]),  # Assuming 'geometricError' is in the fourth column
                "children": [],
                "refine": child_data[5],  # Assuming 'refine' is in the sixth column
                "content": {
                    "uri":  url_for('tiles_one_tile', tile_name = "1")  # tile_name =  # "1"
                }
            }
            tileset_json["root"]["children"].append(child_tile)


    return tileset_json


if __name__ == "__main__":
    
    import time

    start_time = time.time()

    app = create_app()
    app.run(debug=True)

    end_time = time.time()
    execution_time = end_time - start_time

    print(f"Total execution time: {execution_time} seconds")