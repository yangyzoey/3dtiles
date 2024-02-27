import struct
from glb_generator import GLB, read_data
import json


class B3DM:
    def __init__(self):
        self.header = {
            'magic': 0x6d643362, 
            'version': 1,
            'byteLength': 0,
            'featureTableJSONByteLength': 0,
            'featureTableBinaryByteLength': 0,
            'batchTableJSONByteLength': 0,
            'batchTableBinaryByteLength': 0,
        }


        self.headerBuffer = bytearray()
        self.featureTableBuffer = bytearray()
        self.batchTableBuffer = bytearray()
        self.glbBuffer = bytearray()


    def build_header_chunk(self):
        # Calculate the byteLength based on the provided formula
        self.header['byteLength'] = (
            28
            + len(self.featureTableBuffer)
            + 0 #featureTableBinaryByteLength
            + len(self.batchTableBuffer)
            + 0 #batchTableBinaryByteLength
            + len(self.glbBuffer)
        )
        

        # # Calculate padding for the byteLength to align to an 8-byte boundary
        # padding = 8 - (self.header['byteLength'] % 8)
        # if padding < 8:
        #     self.header['byteLength'] += padding
        
        self.header['featureTableJSONByteLength'] = len(self.featureTableBuffer)
        self.header['featureTableBinaryByteLength'] = 0 # method1 
        self.header['batchTableJSONByteLength'] = len(self.batchTableBuffer)
        self.header['batchTableBinaryByteLength'] = 0 # method1

        
        self.headerBuffer.extend(struct.pack('<III', self.header['magic'], self.header['version'], self.header['byteLength']))
        self.headerBuffer.extend(struct.pack('<I', self.header['featureTableJSONByteLength']))
        self.headerBuffer.extend(struct.pack('<I', self.header['featureTableBinaryByteLength']))
        self.headerBuffer.extend(struct.pack('<I', self.header['batchTableJSONByteLength']))
        self.headerBuffer.extend(struct.pack('<I', self.header['batchTableBinaryByteLength']))


    def build_feature_table_chunk(self, featureTableData):

        # Encode the UTF-8 string to bytes
        featureTablebytes = featureTableData.encode('utf-8')
        self.featureTableBuffer.extend(featureTablebytes)

        # 8-byte aligned
        # check for header and feature table
        total_len = 28 + len(self.featureTableBuffer)
        padding_count = (8 - total_len % 8) % 8  # Ensure padding_size is between 0 and 7
        # print("previous featureTable:", len(self.featureTableBuffer))
        # print("padding_count: ", padding_count)
        # # Convert the padding spaces to bytes (0x20)
        space_chars = b' ' * padding_count
        self.featureTableBuffer.extend(space_chars )
        # print("after featureTable:", len(self.featureTableBuffer))
        # print("\n")



    def build_batch_table_chunk(self, batchTableData):

        # Encode the UTF-8 string to bytes
        batchTablebytes = batchTableData.encode('utf-8')
        self.batchTableBuffer.extend(batchTablebytes)

        # 8-byte aligned
        # check for header and feature table and batchtable
        total_len = 28 + len(self.featureTableBuffer)+ len(self.batchTableBuffer)
        padding_count = (8 - total_len % 8) % 8  # Ensure padding_size is between 0 and 7
        # print("previous batchTable:", len(self.batchTableBuffer))
        # print("padding_count: ", padding_count)
        # # Convert the padding spaces to bytes (0x20)
        space_chars = b' ' * padding_count
        self.batchTableBuffer.extend(space_chars)
        # print("after batchTable:", len(self.batchTableBuffer))
        # print("\n")



    def build_glb_bytes_chunk(self, glbBytesData):
        self.glbBuffer = glbBytesData


    def draw_b3dm(self, featureTable, batchTable, glbBytes):
        """Generate the b3dm file to path
        """

        b3dm = bytearray()

        if featureTable == None or batchTable == None:
            # TEST without feature and batch table
            self.build_glb_bytes_chunk(glbBytes)
            # Build header last since it needs the total byte length
            self.build_header_chunk()

            b3dm.extend(self.headerBuffer)
            b3dm.extend(self.glbBuffer)

        else:
            # TEST with feature and batch table
            self.build_feature_table_chunk(featureTable)
            self.build_batch_table_chunk(batchTable)
            self.build_glb_bytes_chunk(glbBytes)
            # Build header last since it needs the total byte length
            self.build_header_chunk()

            b3dm.extend(self.headerBuffer)
            b3dm.extend(self.featureTableBuffer)
            b3dm.extend(self.batchTableBuffer)
            b3dm.extend(self.glbBuffer)

        return b3dm


# def read_glb(glb_file_path): 
#     try:
#         # Open the GLB file in binary mode
#         with open(glb_file_path, 'rb') as glb_file:
#             # Read the entire contents of the GLB file as bytes
#             glb_bytes = glb_file.read()

#         # Now, 'glb_bytes' contains the binary data of the GLB file
#         # You can process, manipulate, or save these bytes as needed

#         # For example, you can print the length of the byte data
#         print(f"GLB file size: {len(glb_bytes)} bytes")
#         # print(glb_bytes)

#     except FileNotFoundError:
#         print(f"GLB file not found at {glb_file_path}")
#     except Exception as e:
#         print(f"An error occurred: {str(e)}")

#     return glb_bytes




def glb_test():

    # test dataset 1
    positions, normals, ids, indices = read_data("data/parent_pos_nor_id.json")
    
    # # test dataset 2
    # positions = [[-0.5, -0.5, 0.5], [0.5, -0.5, 0.5], [0.5, -0.5, -0.5], [-0.5, -0.5, -0.5],
    #         [-0.5, -0.5, 0.5], [0.5, -0.5, 0.5], [0.5, 0.5, 0.5], [-0.5, 0.5, 0.5],
    #         [-0.5, 0.5, -0.5], [-0.5, 0.5, 0.5], [0.5, 0.5, -0.5], [0.5, 0.5, 0.5],
    #         [-0.5, -0.5, -0.5], [0.5, -0.5, -0.5], [0.5, 0.5, -0.5], [-0.5, 0.5, -0.5],
    #         [-0.5, -0.5, 0.5], [-0.5, 0.5, 0.5], [-0.5, 0.5, -0.5], [-0.5, -0.5, -0.5],
    #         [0.5, -0.5, -0.5], [0.5, 0.5, 0.5], [0.5, -0.5, 0.5], [0.5, 0.5, -0.5]]

    # normals = [[0, -1, 0]] * 4 + [[0, 0, 1]] * 4 + [[0, 1, 0]] * 4 + [[0, 0, -1]] * 4 + [[-1, 0, 0]] * 4 + [[1, 0, 0]] * 4

    # ids = [0] * 24

    # indices = [0, 3, 1, 3, 2, 1,
    #                 4, 6, 7, 4, 5, 6,
    #                 8, 9, 11, 8, 11, 10,
    #                 12, 14, 13, 12, 15, 14,
    #                 19, 16, 17, 19, 17, 18,
    #                 20, 21, 22, 20, 23, 21]


    # Initialization and generate the glTF file
    glb_generator = GLB()

    glb_generator.update_value(positions, normals, ids, indices)
    
    # glb_generator.export_glb("1102output.glb")

    glb_bytes = glb_generator.draw_glb()
    with open("1108web_generate.glb", 'wb') as glb_f:
      glb_f.write(glb_bytes)

    return glb_bytes


# # test magic number
# decimalNumber = 1835283298
# hexadecimalNumber = hex(decimalNumber) # 0x6d643362
# print(0x6d643362 == 1835283298)  #True


if __name__ == "__main__":
    # Create an instance of the B3DM class
    b3dm = B3DM()

    # Set glb data
    glbBytesData = glb_test()
    # # glb_bytes = read_glb("data/parent.glb")

    # # Set the Feature Table data
    #json_data = {"BATCH_LENGTH":10}
    json_data = {"BATCH_LENGTH":10,"RTC_CENTER":[1215019.2111447915,-4736339.477299974,4081627.9570209784]}
    # Serialize the JSON data to a string
    featureTableData = json.dumps(json_data, separators=(',', ':'))
    # Encode the JSON string to bytes
    # featureTableData = json_string.encode() #'utf-8'


    # # Set the Batch Table data
    #json_object = {"id": [0], "Longitude": [-1.31968]}
    json_object = {"Longitude":[-1.31968,-1.3196832683949145,-1.3196637662080655,-1.3196656317210846,-1.319679266890895,-1.319693717777418,-1.3196607462778132,-1.3196940116311096,-1.319683648959897,-1.3196959060375169],"Latitude":[0.698874,0.6988615321420496,0.6988736012180136,0.6988863062831799,0.6988864387845588,0.6988814788613282,0.6988618972526105,0.6988590050687061,0.6988690935212543,0.6988854945986224],"Height":[78.1558019220829,85.41026367992163,78.10224648751318,78.74249991215765,78.86988856643438,82.70132680051029,78.16386888921261,84.22482559457421,84.54620283842087,79.63207503221929]}
    #"id":[0,1,2,3,4,5,6,7,8,9]
    # Serialize the JSON object to a JSON string
    batchTableData = json.dumps(json_object, separators=(',', ':'))
    # Encode the JSON string into bytes (UTF-8 encoding is used by default)
    # batchTableData = json_string.encode()

    

    # Write b3dm
    output = b3dm.draw_b3dm(featureTableData, batchTableData, glbBytesData)
    # output = b3dm.draw_b3dm(None, None, glbBytesData)

    write_path = "test_b3dm/zoeyyy_test_table.b3dm"
    with open(write_path, 'wb') as b3dm_f:
            b3dm_f.write(output)