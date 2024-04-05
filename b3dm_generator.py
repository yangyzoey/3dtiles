import struct
from glb_generator import GLB
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


def glb_test():

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
    
    pass
    