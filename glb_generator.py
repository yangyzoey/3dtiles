import json
import base64
import struct
import numpy as np

 
class GLTF:
    def __init__(self):


        self.bin_data= bytearray()

        self.json_data = {
            "accessors": [
    {
      "bufferView": 0, 
      "byteOffset": 0, # positions
      "componentType": 5126,
      "count": 240, 
      "type": "VEC3",
      "min": [
        -103.97583675780334,
        -114.61250572279096,
        -102.50925003085285
      ], 
      "max": [
        118.4458890708629,
        106.54341480974108,
        93.78807587129995
      ] 
    },
    {
      "bufferView": 1, 
      "byteOffset": 0, # normals
      "componentType": 5126,
      "count": 240, 
      "type": "VEC3",
      "min": [
        -0.9686356343768793,
        -0.7415555652213446,
        -0.765567091384559
      ], 
      "max": [
        0.9686356343768793,
        0.7415555652213446,
        0.765567091384559
      ] 
    },
    {
      "bufferView": 2, 
      "byteOffset": 0, # ids
      "componentType": 5126,
      "count": 240, 
      "type": "SCALAR",
      "min": [
        0
      ],
      "max": [
        9    
      ]
    },
  ],
            "asset": {
                "generator": "zoey-test-generator",
                "version": "2.0"
            },
            "buffers": [
              {
                    "name": "buffer",
                    "byteLength": 7440 
              }
            ],
            "bufferViews": [
              {
      "buffer": 0,
      "byteLength": 2880, 
      "byteOffset": 0, 
      "target": 34962, 
      "byteStride": 12
    },
    {
      "buffer": 0,
      "byteLength": 2880, 
      "byteOffset": 2880, 
      "target": 34962,
      "byteStride": 12
    },
    {
      "buffer": 0,
      "byteLength": 960, 
      "byteOffset": 5760, 
      "target": 34962,
      "byteStride": 4
    },
            ],
              "materials": [
                {
                "pbrMetallicRoughness": {
                    "baseColorFactor": [0.53, 0.81, 0.98, 1], # blue # [1, 0.75, 0.8, 1] # pink [1, 1, 1, 1] # white
                    "roughnessFactor": 1,
                    "metallicFactor": 0
                },
                "alphaMode": "OPAQUE",
                "doubleSided": False, # false 
                "emissiveFactor": [
                    0,
                    0,
                    0
                ]
                }
            ],
            "meshes": [
                {
                "primitives": [
                    {
                    "attributes": {
                        "POSITION": 0,
                        "NORMAL": 1,
                        "_BATCHID": 2
                    },
                    "material": 0,
                    "mode": 4
                    }
                ]
                }
            ],
            "nodes": [{"matrix": [1,0,0,0,0,0,-1,0,0,1,0,0,0,0,0,1],"mesh": 0,"name": "rootNode"}],
            "scene": 0,
            "scenes": [{"nodes": [0]}]
        }


    @staticmethod
    def _get_ele_num(eleType):
        """:return: number of components in an element
        """
        TYPE_MAP = {
            "SCALAR": 1,
            "VEC3": 3
        }
        return TYPE_MAP[eleType]


    @staticmethod
    def _get_comptype_size(componentType):
        """:return: the size of each component
        """
        COMPONENTTYPE_SIZE_MAP = {
            5125: 4,
            # 5123: 2,
            5126: 4
        }
        return COMPONENTTYPE_SIZE_MAP[componentType]


    @staticmethod
    def _get_comptype_formatter(componentType):
        COMPONENTTYPE_MAP = {
            5125: 'I',  # unsigned int
            #5123: 'H',  # unsigned short
            5126: 'f'  # float
        }
        return COMPONENTTYPE_MAP[componentType]


    @staticmethod
    def _generate_chunk(data, componentType, eleType, offset, bufferView_idx):
        """Generate a binary blob with the required data alignment:
        
            - byteLength must be a multiple of 4
            - Pad data with trailing \x00
            - byteStride = ele_num * comptype_size
        :return: (binary data<bytearray>, byteLength<int>, byteOffset<int>)

        Note: The returned byteOffset is the offset based on ?
        """
        final_barray = bytearray()

        ele_num = GLTF._get_ele_num(eleType)
        comp_size = GLTF._get_comptype_size(componentType)
        comp_formatter = GLTF._get_comptype_formatter(componentType)
        ele_size = comp_size * ele_num  # 12 /2

        # ele_padding_count = (4 - ele_size % 4) % 4 if bufferView_idx != 3 else 0
        # formatter = '<{}{}{}x'.format(ele_num, comp_formatter, ele_padding_count)
        formatter = '<{}{}{}x'.format(ele_num, comp_formatter, 0)

        # "SCA" or "VEC"
        type_pref = eleType[:3]

        if type_pref == "SCA":
            for ele in data:
                b = struct.pack(formatter, ele)
                final_barray.extend(b)
        elif type_pref == "VEC":
            for ele in data:
                b = struct.pack(formatter, *ele)
                final_barray.extend(b)


        byte_length = len(final_barray)
        chunk_padding_count = (4 - byte_length % 4) % 4
        final_barray.extend(struct.pack('{}x'.format(chunk_padding_count)))

        byte_offset = offset + len(final_barray)

        return final_barray, byte_length, byte_offset  


    @staticmethod
    def _compute_bbx(array):
        min_list = np.amin(array, axis=0).tolist()
        max_list = np.amax(array, axis=0).tolist()

        # print("array", array)
        # print("min_list", min_list)
        # print("max_list", max_list)
        
        return min_list, max_list


    def update_value(self, position, normal, ids, indices, rgb):
        """
        --position array
        --normal array
        --ids array
        --indices array

        update "count", "min", "max" in "accessors"
        update "byteLength" in "buffers"
        update "byteLength" and "byteOffset" in "bufferViews"
        """

        byte_offset0 = 0
        final_barray0, byte_length0, byte_offset1 = GLTF. _generate_chunk(position, 5126, "VEC3", byte_offset0, 0)
        final_barray1, byte_length1, byte_offset2 = GLTF. _generate_chunk(normal, 5126, "VEC3", byte_offset1, 1)
        final_barray2, byte_length2, byte_offset3 = GLTF. _generate_chunk(ids, 5126, "SCALAR", byte_offset2, 2)
        if indices != None:
            final_barray3, byte_length3, byte_offset4 = GLTF. _generate_chunk(indices, 5125, "SCALAR", byte_offset3, 3) # 5123 too small
        else:
            pass


        # Update position
        self.json_data["accessors"][0]["count"] = len(position)
        self.json_data["accessors"][0]["min"] = GLTF._compute_bbx(position)[0]
        self.json_data["accessors"][0]["max"] = GLTF._compute_bbx(position)[1]

        self.json_data["bufferViews"][0]["byteLength"] = byte_length0
        self.json_data["bufferViews"][0]["byteOffset"] = byte_offset0


        # Update normal
        self.json_data["accessors"][1]["count"] = len(normal)
        self.json_data["accessors"][1]["min"] = GLTF._compute_bbx(normal)[0]
        self.json_data["accessors"][1]["max"] = GLTF._compute_bbx(normal)[1]

        self.json_data["bufferViews"][1]["byteLength"] = byte_length1
        self.json_data["bufferViews"][1]["byteOffset"] = byte_offset1


        # Update ID
        self.json_data["accessors"][2]["count"] = len(ids)
        self.json_data["accessors"][2]["min"] = [min(ids)]
        self.json_data["accessors"][2]["max"] = [max(ids)]

        self.json_data["bufferViews"][2]["byteLength"] = byte_length2
        self.json_data["bufferViews"][2]["byteOffset"] = byte_offset2


        # Update color
        self.json_data["materials"][0]["pbrMetallicRoughness"]["baseColorFactor"] = rgb
 

        # Update indices
        if indices != None:

            self.json_data["meshes"][0]["primitives"][0]['indices']= 3

            self.json_data["accessors"].append({
              "bufferView": 3,
              "byteOffset": 0, # indices
              "componentType": 5125, #5123 too small
              "count": 360, 
              "type": "SCALAR",
              "min": [
                0
              ],
              "max": [
                239 
              ]
            })


            self.json_data["accessors"][3]["count"] = len(indices)
            self.json_data["accessors"][3]["min"] = [min(indices)]
            self.json_data["accessors"][3]["max"] = [max(indices)]


            self.json_data["bufferViews"].append(    {
              "buffer": 0,
              "byteLength": 720, 
              "byteOffset": 6720, 
              "target": 34963
            })


            self.json_data["bufferViews"][3]["byteLength"] = byte_length3
            self.json_data["bufferViews"][3]["byteOffset"] = byte_offset3

            # Update "byteLength" in "buffers"
            self.json_data["buffers"][0]["byteLength"] = byte_offset4

        else:
            # Update "byteLength" in "buffers"
            self.json_data["buffers"][0]["byteLength"] = byte_offset3
        

        # create bin_chunk
        self.bin_data.extend(final_barray0)
        self.bin_data.extend(final_barray1)
        self.bin_data.extend(final_barray2)
        if indices != None:
            self.bin_data.extend(final_barray3)
        else:
            pass


    def export_gltf_file(self, position, normal, ids, indices, rgb, output_file):
        with open(output_file, "w") as f:
            self.update_value(position, normal, ids, indices, rgb)
            json.dump(self.json_data, f, indent=2)

        print(f"glTF file generated as '{output_file}'.")


class GLB(GLTF):
    """
    GLB is a subclass of GLTF. This means that GLB will inherit the properties and methods defined in the GLTF class. 
    Instances of the GLB class will have access to both the properties and methods of GLB and GLTF.
    """
    MAGIC = int.from_bytes(b"glTF", byteorder='little')
    VERSION = 2
    JSON_CHUNK_TYPE = b"JSON"
    BIN_CHUNK_TYPE = b"BIN\x00"

    def __init__(self):
        super(GLB, self).__init__()


    def _build_header(self, total_length):
        """Build the 12-byte header chunk

        :return: a 12-byte binary object
        """
        header = struct.pack("<III", self.MAGIC, self.VERSION, total_length)

        return header


    def _build_json_chunk(self, extra_padding):
        """Build the padded JSON chunk

        :return: the chunk <bytearray>
        """

        json_chunk_data = json.dumps(self.json_data, separators=(',', ':'))

        # Pad with trailing spaces (0x20) to satisfy alignment requirements
        if len(json_chunk_data) % 4:
            json_chunk_data += (4 - len(json_chunk_data) % 4) * " "

        # -------------------------extra padding (for rebuild) start-----------------------------------------------
        json_chunk_data +=  extra_padding * " "
        # buffer.extend(struct.pack('{}x'.format(padding_count)))
        # -------------------------extra padding (for rebuild) end-------------------------------------------------
        json_chunk = bytearray()
        json_chunk.extend(struct.pack("<I", len(json_chunk_data)))  # u32int # json_chunk_data_length
        json_chunk.extend(struct.pack("<4s", self.JSON_CHUNK_TYPE))  # u32int
        json_chunk.extend(bytes(json_chunk_data, "utf-8"))

        return json_chunk


    def _build_bin_chunk(self):
        """Build the padded Binary chunk

        :return: the chunk <bytearray>
        """

        bin_chunk_data = self.bin_data

        bin_chunk = bytearray()
        bin_chunk_data_length = len(bin_chunk_data)
        # No need to pad with trailing spaces to conform to byte alignment

        bin_chunk.extend(struct.pack("<I", bin_chunk_data_length))  # u32int
        bin_chunk.extend(struct.pack("<4s", self.BIN_CHUNK_TYPE))  # u32int
        bin_chunk.extend(bin_chunk_data)

        return bin_chunk



    def draw_glb(self,position, normal, ids, indices, rgb):
        """Generate the glb file to path
        """

        glb = bytearray()

        self.update_value(position, normal, ids, indices, rgb)

        json_chunk = self._build_json_chunk(0)
        bin_chunk = self._build_bin_chunk()
        # Build header last since it needs the total byte length
        total_byte_length = 12 + len(bin_chunk) + len(json_chunk)


        # ----------------------------------------this is for glb packed with b3dm--------------------------------------------
        # check if glb align with 8 bytes
        padding_count = (8 - total_byte_length % 8) % 8  
        # print("previous total_byte_length:", total_byte_length)
        # print("padding_count: ", padding_count)
        # rebuild json chunk
        json_chunk = self._build_json_chunk(padding_count)
        total_byte_length += padding_count
        # print("after total_byte_length:", total_byte_length)
        # ----------------------------------------this is for glb packed with b3dm--------------------------------------------
        header_chunk = self._build_header(total_byte_length)
        glb.extend(header_chunk)
        glb.extend(json_chunk)
        glb.extend(bin_chunk)

        # print(self.json_data)

        return glb



if __name__ == "__main__":

    pass