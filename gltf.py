import struct
import numpy as np
import json


class GlTF(object):

    def __init__(self):
        self.header = {}
        self.body = None

    def to_array(self):  # glb
        scene = json.dumps(self.header, separators=(',', ':'))

        # body must be 4-byte aligned
        scene += ' ' * ((4 - len(scene) % 4) % 4)

        padding = np.array([0 for i in range(0, (4 - len(self.body) % 4) % 4)],
                           dtype=np.uint8)

        length = 28 + len(self.body) + len(scene) + len(padding)
        binaryHeader = np.array([0x46546C67,  # "glTF" magic
                                 2,  # version
                                 length], dtype=np.uint32)
        jsonChunkHeader = np.array([len(scene),  # JSON chunck length
                                    0x4E4F534A], dtype=np.uint32)  # "JSON"

        binChunkHeader = np.array([len(self.body) + len(padding),
                                   # BIN chunck length
                                   0x004E4942], dtype=np.uint32)  # "BIN"

        return np.concatenate((binaryHeader.view(np.uint8),
                               jsonChunkHeader.view(np.uint8),
                               np.frombuffer(scene.encode('utf-8'), dtype=np.uint8),
                               binChunkHeader.view(np.uint8),
                               self.body,
                               padding))

    @staticmethod
    def from_array(array):
        """
        Parameters: array (numpy.array)
        """

        glTF = GlTF()


        length = struct.unpack("i", array[8:12])[0]
        content_length = struct.unpack("i", array[12:16])[0]



        header = struct.unpack(str(content_length) + "s",
                               array[20:20 + content_length])[0]
        glTF.header = json.loads(header.decode("ascii"))
        glTF.body = array[20 + content_length:length]

        return glTF

    @staticmethod
    def from_binary_arrays(arrays, transform, binary=True, batched=True):
        """
        Parameters: arrays (array of dictionaries)
        Each dictionary has the data for one feature
            arrays['position']: binary array of vertex positions
            arrays['normal']: binary array of vertex normals
            arrays['bbox']: geometry bounding box (numpy.array)
            transform : numpy.array
        """

        glTF = GlTF()


        binVertices = []
        binNormals = []
        binIds = []
        binUvs = []
        nVertices = []
        bb = []
        batchLength = 0
        for i, geometry in enumerate(arrays):
            binVertices.append(geometry['position'])
            binNormals.append(geometry['normal'])
            n = round(len(geometry['position']) / 12)
            nVertices.append(n)
            bb.append(geometry['bbox'])
            if batched:
                binIds.append(np.full(n, i, dtype=np.float32))


        if batched:
            binVertices = [b''.join(binVertices)]
            binNormals = [b''.join(binNormals)]
            binUvs = [b''.join(binUvs)]
            binIds = [b''.join(binIds)]
            nVertices = [sum(nVertices)]
            batchLength = len(arrays)
            [minx, miny, minz] = bb[0][0]
            [maxx, maxy, maxz] = bb[0][1]
            for box in bb[1:]:
                minx = min(minx, box[0][0])
                miny = min(miny, box[0][1])
                minz = min(minz, box[0][2])
                maxx = max(maxx, box[1][0])
                maxy = max(maxy, box[1][1])
                maxz = max(maxz, box[1][2])
            bb = [[[minx, miny, minz], [maxx, maxy, maxz]]]

        glTF.header = compute_header(binVertices, nVertices, bb, transform,
                                     batched, batchLength)
        glTF.body = np.frombuffer(compute_binary(binVertices, binNormals,
                                  binIds, binUvs), dtype=np.uint8)

        return glTF


def compute_binary(binVertices, binNormals, binIds, binUvs):
    bv = b''.join(binVertices)
    bn = b''.join(binNormals)
    bid = b''.join(binIds)
    buv = b''.join(binUvs)
    return bv + bn + buv + bid


def compute_header(binVertices, nVertices, bb, transform,batched, batchLength):
    # Buffer
    meshNb = len(binVertices)
    sizeVce = []
    for i in range(0, meshNb):
        sizeVce.append(len(binVertices[i]))

    byteLength = 2 * sum(sizeVce)

    if batched:
        byteLength += int(round(sum(sizeVce) / 3))
    buffers = [{
        'byteLength': byteLength
    }]


    # Buffer view
    bufferViews = []
    # vertices
    bufferViews.append({
        'buffer': 0,
        'byteLength': sum(sizeVce),
        'byteOffset': 0,
        'target': 34962
    })
    bufferViews.append({
        'buffer': 0,
        'byteLength': sum(sizeVce),
        'byteOffset': sum(sizeVce),
        'target': 34962
    })

    if batched:
        bufferViews.append({
            'buffer': 0,
            'byteLength': int(round(sum(sizeVce) / 3)),
            else 2 * sum(sizeVce),
            'target': 34962
        })

    # Accessor
    accessors = []
    for i in range(0, meshNb):
        # vertices
        accessors.append({
            'bufferView': 0,
            'byteOffset': sum(sizeVce[0:i]),
            'componentType': 5126,
            'count': nVertices[i],
            'max': [bb[i][0][1], bb[i][0][2], bb[i][0][0]],
            'min': [bb[i][1][1], bb[i][1][2], bb[i][1][0]],
            'type': "VEC3"
        })
        # normals
        accessors.append({
            'bufferView': 1,
            'byteOffset': sum(sizeVce[0:i]),
            'componentType': 5126,
            'count': nVertices[i],
            'max': [1, 1, 1],
            'min': [-1, -1, -1],
            'type': "VEC3"
        })

    if batched:
        accessors.append({
            'bufferView': 2,
            'byteOffset': 0,
            'componentType': 5126,
            'count': nVertices[0],
            'max': [batchLength],
            'min': [0],
            'type': "SCALAR"
        })

    # Meshes
    meshes = []
    nAttributes = 2
    for i in range(0, meshNb):
        meshes.append({
            'primitives': [{
                'attributes': {
                    "POSITION": nAttributes * i,
                    "NORMAL": nAttributes * i + 1
                },
                "material": 0,
                "mode": 4
            }]
        })

        meshes[0]['primitives'][0]['attributes']['_BATCHID'] = nAttributes

    # Nodes
    nodes = []
    for i in range(0, meshNb):
        nodes.append({
            'matrix': [float(e) for e in transform],
            'mesh': i
        })

    # Materials
    materials = [{
        'pbrMetallicRoughness': {
            'metallicFactor': 0
        },
        'name': 'Material',
    }]


    # Final glTF
    header = {
        'asset': {
            "generator": "zoey_test",
            "version": "2.0"
        },
        'scene': 0,
        'scenes': [{
            'nodes': [i for i in range(0, len(nodes))]
        }],
        'nodes': nodes,
        'meshes': meshes,
        'materials': materials,
        'accessors': accessors,
        'bufferViews': bufferViews,
        'buffers': buffers
    }


    return header
