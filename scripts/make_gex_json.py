import argparse
import gzip
import json
from os import path
from scipy import sparse
import numpy as np
import sys
import msgpack
import struct

parser = argparse.ArgumentParser()
parser.add_argument("-n", "--name", help="name")
parser.add_argument("-i", "--institution", help="institution")
parser.add_argument("-f", "--file", help="name")
parser.add_argument("-d", "--dir", help="dir")
args = parser.parse_args()
file = args.file
dir = args.dir

BLOCK_SIZE = 256

f = gzip.open(file, "r")
# skip header
f.readline()

c = 0
block = 1

genes = []


offsets = []
buffer = b""

for line in f:
    tokens = line.decode().strip().split("\t")
    g = tokens[0]
    # print(g)
    ensembl, symbol = g.split(";")

    data = np.array([float(x) for x in tokens[1:]])

    s = data.sum()

    if s == 0:
        # print("reject, no exp", g)
        continue

    # reject if not in 10 of cells
    idx = np.where(data > 0)[0]

    # if len(idx) < 20:
    # print("reject, not enough cells", g)
    #    continue

    sparse_matrix = sparse.coo_matrix(data)
    # row unnecessary as we are looking at individual rows
    sparse_data = [
        [int(c), round(float(v), 4)]
        for r, c, v in zip(sparse_matrix.row, sparse_matrix.col, sparse_matrix.data)
    ]

    out = {"id": ensembl, "sym": symbol, "gex": sparse_data}

    genes.append(out)

    encoded = msgpack.packb(out, use_single_float=True)
    offsets.append(len(buffer))
    buffer += encoded

    # bunch genes into blocks of 32
    if len(genes) == BLOCK_SIZE:
        # fout = path.join(dir, f"gex_{block}.json.gz")
        # with gzip.open(fout, "wt", encoding="utf-8") as f:
        #     json.dump(genes, f)

        fout = path.join(dir, f"gex_{block}.dat")
        with open(fout, "wb") as f:
            f.write(struct.pack("<B", 42))  # magic
            f.write(struct.pack("<I", len(offsets)))  # number of entries
            for offset in offsets:
                f.write(struct.pack("<I", offset))  # 4 bytes each
            f.write(buffer)

        genes = []

        buffer = b""
        offsets = []

        block += 1
        # break

    c += 1

    if c % 1000 == 0:
        print(c, file=sys.stderr)


f.close()

if len(genes) > 0:
    # fout = path.join(dir, f"gex_{block}.json.gz")
    # with gzip.open(fout, "wt", encoding="utf-8") as f:
    #     json.dump(genes, f)  # , indent=2)

    fout = path.join(dir, f"gex_{block}.dat")
    with open(fout, "wb") as f:
        f.write(struct.pack("<B", 42))  # magic
        f.write(struct.pack("<I", len(offsets)))  # number of entries
        for offset in offsets:
            f.write(struct.pack("<I", offset))  # 4 bytes each
        f.write(buffer)
